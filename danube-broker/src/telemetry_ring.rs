use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_SAMPLE_CAPACITY: usize = 360; // 1 hour at 10s interval

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetricSeriesKey {
    name: String,
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

#[derive(Debug)]
enum MetricValueSlot {
    Counter(Arc<RingCounter>),
    Gauge(Arc<RingGauge>),
    Histogram(Arc<RingHistogram>),
}

impl MetricValueSlot {
    fn kind(&self) -> MetricKind {
        match self {
            Self::Counter(_) => MetricKind::Counter,
            Self::Gauge(_) => MetricKind::Gauge,
            Self::Histogram(_) => MetricKind::Histogram,
        }
    }

    fn current_f64(&self) -> f64 {
        match self {
            Self::Counter(c) => c.value.load(Ordering::Relaxed) as f64,
            Self::Gauge(g) => f64::from_bits(g.value_bits.load(Ordering::Relaxed)),
            Self::Histogram(h) => h.mean_or_last(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TelemetryPoint {
    pub(crate) timestamp_ms: u64,
    pub(crate) value: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct TelemetrySeriesSnapshot {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) labels: BTreeMap<String, String>,
    pub(crate) current_value: f64,
    pub(crate) history: Vec<TelemetryPoint>,
}

#[derive(Debug, Clone)]
struct SampleBucket {
    timestamp_ms: u64,
    values: HashMap<MetricSeriesKey, f64>,
}

/// In-memory time-series telemetry store backed by live `metrics` atomics and
/// a 360-sample (1h @ 10s) rolling window.
#[derive(Debug)]
pub(crate) struct BrokerTelemetryStore {
    slots: Mutex<HashMap<MetricSeriesKey, MetricValueSlot>>,
    samples: Mutex<VecDeque<SampleBucket>>,
    sample_capacity: usize,
}

static GLOBAL_TELEMETRY_STORE: OnceLock<Arc<BrokerTelemetryStore>> = OnceLock::new();

impl Default for BrokerTelemetryStore {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_SAMPLE_CAPACITY)
    }
}

impl BrokerTelemetryStore {
    pub(crate) fn with_capacity(sample_capacity: usize) -> Self {
        let cap = sample_capacity.max(1);
        Self {
            slots: Mutex::new(HashMap::new()),
            samples: Mutex::new(VecDeque::with_capacity(cap)),
            sample_capacity: cap,
        }
    }

    /// Returns the process-wide `BrokerTelemetryStore`.
    pub(crate) fn global() -> Arc<Self> {
        GLOBAL_TELEMETRY_STORE
            .get_or_init(|| Arc::new(Self::default()))
            .clone()
    }

    fn series_key(key: &Key) -> MetricSeriesKey {
        let mut labels = BTreeMap::new();
        for label in key.labels() {
            labels.insert(label.key().to_string(), label.value().to_string());
        }
        MetricSeriesKey {
            name: key.name().to_string(),
            labels,
        }
    }

    /// Captures a point-in-time sample of all registered metrics into the ring buffer.
    pub(crate) fn record_sample(&self) {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let snapshot_values: HashMap<MetricSeriesKey, f64> = {
            let guard = match self.slots.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard
                .iter()
                .map(|(k, slot)| (k.clone(), slot.current_f64()))
                .collect()
        };

        let mut samples = match self.samples.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if samples.len() >= self.sample_capacity {
            samples.pop_front();
        }
        samples.push_back(SampleBucket {
            timestamp_ms,
            values: snapshot_values,
        });
    }

    /// Queries current values and optional history samples over `window_seconds`.
    pub(crate) fn query_snapshot(
        &self,
        metric_names: &[String],
        window_seconds: u32,
    ) -> (u64, Vec<TelemetrySeriesSnapshot>) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let window_ms = (window_seconds.min(3600) as u64).saturating_mul(1000);
        let cutoff_ms = if window_ms == 0 {
            u64::MAX
        } else {
            now_ms.saturating_sub(window_ms)
        };

        let current_slots: Vec<(MetricSeriesKey, MetricKind, f64)> = {
            let guard = match self.slots.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard
                .iter()
                .filter(|(k, _)| {
                    metric_names.is_empty() || metric_names.iter().any(|name| *name == k.name)
                })
                .map(|(k, slot)| (k.clone(), slot.kind(), slot.current_f64()))
                .collect()
        };

        let samples_snapshot: Vec<SampleBucket> = if window_seconds == 0 {
            Vec::new()
        } else {
            let samples = match self.samples.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            samples
                .iter()
                .filter(|b| b.timestamp_ms >= cutoff_ms)
                .cloned()
                .collect()
        };

        let mut result: Vec<TelemetrySeriesSnapshot> = current_slots
            .into_iter()
            .map(|(key, kind, current_value)| {
                let history = samples_snapshot
                    .iter()
                    .filter_map(|bucket| {
                        bucket.values.get(&key).map(|&val| TelemetryPoint {
                            timestamp_ms: bucket.timestamp_ms,
                            value: val,
                        })
                    })
                    .collect();
                TelemetrySeriesSnapshot {
                    name: key.name,
                    kind: kind.as_str().to_string(),
                    labels: key.labels,
                    current_value,
                    history,
                }
            })
            .collect();

        result.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.labels.cmp(&b.labels)));
        (now_ms, result)
    }

    fn get_or_insert_counter(
        &self,
        key: &Key,
        inner: Option<Counter>,
    ) -> Arc<RingCounter> {
        let sk = Self::series_key(key);
        let mut guard = match self.slots.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(MetricValueSlot::Counter(existing)) = guard.get(&sk) {
            return existing.clone();
        }
        let handle = Arc::new(RingCounter {
            value: AtomicU64::new(0),
            inner,
        });
        guard.insert(sk, MetricValueSlot::Counter(handle.clone()));
        handle
    }

    fn get_or_insert_gauge(&self, key: &Key, inner: Option<Gauge>) -> Arc<RingGauge> {
        let sk = Self::series_key(key);
        let mut guard = match self.slots.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(MetricValueSlot::Gauge(existing)) = guard.get(&sk) {
            return existing.clone();
        }
        let handle = Arc::new(RingGauge {
            value_bits: AtomicU64::new(0.0_f64.to_bits()),
            inner,
        });
        guard.insert(sk, MetricValueSlot::Gauge(handle.clone()));
        handle
    }

    fn get_or_insert_histogram(
        &self,
        key: &Key,
        inner: Option<Histogram>,
    ) -> Arc<RingHistogram> {
        let sk = Self::series_key(key);
        let mut guard = match self.slots.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(MetricValueSlot::Histogram(existing)) = guard.get(&sk) {
            return existing.clone();
        }
        let handle = Arc::new(RingHistogram {
            count: AtomicU64::new(0),
            sum_bits: AtomicU64::new(0.0_f64.to_bits()),
            last_bits: AtomicU64::new(0.0_f64.to_bits()),
            inner,
        });
        guard.insert(sk, MetricValueSlot::Histogram(handle.clone()));
        handle
    }
}

#[derive(Debug)]
struct RingCounter {
    value: AtomicU64,
    inner: Option<Counter>,
}

impl CounterFn for RingCounter {
    fn increment(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
        if let Some(ref c) = self.inner {
            c.increment(value);
        }
    }

    fn absolute(&self, value: u64) {
        self.value.store(value, Ordering::Relaxed);
        if let Some(ref c) = self.inner {
            c.absolute(value);
        }
    }
}

#[derive(Debug)]
struct RingGauge {
    value_bits: AtomicU64,
    inner: Option<Gauge>,
}

fn atomic_f64_update(atom: &AtomicU64, op: impl Fn(f64) -> f64) {
    let mut cur = atom.load(Ordering::Relaxed);
    loop {
        let next = op(f64::from_bits(cur)).to_bits();
        match atom.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
}

impl GaugeFn for RingGauge {
    fn increment(&self, value: f64) {
        atomic_f64_update(&self.value_bits, |v| v + value);
        if let Some(ref g) = self.inner {
            g.increment(value);
        }
    }

    fn decrement(&self, value: f64) {
        atomic_f64_update(&self.value_bits, |v| v - value);
        if let Some(ref g) = self.inner {
            g.decrement(value);
        }
    }

    fn set(&self, value: f64) {
        self.value_bits.store(value.to_bits(), Ordering::Relaxed);
        if let Some(ref g) = self.inner {
            g.set(value);
        }
    }
}

#[derive(Debug)]
struct RingHistogram {
    count: AtomicU64,
    sum_bits: AtomicU64,
    last_bits: AtomicU64,
    inner: Option<Histogram>,
}

impl RingHistogram {
    fn mean_or_last(&self) -> f64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            f64::from_bits(self.sum_bits.load(Ordering::Relaxed)) / count as f64
        }
    }
}

impl HistogramFn for RingHistogram {
    fn record(&self, value: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        atomic_f64_update(&self.sum_bits, |s| s + value);
        self.last_bits.store(value.to_bits(), Ordering::Relaxed);
        if let Some(ref h) = self.inner {
            h.record(value);
        }
    }
}

/// `metrics::Recorder` that feeds both `BrokerTelemetryStore` and an optional inner
/// Prometheus recorder.
pub(crate) struct TelemetryRecorder {
    store: Arc<BrokerTelemetryStore>,
    inner: Option<Box<dyn Recorder + Send + Sync>>,
}

impl TelemetryRecorder {
    pub(crate) fn new(
        store: Arc<BrokerTelemetryStore>,
        inner: Option<Box<dyn Recorder + Send + Sync>>,
    ) -> Self {
        Self { store, inner }
    }
}

impl Recorder for TelemetryRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        if let Some(ref inner) = self.inner {
            inner.describe_counter(key, unit, description);
        }
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        if let Some(ref inner) = self.inner {
            inner.describe_gauge(key, unit, description);
        }
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        if let Some(ref inner) = self.inner {
            inner.describe_histogram(key, unit, description);
        }
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        let inner = self
            .inner
            .as_ref()
            .map(|r| r.register_counter(key, metadata));
        Counter::from_arc(self.store.get_or_insert_counter(key, inner))
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        let inner = self.inner.as_ref().map(|r| r.register_gauge(key, metadata));
        Gauge::from_arc(self.store.get_or_insert_gauge(key, inner))
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        let inner = self
            .inner
            .as_ref()
            .map(|r| r.register_histogram(key, metadata));
        Histogram::from_arc(self.store.get_or_insert_histogram(key, inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::Level;

    #[test]
    fn telemetry_store_tracks_metrics_and_evicts_ring_samples() {
        let store = Arc::new(BrokerTelemetryStore::with_capacity(2));
        let recorder = TelemetryRecorder::new(store.clone(), None);
        static META: Metadata<'static> = Metadata::new("test", Level::INFO, Some("test"));

        let c = recorder.register_counter(&Key::from_name("danube_test_counter"), &META);
        let g = recorder.register_gauge(&Key::from_name("danube_test_gauge"), &META);
        let h = recorder.register_histogram(&Key::from_name("danube_test_hist"), &META);

        c.increment(5);
        g.set(12.5);
        g.increment(2.5);
        h.record(10.0);
        h.record(20.0);

        store.record_sample();
        c.increment(3);
        store.record_sample();
        c.increment(2);
        store.record_sample();

        let (_, series) = store.query_snapshot(&[], 60);
        assert_eq!(series.len(), 3);
        let counter_series = series
            .iter()
            .find(|s| s.name == "danube_test_counter")
            .unwrap();
        assert_eq!(counter_series.current_value, 10.0);
        assert_eq!(counter_series.history.len(), 2); // bounded by capacity 2

        let gauge_series = series.iter().find(|s| s.name == "danube_test_gauge").unwrap();
        assert_eq!(gauge_series.current_value, 15.0);

        let hist_series = series.iter().find(|s| s.name == "danube_test_hist").unwrap();
        assert_eq!(hist_series.current_value, 15.0);
    }
}

