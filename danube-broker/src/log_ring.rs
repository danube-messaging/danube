use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::field::{Field, Visit};
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

const DEFAULT_LOG_CAPACITY: usize = 5_000;

/// Single structured log record retained in the in-memory ring buffer.
#[derive(Debug, Clone)]
pub(crate) struct BrokerLogEntry {
    pub(crate) timestamp_ms: u64,
    pub(crate) level: String,
    pub(crate) target: String,
    pub(crate) message: String,
    pub(crate) fields: BTreeMap<String, String>,
}

/// Thread-safe bounded ring buffer holding recent structured broker log entries.
#[derive(Debug, Clone)]
pub(crate) struct LogRingHandle {
    entries: Arc<Mutex<VecDeque<BrokerLogEntry>>>,
    capacity: usize,
}

static GLOBAL_LOG_RING: OnceLock<Arc<LogRingHandle>> = OnceLock::new();

impl Default for LogRingHandle {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_LOG_CAPACITY)
    }
}

impl LogRingHandle {
    /// Creates a new log ring buffer with the specified capacity.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::with_capacity(capacity.min(DEFAULT_LOG_CAPACITY)))),
            capacity: capacity.max(1),
        }
    }

    /// Initializes (if unset) and returns the process-wide log ring buffer.
    pub(crate) fn global() -> Arc<Self> {
        GLOBAL_LOG_RING
            .get_or_init(|| Arc::new(Self::default()))
            .clone()
    }

    /// Returns a `tracing_subscriber::Layer` that records events into this ring buffer.
    pub(crate) fn layer(&self) -> RingBufferLogLayer {
        RingBufferLogLayer {
            handle: self.clone(),
        }
    }

    /// Appends an entry, evicting the oldest entry if at capacity.
    pub(crate) fn push(&self, entry: BrokerLogEntry) {
        let mut guard = match self.entries.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.len() >= self.capacity {
            guard.pop_front();
        }
        guard.push_back(entry);
    }

    /// Queries buffered log entries matching optional filters (newest entries last).
    pub(crate) fn query(
        &self,
        level: Option<&str>,
        target: Option<&str>,
        contains: Option<&str>,
        since_ms: Option<u64>,
        limit: usize,
    ) -> (Vec<BrokerLogEntry>, usize) {
        let guard = match self.entries.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let total_buffered = guard.len();
        let max_items = if limit == 0 {
            200
        } else {
            limit.min(self.capacity)
        };
        let level_upper = level.map(|s| s.trim().to_ascii_uppercase());
        let needle = contains.map(|s| s.to_ascii_lowercase());

        let mut matched: Vec<BrokerLogEntry> = guard
            .iter()
            .filter(|entry| {
                if let Some(since) = since_ms {
                    if since > 0 && entry.timestamp_ms < since {
                        return false;
                    }
                }
                if let Some(ref lvl) = level_upper {
                    if !lvl.is_empty() && entry.level != *lvl {
                        return false;
                    }
                }
                if let Some(tgt) = target {
                    if !tgt.is_empty() && !entry.target.contains(tgt) {
                        return false;
                    }
                }
                if let Some(ref n) = needle {
                    if !n.is_empty() {
                        let in_msg = entry.message.to_ascii_lowercase().contains(n);
                        let in_fields = entry.fields.iter().any(|(k, v)| {
                            k.to_ascii_lowercase().contains(n) || v.to_ascii_lowercase().contains(n)
                        });
                        if !in_msg && !in_fields {
                            return false;
                        }
                    }
                }
                true
            })
            .cloned()
            .collect();

        if matched.len() > max_items {
            let drain_count = matched.len() - max_items;
            matched.drain(..drain_count);
        }

        (matched, total_buffered)
    }
}

/// `tracing_subscriber::Layer` capturing structured events into `LogRingHandle`.
#[derive(Debug, Clone)]
pub(crate) struct RingBufferLogLayer {
    handle: LogRingHandle,
}

impl<S: Subscriber> Layer<S> for RingBufferLogLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        if !matches!(
            *metadata.level(),
            tracing::Level::ERROR | tracing::Level::WARN | tracing::Level::INFO
        ) {
            return;
        }

        let mut visitor = FieldExtractor::default();
        event.record(&mut visitor);

        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        self.handle.push(BrokerLogEntry {
            timestamp_ms,
            level: metadata.level().to_string(),
            target: metadata.target().to_string(),
            message: visitor.message,
            fields: visitor.fields,
        });
    }
}

#[derive(Default)]
struct FieldExtractor {
    message: String,
    fields: BTreeMap<String, String>,
}

impl Visit for FieldExtractor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields.insert(field.name().to_string(), value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let formatted = format!("{:?}", value);
        if field.name() == "message" {
            self.message = formatted;
        } else {
            self.fields.insert(field.name().to_string(), formatted);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_eviction_and_filtering() {
        let ring = LogRingHandle::with_capacity(3);
        for i in 1..=5 {
            ring.push(BrokerLogEntry {
                timestamp_ms: i * 100,
                level: if i % 2 == 0 { "WARN" } else { "INFO" }.to_string(),
                target: "danube_broker::topic".to_string(),
                message: format!("event-{}", i),
                fields: BTreeMap::from([("topic".to_string(), format!("t{}", i))]),
            });
        }

        let (all, total) = ring.query(None, None, None, None, 10);
        assert_eq!(total, 3);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].message, "event-3");
        assert_eq!(all[2].message, "event-5");

        let (warns, _) = ring.query(Some("warn"), Some("topic"), Some("t4"), Some(350), 10);
        assert_eq!(warns.len(), 1);
        assert_eq!(warns[0].message, "event-4");
    }
}

