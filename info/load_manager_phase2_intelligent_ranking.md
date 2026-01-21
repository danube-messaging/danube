# LoadManager Phase 2: Topic-Level Metrics & Intelligent Ranking

**Priority:** HIGH  
**Timeline:** 3-4 weeks  
**Status:** Not Started  
**Depends On:** Phase 1 (Resource Monitoring)

---

## 🎯 Goals

- Track per-topic resource consumption (throughput, latency, disk usage)
- Implement weighted ranking algorithms based on actual load
- Move beyond simple topic count to intelligent placement decisions
- Make ranking algorithm configurable

---

## 📋 Implementation Steps

### Step 1: Enhance LoadReport with Topic Metrics
**Status:** ☐ Not Started

**Task:** Update `load_report.rs` to include per-topic statistics

```rust
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LoadReport {
    // System-level metrics (from Phase 1)
    pub(crate) resources_usage: Vec<SystemLoad>,
    
    // Topic-level metrics (NEW)
    pub(crate) topics: Vec<TopicLoad>,
    
    // Aggregate metrics (NEW)
    pub(crate) total_throughput_mbps: f64,
    pub(crate) total_disk_usage_gb: f64,
    pub(crate) total_message_rate: u64,
    
    // Metadata
    pub(crate) timestamp: u64,
    pub(crate) broker_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TopicLoad {
    pub(crate) topic_name: String,
    
    // Throughput metrics
    pub(crate) message_rate: u64,        // messages/second
    pub(crate) byte_rate: u64,           // bytes/second
    pub(crate) byte_rate_mbps: f64,      // Mbps for easier reading
    
    // Connection metrics
    pub(crate) producer_count: usize,
    pub(crate) consumer_count: usize,
    pub(crate) subscription_count: usize,
    
    // Storage metrics
    pub(crate) disk_size_bytes: u64,     // WAL + cloud storage
    pub(crate) wal_size_bytes: u64,      // Local WAL size
    pub(crate) cloud_size_bytes: u64,    // Cloud object storage size
    
    // Performance metrics
    pub(crate) avg_latency_ms: f64,      // Average produce latency
    pub(crate) p99_latency_ms: f64,      // 99th percentile latency
    
    // Age metrics
    pub(crate) oldest_message_age_sec: u64,
    pub(crate) backlog_messages: u64,    // Unconsumed messages
}

impl TopicLoad {
    pub fn estimated_load_score(&self) -> f64 {
        // Weighted score for quick comparisons
        let throughput_score = (self.byte_rate as f64 / 1_000_000.0) * 0.4; // MB/s
        let connection_score = (self.producer_count + self.consumer_count) as f64 * 0.3;
        let storage_score = (self.disk_size_bytes as f64 / 1_000_000_000.0) * 0.2; // GB
        let latency_score = (self.p99_latency_ms / 100.0) * 0.1;
        
        throughput_score + connection_score + storage_score + latency_score
    }
}
```

**Verification:**
- [ ] Structures compile
- [ ] Serialization works
- [ ] Backward compatibility maintained (old LoadReport still deserializes)

---

### Step 2: Add Topic Metrics Collection to BrokerService
**Status:** ☐ Not Started

**Task:** Create metric tracking infrastructure in `broker_service.rs`

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::RwLock;

pub struct TopicMetrics {
    // Counters
    messages_produced: AtomicU64,
    bytes_produced: AtomicU64,
    messages_consumed: AtomicU64,
    bytes_consumed: AtomicU64,
    
    // Latency tracking (simplified histogram)
    latency_sum_ms: AtomicU64,
    latency_count: AtomicU64,
    latency_p99_ms: AtomicU64,  // Updated periodically
    
    // Connection tracking
    producer_count: AtomicU64,
    consumer_count: AtomicU64,
    
    // Last snapshot for rate calculation
    last_snapshot: Arc<RwLock<MetricsSnapshot>>,
}

#[derive(Debug, Clone)]
struct MetricsSnapshot {
    timestamp: Instant,
    messages_produced: u64,
    bytes_produced: u64,
}

impl TopicMetrics {
    pub fn new() -> Self {
        Self {
            messages_produced: AtomicU64::new(0),
            bytes_produced: AtomicU64::new(0),
            messages_consumed: AtomicU64::new(0),
            bytes_consumed: AtomicU64::new(0),
            latency_sum_ms: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latency_p99_ms: AtomicU64::new(0),
            producer_count: AtomicU64::new(0),
            consumer_count: AtomicU64::new(0),
            last_snapshot: Arc::new(RwLock::new(MetricsSnapshot {
                timestamp: Instant::now(),
                messages_produced: 0,
                bytes_produced: 0,
            })),
        }
    }
    
    pub fn record_produce(&self, bytes: u64, latency_ms: f64) {
        self.messages_produced.fetch_add(1, Ordering::Relaxed);
        self.bytes_produced.fetch_add(bytes, Ordering::Relaxed);
        self.latency_sum_ms.fetch_add(latency_ms as u64, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_consume(&self, bytes: u64) {
        self.messages_consumed.fetch_add(1, Ordering::Relaxed);
        self.bytes_consumed.fetch_add(bytes, Ordering::Relaxed);
    }
    
    pub fn add_producer(&self) {
        self.producer_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn remove_producer(&self) {
        self.producer_count.fetch_sub(1, Ordering::Relaxed);
    }
    
    pub fn add_consumer(&self) {
        self.consumer_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn remove_consumer(&self) {
        self.consumer_count.fetch_sub(1, Ordering::Relaxed);
    }
    
    pub async fn get_rates(&self) -> (u64, u64) {
        let mut snapshot = self.last_snapshot.write().await;
        let now = Instant::now();
        let elapsed = now.duration_since(snapshot.timestamp).as_secs_f64();
        
        if elapsed == 0.0 {
            return (0, 0);
        }
        
        let current_messages = self.messages_produced.load(Ordering::Relaxed);
        let current_bytes = self.bytes_produced.load(Ordering::Relaxed);
        
        let message_rate = ((current_messages - snapshot.messages_produced) as f64 / elapsed) as u64;
        let byte_rate = ((current_bytes - snapshot.bytes_produced) as f64 / elapsed) as u64;
        
        snapshot.timestamp = now;
        snapshot.messages_produced = current_messages;
        snapshot.bytes_produced = current_bytes;
        
        (message_rate, byte_rate)
    }
    
    pub fn get_avg_latency_ms(&self) -> f64 {
        let sum = self.latency_sum_ms.load(Ordering::Relaxed);
        let count = self.latency_count.load(Ordering::Relaxed);
        
        if count == 0 {
            return 0.0;
        }
        
        sum as f64 / count as f64
    }
}

// Add to BrokerService
pub struct BrokerService {
    // ... existing fields
    
    // NEW: Per-topic metrics
    topic_metrics: Arc<RwLock<HashMap<String, Arc<TopicMetrics>>>>,
}

impl BrokerService {
    pub async fn get_topic_metrics(&self, topic_name: &str) -> Option<Arc<TopicMetrics>> {
        self.topic_metrics.read().await.get(topic_name).cloned()
    }
    
    pub async fn ensure_topic_metrics(&self, topic_name: &str) -> Arc<TopicMetrics> {
        let mut metrics = self.topic_metrics.write().await;
        metrics.entry(topic_name.to_string())
            .or_insert_with(|| Arc::new(TopicMetrics::new()))
            .clone()
    }
    
    pub async fn collect_topic_loads(&self) -> Vec<TopicLoad> {
        let topics = self.topic_manager.get_all_topics().await;
        let mut loads = Vec::new();
        
        for topic_name in topics {
            if let Some(metrics) = self.get_topic_metrics(&topic_name).await {
                let (msg_rate, byte_rate) = metrics.get_rates().await;
                
                // Get storage sizes
                let (wal_size, cloud_size) = self.get_topic_storage_size(&topic_name).await;
                
                // Get subscription info
                let subscription_count = self.get_subscription_count(&topic_name).await;
                
                // Get backlog
                let backlog = self.get_topic_backlog(&topic_name).await;
                
                loads.push(TopicLoad {
                    topic_name: topic_name.clone(),
                    message_rate: msg_rate,
                    byte_rate,
                    byte_rate_mbps: (byte_rate as f64 * 8.0) / 1_000_000.0,
                    producer_count: metrics.producer_count.load(Ordering::Relaxed) as usize,
                    consumer_count: metrics.consumer_count.load(Ordering::Relaxed) as usize,
                    subscription_count,
                    disk_size_bytes: wal_size + cloud_size,
                    wal_size_bytes: wal_size,
                    cloud_size_bytes: cloud_size,
                    avg_latency_ms: metrics.get_avg_latency_ms(),
                    p99_latency_ms: metrics.latency_p99_ms.load(Ordering::Relaxed) as f64,
                    oldest_message_age_sec: 0, // TODO: implement
                    backlog_messages: backlog,
                });
            }
        }
        
        loads
    }
    
    async fn get_topic_storage_size(&self, topic_name: &str) -> (u64, u64) {
        // Query WalStorageFactory for sizes
        let wal_size = self.wal_factory.get_wal_size(topic_name).await.unwrap_or(0);
        let cloud_size = self.wal_factory.get_cloud_size(topic_name).await.unwrap_or(0);
        (wal_size, cloud_size)
    }
    
    async fn get_subscription_count(&self, topic_name: &str) -> usize {
        // Query metadata for subscription count
        self.resources.lock().await
            .topic
            .get_subscriptions(topic_name)
            .map(|subs| subs.len())
            .unwrap_or(0)
    }
    
    async fn get_topic_backlog(&self, topic_name: &str) -> u64 {
        // Calculate unconsumed messages
        // TODO: Implement based on cursor positions
        0
    }
}
```

**Verification:**
- [ ] Metrics collection compiles
- [ ] No performance impact on produce/consume path (<1% overhead)
- [ ] Metrics are thread-safe

---

### Step 3: Instrument Producer/Consumer Paths
**Status:** ☐ Not Started

**Task:** Add metric recording to message flow

**In producer handler:**
```rust
// broker_service.rs - produce message
pub async fn produce(&self, topic: &str, message: Message) -> Result<()> {
    let start = Instant::now();
    
    // Existing produce logic...
    let result = self.topic_manager.append_message(topic, message).await;
    
    // Record metrics
    if result.is_ok() {
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        let metrics = self.ensure_topic_metrics(topic).await;
        metrics.record_produce(message.payload.len() as u64, latency_ms);
    }
    
    result
}
```

**In consumer handler:**
```rust
// broker_service.rs - consume message
pub async fn consume(&self, topic: &str, subscription: &str) -> Result<Message> {
    let message = self.topic_manager.read_message(topic, subscription).await?;
    
    // Record metrics
    let metrics = self.ensure_topic_metrics(topic).await;
    metrics.record_consume(message.payload.len() as u64);
    
    Ok(message)
}
```

**In producer/consumer connection tracking:**
```rust
// On producer connect
pub async fn register_producer(&self, topic: &str, producer_id: u64) {
    let metrics = self.ensure_topic_metrics(topic).await;
    metrics.add_producer();
}

// On producer disconnect
pub async fn unregister_producer(&self, topic: &str, producer_id: u64) {
    if let Some(metrics) = self.get_topic_metrics(topic).await {
        metrics.remove_producer();
    }
}

// Similar for consumers...
```

**Verification:**
- [ ] Metrics are recorded on produce
- [ ] Metrics are recorded on consume
- [ ] Connection counts are accurate
- [ ] No race conditions

---

### Step 4: Update LoadReport Generation
**Status:** ☐ Not Started

**Task:** Modify `generate_load_report()` to include topic metrics

```rust
pub(crate) async fn generate_load_report(
    broker_service: &BrokerService,
    broker_id: u64,
) -> LoadReport {
    let monitor = get_resource_monitor().await;
    
    // System-level metrics (from Phase 1)
    let cpu_usage = monitor.get_cpu_usage().await.unwrap_or(0.0);
    let mem_usage = monitor.get_memory_usage().await.unwrap_or(0.0);
    let disk_io = monitor.get_disk_io().await.ok();
    let net_io = monitor.get_network_io().await.ok();
    
    let mut resources = vec![
        SystemLoad { resource: ResourceType::CPU, usage: cpu_usage },
        SystemLoad { resource: ResourceType::Memory, usage: mem_usage },
    ];
    
    if let Some(disk) = disk_io {
        resources.push(SystemLoad {
            resource: ResourceType::DiskIO,
            usage: disk.total_bytes_per_sec() as f64,
        });
    }
    
    if let Some(net) = net_io {
        resources.push(SystemLoad {
            resource: ResourceType::NetworkIO,
            usage: net.total_bytes_per_sec() as f64,
        });
    }
    
    // Topic-level metrics (NEW)
    let topics = broker_service.collect_topic_loads().await;
    
    // Calculate aggregates
    let total_throughput_mbps: f64 = topics.iter()
        .map(|t| t.byte_rate_mbps)
        .sum();
    
    let total_disk_usage_gb: f64 = topics.iter()
        .map(|t| t.disk_size_bytes as f64 / 1_000_000_000.0)
        .sum();
    
    let total_message_rate: u64 = topics.iter()
        .map(|t| t.message_rate)
        .sum();
    
    LoadReport {
        resources_usage: resources,
        topics,
        total_throughput_mbps,
        total_disk_usage_gb,
        total_message_rate,
        timestamp: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        broker_id,
    }
}
```

**Verification:**
- [ ] LoadReport includes topic metrics
- [ ] Aggregates are calculated correctly
- [ ] No performance degradation

---

### Step 5: Implement Weighted Composite Ranking
**Status:** ☐ Not Started

**Task:** Create advanced ranking algorithm in `rankings.rs`

```rust
use super::{LoadReport, ResourceType};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct RankingWeights {
    pub cpu: f64,
    pub memory: f64,
    pub disk_io: f64,
    pub network_io: f64,
    pub throughput: f64,
    pub topic_count: f64,
    pub disk_usage: f64,
}

impl Default for RankingWeights {
    fn default() -> Self {
        Self {
            cpu: 0.25,
            memory: 0.20,
            disk_io: 0.15,
            network_io: 0.10,
            throughput: 0.15,
            topic_count: 0.10,
            disk_usage: 0.05,
        }
    }
}

pub async fn rankings_weighted_composite(
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
    weights: RankingWeights,
) -> Vec<(u64, f64)> {
    let brokers = brokers_usage.lock().await;
    
    if brokers.is_empty() {
        return vec![];
    }
    
    // Find max values for normalization
    let max_values = calculate_max_values(&brokers);
    
    let mut broker_scores: Vec<(u64, f64)> = brokers
        .iter()
        .map(|(&broker_id, report)| {
            let score = calculate_broker_score(report, &weights, &max_values);
            (broker_id, score)
        })
        .collect();
    
    // Sort by score (ascending - lower is better)
    broker_scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    
    broker_scores
}

struct MaxValues {
    cpu: f64,
    memory: f64,
    disk_io: f64,
    network_io: f64,
    throughput: f64,
    topic_count: f64,
    disk_usage: f64,
}

fn calculate_max_values(brokers: &HashMap<u64, LoadReport>) -> MaxValues {
    let mut max = MaxValues {
        cpu: 1.0,
        memory: 1.0,
        disk_io: 1.0,
        network_io: 1.0,
        throughput: 1.0,
        topic_count: 1.0,
        disk_usage: 1.0,
    };
    
    for report in brokers.values() {
        // CPU
        if let Some(cpu) = report.resources_usage.iter()
            .find(|r| matches!(r.resource, ResourceType::CPU))
        {
            max.cpu = max.cpu.max(cpu.usage);
        }
        
        // Memory
        if let Some(mem) = report.resources_usage.iter()
            .find(|r| matches!(r.resource, ResourceType::Memory))
        {
            max.memory = max.memory.max(mem.usage);
        }
        
        // Disk I/O
        if let Some(disk) = report.resources_usage.iter()
            .find(|r| matches!(r.resource, ResourceType::DiskIO))
        {
            max.disk_io = max.disk_io.max(disk.usage);
        }
        
        // Network I/O
        if let Some(net) = report.resources_usage.iter()
            .find(|r| matches!(r.resource, ResourceType::NetworkIO))
        {
            max.network_io = max.network_io.max(net.usage);
        }
        
        // Aggregates
        max.throughput = max.throughput.max(report.total_throughput_mbps);
        max.topic_count = max.topic_count.max(report.topics.len() as f64);
        max.disk_usage = max.disk_usage.max(report.total_disk_usage_gb);
    }
    
    max
}

fn calculate_broker_score(
    report: &LoadReport,
    weights: &RankingWeights,
    max_values: &MaxValues,
) -> f64 {
    let mut score = 0.0;
    
    // Normalize and weight each metric
    
    // CPU (0-100%)
    if let Some(cpu) = report.resources_usage.iter()
        .find(|r| matches!(r.resource, ResourceType::CPU))
    {
        score += normalize(cpu.usage, max_values.cpu) * weights.cpu;
    }
    
    // Memory (0-100%)
    if let Some(mem) = report.resources_usage.iter()
        .find(|r| matches!(r.resource, ResourceType::Memory))
    {
        score += normalize(mem.usage, max_values.memory) * weights.memory;
    }
    
    // Disk I/O
    if let Some(disk) = report.resources_usage.iter()
        .find(|r| matches!(r.resource, ResourceType::DiskIO))
    {
        score += normalize(disk.usage, max_values.disk_io) * weights.disk_io;
    }
    
    // Network I/O
    if let Some(net) = report.resources_usage.iter()
        .find(|r| matches!(r.resource, ResourceType::NetworkIO))
    {
        score += normalize(net.usage, max_values.network_io) * weights.network_io;
    }
    
    // Throughput
    score += normalize(report.total_throughput_mbps, max_values.throughput) * weights.throughput;
    
    // Topic count
    score += normalize(report.topics.len() as f64, max_values.topic_count) * weights.topic_count;
    
    // Disk usage
    score += normalize(report.total_disk_usage_gb, max_values.disk_usage) * weights.disk_usage;
    
    score
}

fn normalize(value: f64, max: f64) -> f64 {
    if max == 0.0 {
        0.0
    } else {
        (value / max).min(1.0)
    }
}
```

**Verification:**
- [ ] Rankings are calculated correctly
- [ ] Normalization works
- [ ] Weights sum to ~1.0
- [ ] Lower scores = less loaded brokers

---

### Step 6: Make Ranking Algorithm Configurable
**Status:** ☐ Not Started

**Task:** Add configuration support in `config/danube_broker.yml`

```yaml
# Load Manager Configuration
load_manager:
  # Ranking algorithm: simple (topic count only) or weighted_composite (multi-factor)
  ranking_algorithm: "weighted_composite"
  
  # Weights for composite ranking (must sum to 1.0)
  ranking_weights:
    cpu: 0.25
    memory: 0.20
    disk_io: 0.15
    network_io: 0.10
    throughput: 0.15
    topic_count: 0.10
    disk_usage: 0.05
```

**Add to ServiceConfiguration:**
```rust
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfiguration {
    // ... existing fields
    
    #[serde(default)]
    pub load_manager: LoadManagerConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoadManagerConfig {
    #[serde(default = "default_ranking_algorithm")]
    pub ranking_algorithm: String,
    
    #[serde(default)]
    pub ranking_weights: RankingWeights,
}

impl Default for LoadManagerConfig {
    fn default() -> Self {
        Self {
            ranking_algorithm: "weighted_composite".to_string(),
            ranking_weights: RankingWeights::default(),
        }
    }
}

fn default_ranking_algorithm() -> String {
    "weighted_composite".to_string()
}
```

**Update LoadManager:**
```rust
impl LoadManager {
    async fn calculate_rankings(&self, config: &LoadManagerConfig) {
        let broker_loads = match config.ranking_algorithm.as_str() {
            "simple" => rankings_simple(self.brokers_usage.clone()).await,
            "weighted_composite" => {
                rankings_weighted_composite(
                    self.brokers_usage.clone(),
                    config.ranking_weights.clone()
                ).await
            }
            _ => {
                warn!("Unknown ranking algorithm: {}, using simple", config.ranking_algorithm);
                rankings_simple(self.brokers_usage.clone()).await
            }
        };
        
        *self.rankings.lock().await = broker_loads;
    }
}
```

**Verification:**
- [ ] Configuration loads correctly
- [ ] Algorithm selection works
- [ ] Weights are validated
- [ ] Default values are sensible

---

### Step 7: Add Helper Methods to LoadReport
**Status:** ☐ Not Started

**Task:** Add utility methods for querying LoadReport

```rust
impl LoadReport {
    pub fn get_resource_usage(&self, resource_type: ResourceType) -> f64 {
        self.resources_usage.iter()
            .find(|r| std::mem::discriminant(&r.resource) == std::mem::discriminant(&resource_type))
            .map(|r| r.usage)
            .unwrap_or(0.0)
    }
    
    pub fn get_topic_by_name(&self, name: &str) -> Option<&TopicLoad> {
        self.topics.iter().find(|t| t.topic_name == name)
    }
    
    pub fn get_heaviest_topics(&self, count: usize) -> Vec<&TopicLoad> {
        let mut topics: Vec<_> = self.topics.iter().collect();
        topics.sort_by(|a, b| {
            b.estimated_load_score()
                .partial_cmp(&a.estimated_load_score())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        topics.into_iter().take(count).collect()
    }
    
    pub fn get_lightest_topics(&self, count: usize) -> Vec<&TopicLoad> {
        let mut topics: Vec<_> = self.topics.iter().collect();
        topics.sort_by(|a, b| {
            a.estimated_load_score()
                .partial_cmp(&b.estimated_load_score())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        topics.into_iter().take(count).collect()
    }
    
    pub fn total_load_score(&self) -> f64 {
        self.topics.iter()
            .map(|t| t.estimated_load_score())
            .sum()
    }
}
```

**Verification:**
- [ ] Helper methods work correctly
- [ ] Sorting is stable
- [ ] Edge cases handled (empty topics list)

---

### Step 8: Update Tests
**Status:** ☐ Not Started

**Task:** Add comprehensive tests for new functionality

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_weighted_composite_ranking() {
        let load_manager = create_test_load_manager().await;
        
        // Add brokers with different loads
        add_test_broker(&load_manager, 1, 10.0, 20.0, 5).await;
        add_test_broker(&load_manager, 2, 50.0, 60.0, 10).await;
        add_test_broker(&load_manager, 3, 30.0, 40.0, 3).await;
        
        let weights = RankingWeights::default();
        let rankings = rankings_weighted_composite(
            load_manager.brokers_usage.clone(),
            weights
        ).await;
        
        // Broker 1 should be least loaded
        assert_eq!(rankings[0].0, 1);
        // Broker 2 should be most loaded
        assert_eq!(rankings[2].0, 2);
    }
    
    #[tokio::test]
    async fn test_topic_load_score() {
        let topic = TopicLoad {
            topic_name: "/default/test".to_string(),
            message_rate: 1000,
            byte_rate: 1_000_000, // 1 MB/s
            byte_rate_mbps: 8.0,
            producer_count: 2,
            consumer_count: 3,
            subscription_count: 2,
            disk_size_bytes: 1_000_000_000, // 1 GB
            wal_size_bytes: 500_000_000,
            cloud_size_bytes: 500_000_000,
            avg_latency_ms: 10.0,
            p99_latency_ms: 50.0,
            oldest_message_age_sec: 3600,
            backlog_messages: 10000,
        };
        
        let score = topic.estimated_load_score();
        assert!(score > 0.0);
    }
    
    #[tokio::test]
    async fn test_load_report_helpers() {
        let report = create_test_load_report();
        
        // Test get_resource_usage
        let cpu = report.get_resource_usage(ResourceType::CPU);
        assert_eq!(cpu, 50.0);
        
        // Test get_heaviest_topics
        let heavy = report.get_heaviest_topics(2);
        assert_eq!(heavy.len(), 2);
        
        // Test total_load_score
        let total = report.total_load_score();
        assert!(total > 0.0);
    }
}
```

**Verification:**
- [ ] All tests pass
- [ ] Edge cases covered
- [ ] Performance tests added

---

### Step 9: Update Load Report Posting
**Status:** ☐ Not Started

**Task:** Ensure BrokerService posts updated LoadReports

**Update in `danube_service.rs`:**
```rust
async fn post_load_report(
    broker_service: Arc<BrokerService>,
    meta_store: MetadataStorage,
    broker_id: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    
    loop {
        interval.tick().await;
        
        // Generate comprehensive load report
        let load_report = generate_load_report(&broker_service, broker_id).await;
        
        let path = join_path(&[BASE_BROKER_LOAD_PATH, &broker_id.to_string()]);
        
        match serde_json::to_value(&load_report) {
            Ok(value) => {
                if let Err(e) = meta_store.put(&path, value, MetaOptions::None).await {
                    error!(error = %e, "Failed to post load report");
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to serialize load report");
            }
        }
    }
}
```

**Verification:**
- [ ] LoadReports are posted every 30s
- [ ] ETCD receives complete data
- [ ] No serialization errors
- [ ] Size is reasonable (<10KB per report)

---

### Step 10: Documentation
**Status:** ☐ Not Started

**Task:** Document new features

**Files to update:**
- [ ] `danube-broker/README.md`
- [ ] `config/danube_broker.yml` (inline comments)
- [ ] `info/load_manager_architecture.md` (new file)

**Example documentation:**
```markdown
## Topic-Level Metrics

Each broker now tracks detailed per-topic metrics:

### Throughput Metrics
- Message rate (messages/second)
- Byte rate (bytes/second)
- Bandwidth (Mbps)

### Connection Metrics
- Active producers
- Active consumers
- Subscription count

### Storage Metrics
- WAL size
- Cloud storage size
- Total disk usage

### Performance Metrics
- Average latency
- P99 latency
- Backlog size

## Ranking Algorithms

### Simple (Topic Count)
Ranks brokers purely by number of assigned topics.

### Weighted Composite (Default)
Multi-factor ranking considering:
- CPU usage (25%)
- Memory usage (20%)
- Disk I/O (15%)
- Network I/O (10%)
- Throughput (15%)
- Topic count (10%)
- Disk usage (5%)

Configure weights in `danube_broker.yml`.
```

**Verification:**
- [ ] Documentation is complete
- [ ] Examples are accurate
- [ ] Configuration is explained

---

## ✅ Success Criteria

Phase 2 is complete when:

- [ ] All 10 implementation steps marked complete
- [ ] Topic metrics are collected and accurate
- [ ] Weighted composite ranking works
- [ ] Configuration is flexible
- [ ] LoadReports include comprehensive data
- [ ] Tests pass with >80% coverage
- [ ] Documentation is complete
- [ ] No performance degradation (<2% overhead)

---

## 🧪 Testing Checklist

- [ ] Unit tests for TopicMetrics
- [ ] Unit tests for weighted ranking
- [ ] Integration tests with real topics
- [ ] Load tests (1000+ topics)
- [ ] Configuration validation tests
- [ ] Serialization/deserialization tests
- [ ] Backward compatibility tests

---

## 📊 Metrics to Track

- Ranking accuracy (does it match actual load?)
- Metric collection overhead (should be <2%)
- LoadReport size (should be <10KB)
- Latency measurement accuracy
- Memory usage of metric storage

---

## 🔗 Dependencies

- Phase 1 completion (Resource Monitoring)
- Existing broker metrics infrastructure
- Topic manager access to storage sizes
- Metadata store for subscription counts

---

## 📝 Notes

- Keep metric collection lightweight
- Use atomic operations for counters
- Calculate rates lazily (on LoadReport generation)
- Consider metric retention (how long to keep historical data)
- Test with high-throughput topics (>10K msg/s)
