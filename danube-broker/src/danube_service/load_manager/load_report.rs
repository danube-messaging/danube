use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::{Mutex, OnceCell};

use crate::danube_service::metrics_collector::MetricsCollector;
use crate::danube_service::resource_monitor::{create_resource_monitor, ResourceMonitor};

// LoadReport holds information that are required by Load Manager
// to take the topics allocation decision to brokers
//
// The broker periodically reports its load metrics to Metadata Store.
// In this struct can be added any information that can serve to Load Manager
// to take a better decision in the allocation of topics/partitions to brokers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LoadReport {
    // System resource usage metrics (Phase 1)
    pub(crate) resources_usage: Vec<SystemLoad>,
    
    // Topic-level metrics (Phase 2)
    #[serde(default)]
    pub(crate) topics: Vec<TopicLoad>,
    
    // Aggregate metrics (Phase 2)
    #[serde(default)]
    pub(crate) total_throughput_mbps: f64,
    #[serde(default)]
    pub(crate) total_message_rate: u64,
    #[serde(default)]
    pub(crate) total_lag_messages: u64,
    
    // Metadata
    pub(crate) timestamp: u64,
    pub(crate) broker_id: u64,
}

/// Per-topic load information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TopicLoad {
    pub(crate) topic_name: String,
    
    // Throughput metrics (derived from counters)
    pub(crate) message_rate: u64,       // messages/second
    pub(crate) byte_rate: u64,          // bytes/second
    pub(crate) byte_rate_mbps: f64,     // Mbps
    
    // Connection metrics (from gauges)
    pub(crate) producer_count: usize,
    pub(crate) consumer_count: usize,
    pub(crate) subscription_count: usize,
    
    // Lag metrics (from subscription gauges)
    pub(crate) backlog_messages: u64,
}

impl TopicLoad {
    /// Calculate an estimated load score for this topic
    /// Higher score = more loaded
    /// TODO: Use this in weighted ranking when implemented
    #[allow(dead_code)]
    pub(crate) fn estimated_load_score(&self) -> f64 {
        let throughput_score = (self.byte_rate as f64 / 1_000_000.0) * 0.5; // MB/s
        let connection_score = (self.producer_count + self.consumer_count) as f64 * 0.3;
        let backlog_penalty = (self.backlog_messages as f64 / 10_000.0) * 0.2;
        
        throughput_score + connection_score + backlog_penalty
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SystemLoad {
    pub(crate) resource: ResourceType,
    pub(crate) usage: f64,  // Changed from usize to f64 for better precision
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ResourceType {
    CPU,
    Memory,
    DiskIO,
    NetworkIO,
}

// Global resource monitor instance (singleton pattern)
static RESOURCE_MONITOR: OnceCell<Arc<Box<dyn ResourceMonitor>>> = OnceCell::const_new();

async fn get_resource_monitor() -> Arc<Box<dyn ResourceMonitor>> {
    RESOURCE_MONITOR
        .get_or_init(|| async { Arc::new(create_resource_monitor()) })
        .await
        .clone()
}

/// Tracks previous metric snapshot for rate calculation
/// TODO: Wire this up to calculate msg/sec and bytes/sec rates
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct MetricsSnapshot {
    timestamp: Instant,
    // topic_name -> (messages_total, bytes_total)
    topic_counters: HashMap<String, (u64, u64)>,
}

impl MetricsSnapshot {
    fn new() -> Self {
        Self {
            timestamp: Instant::now(),
            topic_counters: HashMap::new(),
        }
    }
}

// Global metrics snapshot for rate calculation
#[allow(dead_code)]
static METRICS_SNAPSHOT: OnceCell<Arc<Mutex<MetricsSnapshot>>> = OnceCell::const_new();

#[allow(dead_code)]
async fn get_metrics_snapshot() -> Arc<Mutex<MetricsSnapshot>> {
    METRICS_SNAPSHOT
        .get_or_init(|| async { Arc::new(Mutex::new(MetricsSnapshot::new())) })
        .await
        .clone()
}

/// Calculate per-second rates from counter deltas
/// TODO: Call this from generate_load_report to populate message_rate and byte_rate
#[allow(dead_code)]
async fn calculate_rates(
    topic_metrics: &HashMap<String, (u64, u64)>, // topic_name -> (messages, bytes)
) -> HashMap<String, (u64, u64)> {
    let mut rates = HashMap::new();
    let snapshot = get_metrics_snapshot().await;
    let mut snap = snapshot.lock().await;
    
    let now = Instant::now();
    let elapsed_secs = now.duration_since(snap.timestamp).as_secs_f64();
    
    // Avoid division by zero on first call or very quick successive calls
    if elapsed_secs < 0.1 {
        return rates;
    }
    
    for (topic_name, &(current_msgs, current_bytes)) in topic_metrics {
        let (prev_msgs, prev_bytes) = snap.topic_counters
            .get(topic_name)
            .copied()
            .unwrap_or((0, 0));
        
        // Calculate rates (messages/sec, bytes/sec)
        let msg_rate = ((current_msgs.saturating_sub(prev_msgs)) as f64 / elapsed_secs) as u64;
        let byte_rate = ((current_bytes.saturating_sub(prev_bytes)) as f64 / elapsed_secs) as u64;
        
        rates.insert(topic_name.clone(), (msg_rate, byte_rate));
        
        // Update snapshot with current values
        snap.topic_counters.insert(topic_name.clone(), (current_msgs, current_bytes));
    }
    
    // Update timestamp
    snap.timestamp = now;
    
    rates
}

/// Generates a LoadReport with real system metrics and topic-level metrics
pub(crate) async fn generate_load_report(
    broker_id: u64,
    topic_list: Vec<String>,
    metrics_collector: &Arc<MetricsCollector>,
) -> LoadReport {
    let monitor = get_resource_monitor().await;
    
    // Collect real system metrics (Phase 1)
    let cpu_usage = monitor.get_cpu_usage().await.unwrap_or(0.0);
    let mem_usage = monitor.get_memory_usage().await.unwrap_or(0.0);
    let disk_io = monitor.get_disk_io().await.ok();
    let net_io = monitor.get_network_io().await.ok();
    
    let mut resources = vec![
        SystemLoad {
            resource: ResourceType::CPU,
            usage: cpu_usage,
        },
        SystemLoad {
            resource: ResourceType::Memory,
            usage: mem_usage,
        },
    ];
    
    // Add disk I/O if available
    if let Some(disk) = disk_io {
        resources.push(SystemLoad {
            resource: ResourceType::DiskIO,
            usage: disk.total_bytes_per_sec() as f64,
        });
    }
    
    // Add network I/O if available
    if let Some(net) = net_io {
        resources.push(SystemLoad {
            resource: ResourceType::NetworkIO,
            usage: net.total_bytes_per_sec() as f64,
        });
    }
    
    // Phase 2: Topic-level metrics collection from MetricsCollector
    let snapshots = metrics_collector.get_all_snapshots().await;
    
    let topics: Vec<TopicLoad> = topic_list
        .iter()
        .filter_map(|topic_name| {
            snapshots.get(topic_name).map(|snapshot| TopicLoad {
                topic_name: topic_name.clone(),
                message_rate: 0,  // TODO: Calculate rate from counter deltas
                byte_rate: 0,     // TODO: Calculate rate from counter deltas
                byte_rate_mbps: 0.0,
                producer_count: snapshot.active_producers,
                consumer_count: snapshot.active_consumers,
                subscription_count: snapshot.active_subscriptions,
                backlog_messages: 0,  // Skipped for now (not needed)
            })
        })
        .collect();
    
    // Calculate aggregates
    let total_throughput_mbps: f64 = topics.iter()
        .map(|t| t.byte_rate_mbps)
        .sum();
    
    let total_message_rate: u64 = topics.iter()
        .map(|t| t.message_rate)
        .sum();
    
    let total_lag_messages: u64 = topics.iter()
        .map(|t| t.backlog_messages)
        .sum();
    
    LoadReport {
        resources_usage: resources,
        topics,
        total_throughput_mbps,
        total_message_rate,
        total_lag_messages,
        timestamp: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        broker_id,
    }
}
