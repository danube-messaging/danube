//! Shared metric data types for UI and MCP

use serde::Serialize;

/// Cluster-wide metrics summary
#[derive(Clone, Debug, Serialize)]
pub struct ClusterMetrics {
    pub broker_count: u64,
    pub topics_total: u64,
    pub producers_total: u64,
    pub consumers_total: u64,
    pub subscriptions_total: u64,
    pub messages_in_rate: f64,
    pub messages_out_rate: f64,
    pub bytes_in_rate: f64,
    pub bytes_out_rate: f64,
    pub imbalance_cv: f64,
}

/// Per-broker metrics (summary for MCP)
#[derive(Clone, Debug, Serialize)]
pub struct BrokerMetrics {
    pub broker_id: String,
    pub topics_owned: u64,
    pub rpc_total: u64,
    pub producers: u64,
    pub consumers: u64,
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
}

// ===== Modular Broker Metrics (composable building blocks) =====

/// Broker bytes in/out
#[derive(Clone, Debug, Default, Serialize)]
pub struct BrokerBytes {
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
}

/// Broker active producers/consumers
#[derive(Clone, Debug, Default, Serialize)]
pub struct BrokerActive {
    pub producers: u64,
    pub consumers: u64,
}

/// Per-topic metrics (summary for MCP)
#[derive(Clone, Debug, Serialize)]
pub struct TopicMetrics {
    pub topic: String,
    pub messages_in_total: u64,
    pub messages_out_total: u64,
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
    pub producers: u64,
    pub consumers: u64,
    pub subscriptions: u64,
    pub publish_rate_1m: f64,
    pub dispatch_rate_1m: f64,
    pub subscription_lag: u64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
}

// ===== Modular Topic Metrics (composable building blocks) =====

/// Core message/byte counters
#[derive(Clone, Debug, Default, Serialize)]
pub struct TopicCore {
    pub messages_in_total: u64,
    pub messages_out_total: u64,
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
}

/// Active entities on a topic
#[derive(Clone, Debug, Default, Serialize)]
pub struct TopicActive {
    pub producers: u64,
    pub consumers: u64,
    pub subscriptions: u64,
}

/// Message rates (per minute)
#[derive(Clone, Debug, Default, Serialize)]
pub struct TopicRates {
    pub publish_rate_1m: f64,
    pub dispatch_rate_1m: f64,
}

/// Producer send quality metrics
#[derive(Clone, Debug, Default, Serialize)]
pub struct ProducerQuality {
    pub send_ok_total: u64,
    pub send_error_total: u64,
    pub send_error_by_code: Vec<(String, u64)>,
}

/// Message size statistics
#[derive(Clone, Debug, Default, Serialize)]
pub struct MessageSizeStats {
    pub avg_bytes: f64,
    pub sum_bytes: f64,
    pub count: u64,
}

/// Latency percentiles
#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencyPercentiles {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

/// Subscription lag
#[derive(Clone, Debug, Default, Serialize)]
#[allow(dead_code)]
pub struct SubscriptionLag {
    pub total_lag_messages: u64,
}

/// Reliable/persistent topic metrics (WAL + Cloud)
#[derive(Clone, Debug, Default, Serialize)]
pub struct ReliableMetrics {
    pub wal_append_total: u64,
    pub wal_append_bytes_total: u64,
    pub wal_fsync_total: u64,
    pub wal_flush_latency_p50_ms: f64,
    pub wal_flush_latency_p95_ms: f64,
    pub wal_flush_latency_p99_ms: f64,
    pub cloud_upload_bytes_total: u64,
    pub cloud_upload_objects_total: u64,
}

/// Subscription metrics - future use
#[derive(Clone, Debug, Serialize)]
#[allow(dead_code)]
pub struct SubscriptionMetrics {
    pub subscription: String,
    pub topic: String,
    pub consumers: u64,
    pub lag_messages: u64,
}

/// Metric query result with optional errors
#[derive(Clone, Debug, Serialize)]
pub struct MetricResult<T> {
    pub data: T,
    pub errors: Vec<String>,
}

impl<T> MetricResult<T> {
    #[allow(dead_code)]
    pub fn new(data: T) -> Self {
        Self {
            data,
            errors: Vec::new(),
        }
    }

    pub fn with_errors(data: T, errors: Vec<String>) -> Self {
        Self { data, errors }
    }
}
