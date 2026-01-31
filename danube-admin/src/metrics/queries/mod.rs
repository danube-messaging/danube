//! Prometheus query builders and metric fetching functions
//!
//! This module is organized into submodules by domain:
//! - `helpers`: Internal response parsing utilities
//! - `cluster`: Cluster-wide metrics
//! - `broker`: Broker-specific metrics
//! - `topic`: Topic-specific metrics (modular building blocks)
//! - `series`: Time-series range queries

pub mod broker;
pub mod cluster;
mod helpers; // internal only
pub mod series;
pub mod topic;

// Re-export functions used by UI and MCP
pub use broker::{
    fetch_broker_connections, fetch_broker_metrics, fetch_broker_metrics_for_ui,
    fetch_topics_by_broker,
};
pub use cluster::fetch_cluster_metrics;
pub use series::{fetch_topic_series, fetch_topic_series_multi, query_raw};
#[allow(unused_imports)] // Re-exported for external reuse
pub use topic::{
    fetch_latency_histogram, fetch_latency_percentiles, fetch_message_size, fetch_producer_quality,
    fetch_reliable_metrics, fetch_subscription_lag, fetch_topic_active, fetch_topic_core,
    fetch_topic_metrics, fetch_topic_rates,
};
