//! Internal metrics collector for LoadReport generation
//!
//! This module maintains an in-memory snapshot of broker metrics that can be
//! efficiently queried for LoadReport generation. It works alongside the
//! Prometheus metrics (broker_metrics.rs) - both are updated simultaneously.
//!
//! Why not use Prometheus metrics directly?
//! - The `metrics` crate is write-only (no read API)
//! - Scraping our own HTTP endpoint is inefficient
//! - We need real-time access without HTTP overhead

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Internal metrics snapshot for a single topic
#[derive(Debug, Clone, Default)]
pub(crate) struct TopicMetricsSnapshot {
    // Counters (cumulative)
    pub messages_in_total: u64,
    pub bytes_in_total: u64,
    
    // Gauges (current values)
    pub active_producers: usize,
    pub active_consumers: usize,
    pub active_subscriptions: usize,
    
    // Aggregated lag from all subscriptions (not currently used)
    #[allow(dead_code)]
    pub total_backlog_messages: u64,
}

/// Global metrics collector
/// 
/// This structure is updated whenever metrics are recorded. It provides
/// fast read access for LoadReport generation without scraping endpoints.
#[derive(Debug, Clone)]
pub(crate) struct MetricsCollector {
    topics: Arc<RwLock<HashMap<String, TopicMetricsSnapshot>>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Record a message published to a topic
    pub async fn record_message_in(&self, topic_name: &str, bytes: u64) {
        let mut topics = self.topics.write().await;
        let snapshot = topics.entry(topic_name.to_string()).or_default();
        snapshot.messages_in_total += 1;
        snapshot.bytes_in_total += bytes;
    }
    
    /// Update producer count for a topic
    pub async fn set_producer_count(&self, topic_name: &str, count: usize) {
        let mut topics = self.topics.write().await;
        let snapshot = topics.entry(topic_name.to_string()).or_default();
        snapshot.active_producers = count;
    }
    
    /// Update consumer count for a topic
    pub async fn set_consumer_count(&self, topic_name: &str, count: usize) {
        let mut topics = self.topics.write().await;
        let snapshot = topics.entry(topic_name.to_string()).or_default();
        snapshot.active_consumers = count;
    }
    
    /// Update subscription count for a topic
    pub async fn set_subscription_count(&self, topic_name: &str, count: usize) {
        let mut topics = self.topics.write().await;
        let snapshot = topics.entry(topic_name.to_string()).or_default();
        snapshot.active_subscriptions = count;
    }
    
    /// Update total backlog for a topic (aggregate from all subscriptions)
    /// Not currently used - lag tracking was deferred
    #[allow(dead_code)]
    pub async fn set_backlog(&self, topic_name: &str, backlog: u64) {
        let mut topics = self.topics.write().await;
        let snapshot = topics.entry(topic_name.to_string()).or_default();
        snapshot.total_backlog_messages = backlog;
    }
    
    /// Get snapshot for a specific topic
    /// Currently unused - we use get_all_snapshots() instead
    #[allow(dead_code)]
    pub async fn get_topic_snapshot(&self, topic_name: &str) -> Option<TopicMetricsSnapshot> {
        let topics = self.topics.read().await;
        topics.get(topic_name).cloned()
    }
    
    /// Get all topic snapshots
    pub async fn get_all_snapshots(&self) -> HashMap<String, TopicMetricsSnapshot> {
        self.topics.read().await.clone()
    }
    
    /// Remove a topic (when it's deleted)
    /// Not currently called - topics remain in metrics map
    #[allow(dead_code)]
    pub async fn remove_topic(&self, topic_name: &str) {
        let mut topics = self.topics.write().await;
        topics.remove(topic_name);
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}
