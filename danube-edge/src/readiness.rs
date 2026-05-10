//! Per-topic readiness tracking for the MQTT ingestion pipeline.
//!
//! A topic is **READY** for MQTT ingestion only when all conditions are met:
//! 1. Local WAL handle is provisioned
//! 2. Cluster has confirmed registration (via `RegisterEdge`)
//! 3. Schema is resolved (if `schema_subject` is configured; auto-true for raw-bytes topics)
//!
//! The `MqttIngester` checks readiness before accepting messages. Messages
//! for not-ready topics are rejected (QoS 1 devices will retry).

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::info;

/// Cached schema information resolved from the cluster registry.
#[derive(Debug, Clone)]
pub struct CachedSchema {
    /// Schema subject name (e.g. `"telemetry-events"`).
    pub subject: String,
    /// Cluster-assigned schema ID.
    pub schema_id: u64,
    /// Schema version number.
    pub schema_version: u32,
    /// Schema type (e.g. `"json_schema"`, `"avro"`).
    pub schema_type: String,
    /// SHA-256 fingerprint for change detection.
    pub fingerprint: String,
    /// Raw schema definition bytes (for future payload validation).
    pub schema_definition: Vec<u8>,
}

/// Per-topic readiness state.
#[derive(Debug, Clone)]
pub struct TopicState {
    /// WAL handle has been created in the ingester.
    pub local_provisioned: bool,
    /// `RegisterEdge` confirmed this topic on the cluster.
    pub cluster_registered: bool,
    /// Schema resolved from cluster (or not needed for raw-bytes topics).
    pub schema_resolved: bool,
    /// Cached schema info (None for raw-bytes topics).
    pub schema_info: Option<CachedSchema>,
}

impl TopicState {
    fn new() -> Self {
        Self {
            local_provisioned: false,
            cluster_registered: false,
            schema_resolved: false,
            schema_info: None,
        }
    }

    /// A topic is ready for MQTT ingestion when all conditions are met.
    pub fn is_ready(&self) -> bool {
        self.local_provisioned && self.cluster_registered && self.schema_resolved
    }
}

/// Central readiness tracker for all edge topics.
///
/// Thread-safe (behind `RwLock`). Shared between `EdgeService` (writes)
/// and `MqttIngester` (reads).
#[derive(Debug, Clone)]
pub struct TopicReadiness {
    topics: Arc<RwLock<HashMap<String, TopicState>>>,
}

impl TopicReadiness {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize tracking for a topic (called during bootstrap).
    pub async fn track_topic(&self, topic_name: &str) {
        let mut topics = self.topics.write().await;
        topics.entry(topic_name.to_string()).or_insert_with(TopicState::new);
    }

    /// Mark a topic as locally provisioned (WAL handle created).
    pub async fn mark_local_provisioned(&self, topic_name: &str) {
        let mut topics = self.topics.write().await;
        if let Some(state) = topics.get_mut(topic_name) {
            state.local_provisioned = true;
        }
    }

    /// Mark a topic as registered on the cluster.
    pub async fn mark_cluster_registered(&self, topic_name: &str) {
        let mut topics = self.topics.write().await;
        if let Some(state) = topics.get_mut(topic_name) {
            state.cluster_registered = true;
        }
    }

    /// Mark a topic's schema as resolved. Pass `None` for raw-bytes topics.
    pub async fn mark_schema_resolved(
        &self,
        topic_name: &str,
        schema: Option<CachedSchema>,
    ) {
        let mut topics = self.topics.write().await;
        if let Some(state) = topics.get_mut(topic_name) {
            state.schema_resolved = true;
            state.schema_info = schema;
            info!(
                topic = %topic_name,
                has_schema = state.schema_info.is_some(),
                "topic schema resolved"
            );
        }
    }

    /// Update the cached schema for a topic (called on heartbeat schema change).
    pub async fn update_schema(&self, topic_name: &str, schema: CachedSchema) {
        let mut topics = self.topics.write().await;
        if let Some(state) = topics.get_mut(topic_name) {
            info!(
                topic = %topic_name,
                subject = %schema.subject,
                version = schema.schema_version,
                "schema updated from cluster"
            );
            state.schema_info = Some(schema);
        }
    }

    /// Mark a topic's schema as unresolved (schema removed from cluster).
    pub async fn mark_schema_unresolved(&self, topic_name: &str) {
        let mut topics = self.topics.write().await;
        if let Some(state) = topics.get_mut(topic_name) {
            state.schema_resolved = false;
            state.schema_info = None;
        }
    }

    /// Check if a topic is ready for MQTT ingestion.
    pub async fn is_ready(&self, topic_name: &str) -> bool {
        let topics = self.topics.read().await;
        topics.get(topic_name).map_or(false, |s| s.is_ready())
    }

    /// Get cached schema info for a topic (for stamping messages).
    pub async fn get_schema_info(&self, topic_name: &str) -> Option<CachedSchema> {
        let topics = self.topics.read().await;
        topics
            .get(topic_name)
            .and_then(|s| s.schema_info.clone())
    }

    /// Get the fingerprint for a topic's cached schema (for heartbeat comparison).
    pub async fn get_fingerprint(&self, topic_name: &str) -> Option<String> {
        let topics = self.topics.read().await;
        topics
            .get(topic_name)
            .and_then(|s| s.schema_info.as_ref())
            .map(|s| s.fingerprint.clone())
    }

    /// Get list of topics that are not yet ready (for retry/logging).
    pub async fn not_ready_topics(&self) -> Vec<String> {
        let topics = self.topics.read().await;
        topics
            .iter()
            .filter(|(_, state)| !state.is_ready())
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Get list of all tracked topics and their readiness state.
    pub async fn all_states(&self) -> Vec<(String, bool)> {
        let topics = self.topics.read().await;
        topics
            .iter()
            .map(|(name, state)| (name.clone(), state.is_ready()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn topic_not_ready_by_default() {
        let readiness = TopicReadiness::new();
        readiness.track_topic("/edge1/test").await;
        assert!(!readiness.is_ready("/edge1/test").await);
    }

    #[tokio::test]
    async fn unknown_topic_is_not_ready() {
        let readiness = TopicReadiness::new();
        assert!(!readiness.is_ready("/edge1/unknown").await);
    }

    #[tokio::test]
    async fn raw_bytes_topic_ready_after_local_and_cluster() {
        let readiness = TopicReadiness::new();
        readiness.track_topic("/edge1/raw").await;

        readiness.mark_local_provisioned("/edge1/raw").await;
        assert!(!readiness.is_ready("/edge1/raw").await);

        readiness.mark_cluster_registered("/edge1/raw").await;
        assert!(!readiness.is_ready("/edge1/raw").await);

        // Raw bytes topic: schema_resolved with None
        readiness
            .mark_schema_resolved("/edge1/raw", None)
            .await;
        assert!(readiness.is_ready("/edge1/raw").await);
        assert!(readiness.get_schema_info("/edge1/raw").await.is_none());
    }

    #[tokio::test]
    async fn schema_topic_ready_after_all_three() {
        let readiness = TopicReadiness::new();
        readiness.track_topic("/edge1/telemetry").await;

        readiness.mark_local_provisioned("/edge1/telemetry").await;
        readiness
            .mark_cluster_registered("/edge1/telemetry")
            .await;

        // Not ready yet — schema not resolved
        assert!(!readiness.is_ready("/edge1/telemetry").await);

        let schema = CachedSchema {
            subject: "telemetry-events".into(),
            schema_id: 42,
            schema_version: 1,
            schema_type: "json_schema".into(),
            fingerprint: "abc123".into(),
            schema_definition: b"{}".to_vec(),
        };
        readiness
            .mark_schema_resolved("/edge1/telemetry", Some(schema))
            .await;

        assert!(readiness.is_ready("/edge1/telemetry").await);
        let info = readiness.get_schema_info("/edge1/telemetry").await.unwrap();
        assert_eq!(info.schema_id, 42);
        assert_eq!(info.schema_version, 1);
    }

    #[tokio::test]
    async fn schema_unresolved_makes_topic_not_ready() {
        let readiness = TopicReadiness::new();
        readiness.track_topic("/edge1/t").await;
        readiness.mark_local_provisioned("/edge1/t").await;
        readiness.mark_cluster_registered("/edge1/t").await;
        readiness
            .mark_schema_resolved(
                "/edge1/t",
                Some(CachedSchema {
                    subject: "s".into(),
                    schema_id: 1,
                    schema_version: 1,
                    schema_type: "json_schema".into(),
                    fingerprint: "x".into(),
                    schema_definition: vec![],
                }),
            )
            .await;
        assert!(readiness.is_ready("/edge1/t").await);

        // Schema removed on cluster
        readiness.mark_schema_unresolved("/edge1/t").await;
        assert!(!readiness.is_ready("/edge1/t").await);
    }

    #[tokio::test]
    async fn not_ready_topics_returns_incomplete() {
        let readiness = TopicReadiness::new();
        readiness.track_topic("/edge1/ready").await;
        readiness.track_topic("/edge1/notready").await;

        // Make "ready" fully ready
        readiness.mark_local_provisioned("/edge1/ready").await;
        readiness.mark_cluster_registered("/edge1/ready").await;
        readiness
            .mark_schema_resolved("/edge1/ready", None)
            .await;

        // "notready" only has local provisioned
        readiness.mark_local_provisioned("/edge1/notready").await;

        let not_ready = readiness.not_ready_topics().await;
        assert_eq!(not_ready, vec!["/edge1/notready"]);
    }
}
