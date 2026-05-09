//! Unified edge configuration.
//!
//! Loaded from a single YAML file (e.g. `config/edge.yaml`) at edge startup.
//! Covers three sections:
//! - `edge` — identity and cluster connectivity
//! - `replicator` — WAL-to-cloud batching parameters
//! - `mqtt` — MQTT gateway (listener, topic mappings, ingestion batching)

use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

/// Root configuration loaded from the edge config file.
#[derive(Debug, Clone, Deserialize)]
pub struct EdgeConfig {
    /// Edge identity and cluster connectivity.
    pub edge: EdgeIdentity,

    /// WAL-to-cloud replication batching parameters.
    #[serde(default)]
    pub replicator: ReplicatorConfig,

    /// MQTT gateway configuration (optional — gateway disabled if absent).
    pub mqtt: Option<MqttSection>,
}

/// Edge identity: name, cluster URL, and authentication.
#[derive(Debug, Clone, Deserialize)]
pub struct EdgeIdentity {
    /// Unique name for this edge broker (e.g. `"edge-factory-01"`).
    pub edge_name: String,

    /// Cluster broker URL for replication (e.g. `"http://cluster:6650"`).
    pub cluster_url: String,

    /// Authentication token (can be overridden via `--edge-token` CLI arg).
    #[serde(default)]
    pub token: String,
}

/// WAL-to-cloud replication batching.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicatorConfig {
    /// Number of messages per batch before sending to cloud.
    #[serde(default = "default_replicator_batch_size")]
    pub batch_size: usize,

    /// Maximum time (ms) to wait before sending a partial batch.
    #[serde(default = "default_replicator_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
}

impl ReplicatorConfig {
    pub fn batch_timeout(&self) -> Duration {
        Duration::from_millis(self.batch_timeout_ms)
    }
}

impl Default for ReplicatorConfig {
    fn default() -> Self {
        Self {
            batch_size: default_replicator_batch_size(),
            batch_timeout_ms: default_replicator_batch_timeout_ms(),
        }
    }
}

fn default_replicator_batch_size() -> usize {
    100
}

fn default_replicator_batch_timeout_ms() -> u64 {
    1000
}

/// MQTT gateway section.
#[derive(Debug, Clone, Deserialize)]
pub struct MqttSection {
    /// TCP listener address (e.g. `"0.0.0.0:1883"`).
    #[serde(default = "default_mqtt_listener")]
    pub listener: String,

    /// Ordered list of topic mapping rules (first match wins).
    pub topic_mappings: Vec<TopicMapping>,

    /// Ingestion batching configuration.
    #[serde(default)]
    pub ingestion: IngestionConfig,
}

/// A single MQTT-to-Danube topic mapping rule.
#[derive(Debug, Clone, Deserialize)]
pub struct TopicMapping {
    /// MQTT topic pattern with `+` (single-level) and `#` (multi-level) wildcards.
    pub mqtt_pattern: String,

    /// Target Danube topic name (e.g. `/default/telemetry`).
    pub danube_topic: String,

    /// Optional attribute extraction from wildcard captures.
    /// Keys are attribute names, values are `$1`, `$2`, etc.
    #[serde(default)]
    pub extract_attributes: HashMap<String, String>,
}

/// Batching parameters for MQTT WAL ingestion.
#[derive(Debug, Clone, Deserialize)]
pub struct IngestionConfig {
    /// Flush when this many messages accumulate for a single topic.
    #[serde(default = "default_ingestion_batch_size")]
    pub batch_size: usize,

    /// Flush after this timeout (ms) even if `batch_size` is not reached.
    #[serde(default = "default_ingestion_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
}

impl IngestionConfig {
    pub fn batch_timeout(&self) -> Duration {
        Duration::from_millis(self.batch_timeout_ms)
    }
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            batch_size: default_ingestion_batch_size(),
            batch_timeout_ms: default_ingestion_batch_timeout_ms(),
        }
    }
}

fn default_mqtt_listener() -> String {
    "0.0.0.0:1883".to_string()
}

fn default_ingestion_batch_size() -> usize {
    100
}

fn default_ingestion_batch_timeout_ms() -> u64 {
    500
}

impl EdgeConfig {
    /// Load configuration from a YAML file.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read edge config '{}': {}", path, e))?;
        let config: EdgeConfig = serde_yaml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse edge config '{}': {}", path, e))?;
        Ok(config)
    }

    /// Get the list of unique Danube topic names from the MQTT mapping rules.
    /// Used at bootstrap to pre-provision topics and WAL handles.
    pub fn mqtt_danube_topics(&self) -> Vec<String> {
        match &self.mqtt {
            Some(mqtt) => {
                let mut topics: Vec<String> = mqtt
                    .topic_mappings
                    .iter()
                    .map(|m| m.danube_topic.clone())
                    .collect();
                topics.sort();
                topics.dedup();
                topics
            }
            None => Vec::new(),
        }
    }
}
