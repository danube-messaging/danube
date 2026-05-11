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

    /// How often (ms) the edge sends a heartbeat to the cluster.
    /// Default: 30000 (30 seconds).
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
}

impl EdgeIdentity {
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval_ms)
    }
}

fn default_heartbeat_interval_ms() -> u64 {
    30_000
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

    /// Target Danube topic name (e.g. `/edge1/telemetry`).
    pub danube_topic: String,

    /// Optional schema subject to resolve from the cluster registry.
    /// If set, the edge validates MQTT payloads against this schema
    /// and stamps `schema_id`/`schema_version` on messages.
    /// If absent, messages pass through as raw bytes.
    #[serde(default)]
    pub schema_subject: Option<String>,

    /// Payload validation policy: `"none"` (default) or `"enforce"`.
    /// Only applies when `schema_subject` is set.
    /// - `none`: no payload validation (just stamp schema_id)
    /// - `enforce`: validate payload against schema, reject on failure
    #[serde(default)]
    pub validation_policy: Option<String>,

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
        Self::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse edge config '{}': {}", path, e))
    }

    /// Parse configuration from a YAML string.
    pub fn from_str(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
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

    /// Build the list of (topic_name, schema_subject) declarations for RegisterEdge.
    /// Deduplicated by topic name. Used at bootstrap.
    pub fn topic_declarations(&self) -> Vec<(String, Option<String>)> {
        match &self.mqtt {
            Some(mqtt) => {
                let mut seen = std::collections::HashSet::new();
                mqtt.topic_mappings
                    .iter()
                    .filter(|m| seen.insert(m.danube_topic.clone()))
                    .map(|m| (m.danube_topic.clone(), m.schema_subject.clone()))
                    .collect()
            }
            None => Vec::new(),
        }
    }

    /// Validate that all MQTT topic mappings are under the edge's namespace.
    ///
    /// Returns a list of topics that violate the `/{edge_name}/` prefix constraint.
    /// An empty list means all topics are valid.
    pub fn validate_namespaces(&self) -> Vec<String> {
        let prefix = format!("/{}/", self.edge.edge_name);
        match &self.mqtt {
            Some(mqtt) => mqtt
                .topic_mappings
                .iter()
                .filter(|m| !m.danube_topic.starts_with(&prefix))
                .map(|m| m.danube_topic.clone())
                .collect(),
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
