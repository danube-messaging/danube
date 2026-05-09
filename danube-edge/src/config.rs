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
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL_CONFIG: &str = r##"
edge:
  edge_name: "test-edge"
  cluster_url: "http://cluster:6650"
  token: "secret"

replicator:
  batch_size: 50
  batch_timeout_ms: 2000

mqtt:
  listener: "0.0.0.0:1884"
  topic_mappings:
    - mqtt_pattern: "device/+/telemetry"
      danube_topic: "/default/telemetry"
      extract_attributes:
        device_id: "$1"
    - mqtt_pattern: "#"
      danube_topic: "/default/mqtt"
  ingestion:
    batch_size: 200
    batch_timeout_ms: 750
"##;

    const MINIMAL_CONFIG: &str = r#"
edge:
  edge_name: "edge-minimal"
  cluster_url: "http://localhost:6650"
"#;

    #[test]
    fn parse_full_config() {
        let config = EdgeConfig::from_str(FULL_CONFIG).expect("parse full config");

        assert_eq!(config.edge.edge_name, "test-edge");
        assert_eq!(config.edge.cluster_url, "http://cluster:6650");
        assert_eq!(config.edge.token, "secret");

        assert_eq!(config.replicator.batch_size, 50);
        assert_eq!(config.replicator.batch_timeout_ms, 2000);
        assert_eq!(config.replicator.batch_timeout(), Duration::from_millis(2000));

        let mqtt = config.mqtt.as_ref().expect("mqtt section present");
        assert_eq!(mqtt.listener, "0.0.0.0:1884");
        assert_eq!(mqtt.topic_mappings.len(), 2);
        assert_eq!(mqtt.topic_mappings[0].mqtt_pattern, "device/+/telemetry");
        assert_eq!(mqtt.topic_mappings[0].danube_topic, "/default/telemetry");
        assert_eq!(
            mqtt.topic_mappings[0].extract_attributes.get("device_id"),
            Some(&"$1".to_string())
        );
        assert_eq!(mqtt.ingestion.batch_size, 200);
        assert_eq!(mqtt.ingestion.batch_timeout_ms, 750);
    }

    #[test]
    fn parse_minimal_config_uses_defaults() {
        let config = EdgeConfig::from_str(MINIMAL_CONFIG).expect("parse minimal config");

        assert_eq!(config.edge.edge_name, "edge-minimal");
        assert_eq!(config.edge.cluster_url, "http://localhost:6650");
        assert_eq!(config.edge.token, ""); // default empty

        // Replicator uses defaults
        assert_eq!(config.replicator.batch_size, 100);
        assert_eq!(config.replicator.batch_timeout_ms, 1000);

        // No MQTT section
        assert!(config.mqtt.is_none());
    }

    #[test]
    fn mqtt_danube_topics_deduplicates() {
        let yaml = r#"
edge:
  edge_name: "e1"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "a/+"
      danube_topic: "/default/data"
    - mqtt_pattern: "b/+"
      danube_topic: "/default/data"
    - mqtt_pattern: "c/+"
      danube_topic: "/default/other"
"#;
        let config = EdgeConfig::from_str(yaml).unwrap();
        let topics = config.mqtt_danube_topics();

        assert_eq!(topics, vec!["/default/data", "/default/other"]);
    }

    #[test]
    fn mqtt_danube_topics_empty_without_mqtt() {
        let config = EdgeConfig::from_str(MINIMAL_CONFIG).unwrap();
        assert!(config.mqtt_danube_topics().is_empty());
    }

    #[test]
    fn mqtt_defaults_applied() {
        let yaml = r##"
edge:
  edge_name: "e1"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "#"
      danube_topic: "/default/all"
"##;
        let config = EdgeConfig::from_str(yaml).unwrap();
        let mqtt = config.mqtt.unwrap();

        assert_eq!(mqtt.listener, "0.0.0.0:1883"); // default
        assert_eq!(mqtt.ingestion.batch_size, 100); // default
        assert_eq!(mqtt.ingestion.batch_timeout_ms, 500); // default
    }

    #[test]
    fn config_from_file_loads_actual_config() {
        // Test against the real config/edge.yaml in the repo
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let config_path = format!("{}/../config/edge.yaml", manifest_dir);
        let config = EdgeConfig::from_file(&config_path).expect("load config/edge.yaml");

        assert_eq!(config.edge.edge_name, "edge1");
        assert!(!config.edge.cluster_url.is_empty());
        assert!(config.mqtt.is_some());
    }
}

