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
mod tests {
    use super::*;

    const FULL_CONFIG: &str = r##"
edge:
  edge_name: "test-edge"
  cluster_url: "http://cluster:6650"
  token: "secret"
  heartbeat_interval_ms: 15000

replicator:
  batch_size: 50
  batch_timeout_ms: 2000

mqtt:
  listener: "0.0.0.0:1884"
  topic_mappings:
    - mqtt_pattern: "device/+/telemetry"
      danube_topic: "/test-edge/telemetry"
      schema_subject: "telemetry-events"
      extract_attributes:
        device_id: "$1"
    - mqtt_pattern: "#"
      danube_topic: "/test-edge/mqtt"
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
        assert_eq!(config.edge.heartbeat_interval_ms, 15000);
        assert_eq!(
            config.edge.heartbeat_interval(),
            Duration::from_millis(15000)
        );

        assert_eq!(config.replicator.batch_size, 50);
        assert_eq!(config.replicator.batch_timeout_ms, 2000);
        assert_eq!(
            config.replicator.batch_timeout(),
            Duration::from_millis(2000)
        );

        let mqtt = config.mqtt.as_ref().expect("mqtt section present");
        assert_eq!(mqtt.listener, "0.0.0.0:1884");
        assert_eq!(mqtt.topic_mappings.len(), 2);
        assert_eq!(mqtt.topic_mappings[0].mqtt_pattern, "device/+/telemetry");
        assert_eq!(mqtt.topic_mappings[0].danube_topic, "/test-edge/telemetry");
        assert_eq!(
            mqtt.topic_mappings[0].schema_subject,
            Some("telemetry-events".to_string())
        );
        assert_eq!(
            mqtt.topic_mappings[0].extract_attributes.get("device_id"),
            Some(&"$1".to_string())
        );
        // Catch-all has no schema
        assert_eq!(mqtt.topic_mappings[1].schema_subject, None);
        assert_eq!(mqtt.ingestion.batch_size, 200);
        assert_eq!(mqtt.ingestion.batch_timeout_ms, 750);
    }

    #[test]
    fn parse_minimal_config_uses_defaults() {
        let config = EdgeConfig::from_str(MINIMAL_CONFIG).expect("parse minimal config");

        assert_eq!(config.edge.edge_name, "edge-minimal");
        assert_eq!(config.edge.cluster_url, "http://localhost:6650");
        assert_eq!(config.edge.token, ""); // default empty
        assert_eq!(config.edge.heartbeat_interval_ms, 30_000); // default

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
      danube_topic: "/e1/data"
    - mqtt_pattern: "b/+"
      danube_topic: "/e1/data"
    - mqtt_pattern: "c/+"
      danube_topic: "/e1/other"
"#;
        let config = EdgeConfig::from_str(yaml).unwrap();
        let topics = config.mqtt_danube_topics();

        assert_eq!(topics, vec!["/e1/data", "/e1/other"]);
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
      danube_topic: "/e1/all"
"##;
        let config = EdgeConfig::from_str(yaml).unwrap();
        let mqtt = config.mqtt.unwrap();

        assert_eq!(mqtt.listener, "0.0.0.0:1883"); // default
        assert_eq!(mqtt.ingestion.batch_size, 100); // default
        assert_eq!(mqtt.ingestion.batch_timeout_ms, 500); // default
    }

    #[test]
    fn topic_declarations_deduplicates_with_schema() {
        let yaml = r##"
edge:
  edge_name: "e1"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "a/+"
      danube_topic: "/e1/data"
      schema_subject: "data-events"
    - mqtt_pattern: "b/+"
      danube_topic: "/e1/data"
      schema_subject: "data-events"
    - mqtt_pattern: "#"
      danube_topic: "/e1/raw"
"##;
        let config = EdgeConfig::from_str(yaml).unwrap();
        let decls = config.topic_declarations();

        // Deduplicated: 2 unique topics
        assert_eq!(decls.len(), 2);
        assert_eq!(decls[0].0, "/e1/data");
        assert_eq!(decls[0].1, Some("data-events".to_string()));
        assert_eq!(decls[1].0, "/e1/raw");
        assert_eq!(decls[1].1, None);
    }

    #[test]
    fn topic_declarations_empty_without_mqtt() {
        let config = EdgeConfig::from_str(MINIMAL_CONFIG).unwrap();
        assert!(config.topic_declarations().is_empty());
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
        // Verify schema_subject is parsed from the real config
        let mqtt = config.mqtt.as_ref().unwrap();
        assert_eq!(
            mqtt.topic_mappings[0].schema_subject,
            Some("telemetry-events".to_string())
        );
    }

    // --- Namespace validation tests ---

    #[test]
    fn validate_namespaces_all_valid() {
        let yaml = r##"
edge:
  edge_name: "edge1"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "a/+"
      danube_topic: "/edge1/data"
    - mqtt_pattern: "#"
      danube_topic: "/edge1/raw"
"##;
        let config = EdgeConfig::from_str(yaml).unwrap();
        assert!(
            config.validate_namespaces().is_empty(),
            "all topics under /edge1/ should be valid"
        );
    }

    #[test]
    fn validate_namespaces_rejects_wrong_namespace() {
        let yaml = r##"
edge:
  edge_name: "edge1"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "a/+"
      danube_topic: "/edge2/data"
    - mqtt_pattern: "#"
      danube_topic: "/default/sneaky"
"##;
        let config = EdgeConfig::from_str(yaml).unwrap();
        let violations = config.validate_namespaces();
        assert_eq!(violations.len(), 2);
        assert!(violations.contains(&"/edge2/data".to_string()));
        assert!(violations.contains(&"/default/sneaky".to_string()));
    }

    #[test]
    fn validate_namespaces_mixed_valid_and_invalid() {
        let yaml = r##"
edge:
  edge_name: "factory-01"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "sensors/#"
      danube_topic: "/factory-01/telemetry"
    - mqtt_pattern: "alerts/#"
      danube_topic: "/other-edge/alerts"
    - mqtt_pattern: "#"
      danube_topic: "/factory-01/raw"
"##;
        let config = EdgeConfig::from_str(yaml).unwrap();
        let violations = config.validate_namespaces();
        assert_eq!(violations, vec!["/other-edge/alerts"]);
    }

    #[test]
    fn validate_namespaces_no_mqtt_section() {
        let config = EdgeConfig::from_str(MINIMAL_CONFIG).unwrap();
        assert!(config.validate_namespaces().is_empty());
    }
}
