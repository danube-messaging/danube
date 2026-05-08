//! MQTT gateway configuration.
//!
//! Loaded from a YAML file (e.g. `config/edge_mqtt.yaml`) at edge startup.
//! Defines the TCP listener address, topic mapping rules, and ingestion
//! batching parameters.

use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

/// Root configuration loaded from the MQTT config file.
#[derive(Debug, Clone, Deserialize)]
pub struct MqttConfig {
    pub mqtt: MqttSection,
}

/// MQTT gateway section.
#[derive(Debug, Clone, Deserialize)]
pub struct MqttSection {
    /// TCP listener address (e.g. `"0.0.0.0:1883"`).
    #[serde(default = "default_listener")]
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

/// Batching parameters for WAL ingestion.
#[derive(Debug, Clone, Deserialize)]
pub struct IngestionConfig {
    /// Flush when this many messages accumulate for a single topic.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Flush after this timeout (ms) even if `batch_size` is not reached.
    #[serde(default = "default_batch_timeout_ms")]
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
            batch_size: default_batch_size(),
            batch_timeout_ms: default_batch_timeout_ms(),
        }
    }
}

fn default_listener() -> String {
    "0.0.0.0:1883".to_string()
}

fn default_batch_size() -> usize {
    100
}

fn default_batch_timeout_ms() -> u64 {
    500
}

impl MqttConfig {
    /// Load configuration from a YAML file.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read MQTT config '{}': {}", path, e))?;
        let config: MqttConfig = serde_yaml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse MQTT config '{}': {}", path, e))?;
        Ok(config)
    }

    /// Get the list of unique Danube topic names from the mapping rules.
    /// Used at bootstrap to pre-provision topics and WAL handles.
    pub fn danube_topics(&self) -> Vec<String> {
        let mut topics: Vec<String> = self
            .mqtt
            .topic_mappings
            .iter()
            .map(|m| m.danube_topic.clone())
            .collect();
        topics.sort();
        topics.dedup();
        topics
    }
}
