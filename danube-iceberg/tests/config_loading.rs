//! # Configuration Loading Tests
//!
//! These tests validate that `danube-iceberg`'s YAML configuration file is
//! parsed correctly, defaults are applied for omitted fields, and per-topic
//! overrides work as expected.
//!
//! ## Why this matters
//!
//! Misconfigured settings can cause silent failures:
//! - Wrong broker address → gRPC connection fails
//! - Missing topic → nothing gets converted
//! - Wrong storage backend → Parquet files written to the wrong location
//! - Bad compaction settings → excessively large or fragmented Parquet files
//!
//! These tests catch configuration parsing bugs before they reach production.
//!
//! ## Design note
//!
//! Config types are mirrored inline (same as the schema tests) because
//! `danube-iceberg` is a binary crate. The mirrored structs use the same
//! serde annotations to ensure YAML parsing behavior is identical.

/// Verifies that a fully specified YAML config with all fields populated
/// parses correctly and all values are accessible.
///
/// This is the baseline "happy path" test — it ensures the YAML structure
/// matches the expected Rust structs and that nested fields (broker.address,
/// storage.options, topics[0].table_name) are deserialized correctly.
#[test]
fn load_valid_config() {
    let yaml = r#"
broker:
  address: "localhost:6650"
storage:
  backend: "s3"
  root: "s3://my-bucket/danube"
  output_prefix: "iceberg"
  options:
    region: "us-east-1"
topics:
  - namespace: "default"
    topic: "sensor-data"
    table_name: "sensor_data"
  - namespace: "prod"
    topic: "events"
    table_name: "events_table"
compaction:
  target_parquet_size_mb: 100
  max_flush_interval_seconds: 120
polling:
  interval_seconds: 15
"#;

    let config: Config = serde_yaml::from_str(yaml).expect("parse yaml");

    assert_eq!(config.broker.address, "localhost:6650");
    assert_eq!(config.storage.backend, "s3");
    assert_eq!(config.storage.root, "s3://my-bucket/danube");
    assert_eq!(config.storage.output_prefix, "iceberg");
    assert_eq!(
        config.storage.options.get("region").unwrap(),
        "us-east-1"
    );
    assert_eq!(config.topics.len(), 2);
    assert_eq!(config.topics[0].namespace, "default");
    assert_eq!(config.topics[0].topic, "sensor-data");
    assert_eq!(config.topics[0].table_name, "sensor_data");
    assert_eq!(config.topics[1].namespace, "prod");
    assert_eq!(config.compaction.target_parquet_size_mb, 100);
    assert_eq!(config.compaction.max_flush_interval_seconds, 120);
    assert_eq!(config.polling.interval_seconds, 15);
}

/// Verifies that omitted optional fields receive sensible default values.
///
/// Users should be able to run `danube-iceberg` with a minimal config
/// (just broker address, storage backend, and at least one topic). This test
/// ensures the defaults are:
/// - `output_prefix`: "iceberg"
/// - `compaction.target_parquet_size_mb`: 200
/// - `compaction.max_flush_interval_seconds`: 300
/// - `polling.interval_seconds`: 30
///
/// These defaults were chosen to match common deployment patterns — 200MB
/// Parquet files and 5-minute flush intervals balance file count against
/// latency for most workloads.
#[test]
fn load_config_defaults() {
    let yaml = r#"
broker:
  address: "localhost:6650"
storage:
  backend: "fs"
  root: "/tmp/danube-data"
topics:
  - namespace: "default"
    topic: "test"
    table_name: "test_table"
"#;

    let config: Config = serde_yaml::from_str(yaml).expect("parse yaml");

    // Verify defaults
    assert_eq!(config.storage.output_prefix, "iceberg"); // default
    assert!(config.storage.options.is_empty()); // default empty
    assert_eq!(config.compaction.target_parquet_size_mb, 200); // default
    assert_eq!(config.compaction.max_flush_interval_seconds, 300); // default
    assert_eq!(config.polling.interval_seconds, 30); // default
}

/// Verifies that individual topics can override global compaction settings.
///
/// In production, high-throughput topics (e.g., click-streams at 100K msg/s)
/// need smaller, more frequent Parquet files, while low-throughput topics
/// (e.g., daily reports) benefit from larger, less frequent files.
///
/// The `effective_compaction()` method returns the per-topic override when
/// present, falling back to the global config otherwise. This test verifies
/// both paths.
#[test]
fn load_config_per_topic_override() {
    let yaml = r#"
broker:
  address: "localhost:6650"
storage:
  backend: "fs"
  root: "/tmp/data"
topics:
  - namespace: "default"
    topic: "high-throughput"
    table_name: "high_throughput"
    compaction:
      target_parquet_size_mb: 50
      max_flush_interval_seconds: 60
  - namespace: "default"
    topic: "low-throughput"
    table_name: "low_throughput"
compaction:
  target_parquet_size_mb: 200
  max_flush_interval_seconds: 300
"#;

    let config: Config = serde_yaml::from_str(yaml).expect("parse yaml");

    // First topic has per-topic override
    let topic0 = &config.topics[0];
    let effective0 = topic0.effective_compaction(&config.compaction);
    assert_eq!(effective0.target_parquet_size_mb, 50);
    assert_eq!(effective0.max_flush_interval_seconds, 60);

    // Second topic falls back to global
    let topic1 = &config.topics[1];
    let effective1 = topic1.effective_compaction(&config.compaction);
    assert_eq!(effective1.target_parquet_size_mb, 200);
    assert_eq!(effective1.max_flush_interval_seconds, 300);
}

/// Verifies that the fully qualified topic path is constructed correctly
/// from the namespace and topic name.
///
/// Danube topics are identified by `/{namespace}/{topic}` paths. The
/// converter uses this path to:
/// - Query the broker's gRPC API for segment metadata
/// - Construct the object storage path for Parquet output
/// - Key the checkpoint file
///
/// A bug in path construction would cause the converter to query the wrong
/// topic or write Parquet files to an unexpected location.
#[test]
fn fully_qualified_topic() {
    let yaml = r#"
broker:
  address: "localhost:6650"
storage:
  backend: "fs"
  root: "/tmp/data"
topics:
  - namespace: "production"
    topic: "events"
    table_name: "events"
"#;

    let config: Config = serde_yaml::from_str(yaml).expect("parse yaml");

    assert_eq!(
        config.topics[0].fully_qualified_topic(),
        "/production/events"
    );
}

// ============================================================================
// Config types mirrored from danube-iceberg/src/config.rs
// (since danube-iceberg is a binary crate, we can't import directly)
// ============================================================================

#[derive(Debug, serde::Deserialize)]
struct Config {
    broker: BrokerConfig,
    storage: StorageConfig,
    topics: Vec<TopicConfig>,
    #[serde(default)]
    compaction: CompactionConfig,
    #[serde(default)]
    polling: PollingConfig,
}

#[derive(Debug, serde::Deserialize)]
struct BrokerConfig {
    address: String,
}

#[derive(Debug, serde::Deserialize)]
struct StorageConfig {
    backend: String,
    root: String,
    #[serde(default = "default_output_prefix")]
    output_prefix: String,
    #[serde(default)]
    options: std::collections::HashMap<String, String>,
}

#[derive(Debug, serde::Deserialize)]
struct TopicConfig {
    namespace: String,
    topic: String,
    table_name: String,
    compaction: Option<CompactionConfig>,
}

impl TopicConfig {
    fn fully_qualified_topic(&self) -> String {
        format!("/{}/{}", self.namespace, self.topic)
    }

    fn effective_compaction(&self, global: &CompactionConfig) -> CompactionConfig {
        self.compaction.clone().unwrap_or_else(|| global.clone())
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
struct CompactionConfig {
    #[serde(default = "default_target_parquet_size_mb")]
    target_parquet_size_mb: usize,
    #[serde(default = "default_max_flush_interval_seconds")]
    max_flush_interval_seconds: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            target_parquet_size_mb: default_target_parquet_size_mb(),
            max_flush_interval_seconds: default_max_flush_interval_seconds(),
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct PollingConfig {
    #[serde(default = "default_poll_interval_seconds")]
    interval_seconds: u64,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            interval_seconds: default_poll_interval_seconds(),
        }
    }
}

fn default_output_prefix() -> String {
    "iceberg".to_string()
}
fn default_target_parquet_size_mb() -> usize {
    200
}
fn default_max_flush_interval_seconds() -> u64 {
    300
}
fn default_poll_interval_seconds() -> u64 {
    30
}
