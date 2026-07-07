//! Configuration for the danube-iceberg converter.
//!
//! Parsed from a YAML config file. Supports per-topic overrides for
//! compaction thresholds.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// Top-level configuration for `danube-iceberg`.
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Broker gRPC endpoint for segment discovery and schema registry.
    pub broker: BrokerConfig,
    /// Object storage backend for reading .dnb1 and writing .parquet.
    pub storage: StorageConfig,
    /// Iceberg catalog configuration (optional — omit for Parquet-only mode).
    #[serde(default)]
    pub catalog: CatalogConfig,
    /// Topics to export (explicit opt-in list).
    pub topics: Vec<TopicConfig>,
    /// Global compaction defaults.
    #[serde(default)]
    pub compaction: CompactionConfig,
    /// Polling configuration.
    #[serde(default)]
    pub polling: PollingConfig,
}

/// Broker connection settings.
#[derive(Debug, Deserialize)]
pub struct BrokerConfig {
    /// gRPC address of any broker in the cluster (e.g., "localhost:6650").
    pub address: String,
}

/// Object storage configuration.
#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    /// Storage backend type: "s3", "gcs", "azure", "fs".
    pub backend: String,
    /// Root path for reading .dnb1 segments (e.g., "s3://my-bucket/danube").
    pub root: String,
    /// Output prefix for Parquet files (e.g., "iceberg").
    #[serde(default = "default_output_prefix")]
    pub output_prefix: String,
    /// Backend-specific options (endpoint, region, credentials, etc.).
    #[serde(default)]
    pub options: std::collections::HashMap<String, String>,
}

/// Per-topic export configuration.
#[derive(Debug, Deserialize)]
pub struct TopicConfig {
    /// Namespace (e.g., "default").
    pub namespace: String,
    /// Topic name (e.g., "sensor-data").
    pub topic: String,
    /// Iceberg table name (e.g., "sensor_data").
    pub table_name: String,
    /// Optional per-topic compaction overrides.
    pub compaction: Option<CompactionConfig>,
}

impl TopicConfig {
    /// Returns the fully qualified topic name: "/{namespace}/{topic}".
    pub fn fully_qualified_topic(&self) -> String {
        format!("/{}/{}", self.namespace, self.topic)
    }

    /// Returns the effective compaction config (per-topic override or global default).
    pub fn effective_compaction(&self, global: &CompactionConfig) -> CompactionConfig {
        self.compaction.clone().unwrap_or_else(|| global.clone())
    }
}

/// Compaction thresholds for Parquet file generation.
#[derive(Debug, Clone, Deserialize)]
pub struct CompactionConfig {
    /// Target Parquet file size in MB (default: 200).
    #[serde(default = "default_target_parquet_size_mb")]
    pub target_parquet_size_mb: usize,
    /// Maximum interval between flushes in seconds (default: 300).
    #[serde(default = "default_max_flush_interval_seconds")]
    pub max_flush_interval_seconds: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            target_parquet_size_mb: default_target_parquet_size_mb(),
            max_flush_interval_seconds: default_max_flush_interval_seconds(),
        }
    }
}

/// Polling configuration.
#[derive(Debug, Deserialize)]
pub struct PollingConfig {
    /// Interval between segment discovery polls in seconds (default: 30).
    #[serde(default = "default_poll_interval_seconds")]
    pub interval_seconds: u64,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            interval_seconds: default_poll_interval_seconds(),
        }
    }
}

/// Iceberg catalog configuration.
///
/// Supported catalog types:
/// - `"rest"` — REST catalog (Nessie, Polaris, Tabular, Snowflake, Databricks Unity)
/// - `"glue"` — AWS Glue Data Catalog
/// - `"s3tables"` — AWS S3 Tables (managed Iceberg)
/// - `"sql"` — SQL-backed catalog (SQLite, PostgreSQL)
/// - `"none"` — Parquet-only mode (no catalog integration)
#[derive(Debug, Deserialize)]
pub struct CatalogConfig {
    /// Catalog type: "rest", "glue", "s3tables", "sql", or "none".
    #[serde(rename = "type", default = "default_catalog_type")]
    pub type_: String,
    /// Catalog name (used as identifier when loading).
    #[serde(default = "default_catalog_name")]
    pub name: String,
    /// Catalog-specific properties passed to the builder's `load()` method.
    ///
    /// Common keys by catalog type:
    /// - REST: `uri`, `warehouse`, `credential`, `token`
    /// - Glue: `warehouse`
    /// - S3Tables: `table_bucket_arn`, `endpoint_url`
    /// - SQL: `uri`, `warehouse`
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl CatalogConfig {
    /// Returns true if catalog integration is disabled (Parquet-only mode).
    pub fn is_disabled(&self) -> bool {
        self.type_ == "none"
    }
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            type_: default_catalog_type(),
            name: default_catalog_name(),
            properties: HashMap::new(),
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

fn default_catalog_type() -> String {
    "none".to_string()
}

fn default_catalog_name() -> String {
    "danube_catalog".to_string()
}

impl Config {
    /// Load configuration from a YAML file.
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let path = Path::new(path);
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read config file {}: {}", path.display(), e))?;
        let config: Config = serde_yaml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse config file {}: {}", path.display(), e))?;

        if config.topics.is_empty() {
            anyhow::bail!("no topics configured — at least one topic must be specified");
        }

        Ok(config)
    }
}
