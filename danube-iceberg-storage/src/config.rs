use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

/// Configuration for Iceberg storage backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergConfig {
    /// Catalog configuration (AWS Glue or REST)
    pub catalog: CatalogConfig,
    /// Object store configuration (S3, MinIO, etc.)
    pub object_store: ObjectStoreConfig,
    /// Write-Ahead Log configuration
    pub wal: WalConfig,
    /// Warehouse path for Iceberg tables
    pub warehouse: String,
    /// Topic writer configuration
    pub writer: WriterConfig,
    /// Topic reader configuration
    pub reader: ReaderConfig,
}

/// Iceberg catalog configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CatalogConfig {
    #[serde(rename = "glue")]
    Glue {
        /// AWS region for Glue catalog
        region: String,
        /// Glue database name
        database: String,
        /// Optional AWS profile
        profile: Option<String>,
    },
    #[serde(rename = "rest")]
    Rest {
        /// REST catalog endpoint URL
        uri: String,
        /// Optional authentication token
        token: Option<String>,
        /// Additional properties
        #[serde(default)]
        properties: HashMap<String, String>,
    },
}

/// Object store configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ObjectStoreConfig {
    #[serde(rename = "s3")]
    S3 {
        /// S3 bucket name
        bucket: String,
        /// AWS region
        region: String,
        /// Optional AWS profile
        profile: Option<String>,
        /// Optional endpoint for S3-compatible stores (MinIO)
        endpoint: Option<String>,
        /// Path-style access (for MinIO compatibility)
        #[serde(default)]
        path_style: bool,
    },
    #[serde(rename = "local")]
    Local {
        /// Local filesystem path
        path: String,
    },
}

/// Write-Ahead Log configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Base directory for WAL files
    pub base_path: String,
    /// Maximum WAL file size in bytes (default: 64MB)
    #[serde(default = "default_wal_max_size")]
    pub max_file_size: u64,
    /// Sync mode for durability
    #[serde(default)]
    pub sync_mode: SyncMode,
}

/// Topic writer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterConfig {
    /// Batch size for flushing to Iceberg (number of messages)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum time to wait before flushing (milliseconds)
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    /// Maximum memory usage per topic writer (bytes)
    #[serde(default = "default_max_memory")]
    pub max_memory_bytes: u64,
}

/// Topic reader configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReaderConfig {
    /// Polling interval for new snapshots (milliseconds)
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Maximum number of concurrent file reads
    #[serde(default = "default_max_concurrent_reads")]
    pub max_concurrent_reads: usize,
    /// Prefetch buffer size (number of record batches)
    #[serde(default = "default_prefetch_size")]
    pub prefetch_size: usize,
}

/// WAL sync mode for durability vs performance tradeoff
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum SyncMode {
    /// Sync after every write (highest durability, lowest performance)
    #[serde(rename = "always")]
    Always,
    /// Sync periodically (balanced durability and performance)
    #[serde(rename = "periodic")]
    #[default]
    Periodic,
    /// No explicit sync (highest performance, lowest durability)
    #[serde(rename = "none")]
    None,
}

// Default values
fn default_wal_max_size() -> u64 {
    64 * 1024 * 1024 // 64MB
}

fn default_batch_size() -> usize {
    1000
}

fn default_flush_interval_ms() -> u64 {
    5000 // 5 seconds
}

fn default_max_memory() -> u64 {
    128 * 1024 * 1024 // 128MB
}

fn default_poll_interval_ms() -> u64 {
    1000 // 1 second
}

fn default_max_concurrent_reads() -> usize {
    10
}

fn default_prefetch_size() -> usize {
    5
}

impl Display for IcebergConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Iceberg Storage (Catalog: {}, Object Store: {}, WAL: {}, Warehouse: {})",
            self.catalog, self.object_store, self.wal.base_path, self.warehouse
        )
    }
}

impl Display for CatalogConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogConfig::Glue {
                region, database, ..
            } => {
                write!(f, "AWS Glue ({}/{})", region, database)
            }
            CatalogConfig::Rest { uri, .. } => {
                write!(f, "REST ({})", uri)
            }
        }
    }
}

impl Display for ObjectStoreConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectStoreConfig::S3 { bucket, region, .. } => {
                write!(f, "S3 ({}/{})", region, bucket)
            }
            ObjectStoreConfig::Local { path } => {
                write!(f, "Local ({})", path)
            }
        }
    }
}
