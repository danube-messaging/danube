use crate::message::StreamMessage;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

/// Legacy segment-based storage backend trait (deprecated)
/// This trait will be phased out in favor of PersistentStorage
#[async_trait]
pub trait StorageBackend: Send + Sync + std::fmt::Debug + 'static {
    async fn get_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> Result<Option<Arc<RwLock<Segment>>>, StorageBackendError>;
    async fn put_segment(
        &self,
        topic_name: &str,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> Result<(), StorageBackendError>;
    async fn remove_segment(&self, topic_name: &str, id: usize) -> Result<(), StorageBackendError>;
}

/// Modern persistent storage trait for cloud-native backends
/// This trait supports streaming writes, background processing, and cloud object storage
#[async_trait]
pub trait PersistentStorage: Send + Sync + std::fmt::Debug + 'static {
    /// Store a batch of messages for a topic
    /// Messages are written to a Write-Ahead Log for immediate acknowledgment
    /// and asynchronously flushed to persistent storage
    async fn store_messages(
        &self,
        topic_name: &str,
        messages: Vec<StreamMessage>,
    ) -> Result<(), PersistentStorageError>;

    /// Create a message stream for consuming from a topic
    /// Returns a receiver that streams messages from the persistent storage
    async fn create_message_stream(
        &self,
        topic_name: &str,
        start_position: Option<u64>,
    ) -> Result<tokio::sync::mpsc::Receiver<StreamMessage>, PersistentStorageError>;

    /// Get the current write position for a topic
    /// This represents the latest offset that has been written to the WAL
    async fn get_write_position(&self, topic_name: &str) -> Result<u64, PersistentStorageError>;

    /// Get the current committed position for a topic
    /// This represents the latest offset that has been committed to persistent storage
    async fn get_committed_position(&self, topic_name: &str)
        -> Result<u64, PersistentStorageError>;

    /// Create or initialize a topic in the storage backend
    async fn create_topic(&self, topic_name: &str) -> Result<(), PersistentStorageError>;

    /// Delete a topic and all its data from the storage backend
    async fn delete_topic(&self, topic_name: &str) -> Result<(), PersistentStorageError>;

    /// List all topics in the storage backend
    async fn list_topics(&self) -> Result<Vec<String>, PersistentStorageError>;

    /// Shutdown the storage backend gracefully
    async fn shutdown(&self) -> Result<(), PersistentStorageError>;
}

#[derive(Debug, Error)]
pub enum StorageBackendError {
    #[error("Memory storage error: {0}")]
    Memory(String),

    #[error("Disk storage error: {0}")]
    Disk(String),

    #[error("Managed storage error: {0}")]
    Managed(String),
}

#[derive(Debug, Error)]
pub enum PersistentStorageError {
    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Object store error: {0}")]
    ObjectStore(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Invalid position: {0}")]
    InvalidPosition(String),
}

/// Segment is a collection of messages, the segment is closed for writing when it's capacity is reached
/// The segment is closed for reading when all subscriptions have acknowledged the segment
/// The segment is immutable after it's closed for writing
/// The messages in the segment are in the order of arrival
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    // Unique segment ID
    pub id: usize,
    // Segment close time, is the time when the segment is closed for writing
    pub close_time: u64,
    // Messages in the segment
    pub messages: Vec<StreamMessage>,
    // Current size of the segment in bytes
    pub current_size: usize,
    // Next entry ID, used to generate unique entry IDs
    pub next_offset: u64,
}

impl Segment {
    pub fn new(id: usize, capacity: usize) -> Self {
        Self {
            id,
            close_time: 0,
            messages: Vec::with_capacity(capacity),
            current_size: 0,
            next_offset: 0,
        }
    }

    pub fn is_full(&self, max_size: usize) -> bool {
        self.current_size >= max_size
    }

    pub fn add_message(&mut self, message: StreamMessage) {
        self.current_size += message.size();
        self.messages.push(message);
        self.next_offset += 1;
    }
}

// --- StorageConfig section ---

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiskConfig {
    pub path: String,
}

impl Display for DiskConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DiskConfig(path: {})", self.path)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RemoteStorageConfig {
    pub endpoint: String,
    pub use_tls: bool,
    pub ca_file: String,
    pub connection_timeout: usize,
}

impl Display for RemoteStorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RemoteStorageConfig(endpoint: {}, connection_timeout: {})",
            self.endpoint, self.connection_timeout
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CacheConfig {
    pub max_capacity: u64,
    pub time_to_idle: u64,
}

impl Display for CacheConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CacheConfig(max_capacity: {}, time_to_idle: {})",
            self.max_capacity, self.time_to_idle
        )
    }
}

/// Iceberg storage configuration for cloud-native persistent storage
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IcebergStorageConfig {
    /// Catalog configuration (AWS Glue, REST, etc.)
    pub catalog_type: String,
    pub catalog_uri: Option<String>,
    pub warehouse_path: String,

    /// Object store configuration
    pub object_store_type: String, // "s3", "gcs", "local", "memory"
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub path_style: Option<bool>,
    pub profile: Option<String>,
    pub allow_anonymous: Option<bool>,
    pub local_path: Option<String>, // For local storage type

    /// WAL configuration
    pub wal_path: String,
    pub wal_segment_size: usize,
    pub wal_sync_mode: String, // "always", "periodic", "never"

    /// Writer configuration
    pub writer_batch_size: usize,
    pub writer_flush_interval_ms: u64,
    pub writer_max_memory_bytes: u64,

    /// Reader configuration
    pub reader_poll_interval_ms: u64,
    pub reader_prefetch_batches: usize,
}

impl Display for IcebergStorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IcebergStorageConfig(catalog: {}, warehouse: {}, object_store: {})",
            self.catalog_type, self.warehouse_path, self.object_store_type
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum StorageConfig {
    #[serde(rename = "inmemory")]
    InMemory { cache: CacheConfig },
    #[serde(rename = "local")]
    Local {
        local_config: DiskConfig,
        cache: CacheConfig,
    },
    #[serde(rename = "remote")]
    Remote {
        remote_config: RemoteStorageConfig,
        cache: CacheConfig,
    },
    #[serde(rename = "iceberg")]
    Iceberg {
        iceberg_config: IcebergStorageConfig,
    },
}

impl Display for StorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageConfig::InMemory { cache } => {
                write!(
                    f,
                    "In-Memory Storage (Cache entries: {}, TTL: {}min)",
                    cache.max_capacity, cache.time_to_idle
                )
            }
            StorageConfig::Local {
                local_config,
                cache,
            } => {
                write!(
                    f,
                    "Local Disk Storage at '{}' (Cache entries: {}, TTL: {}min)",
                    local_config.path, cache.max_capacity, cache.time_to_idle
                )
            }
            StorageConfig::Remote {
                remote_config,
                cache,
            } => {
                write!(
                    f,
                    "Remote Storage at '{}' (Cache entries: {}, TTL: {}min)",
                    remote_config.endpoint, cache.max_capacity, cache.time_to_idle
                )
            }
            StorageConfig::Iceberg { iceberg_config } => {
                write!(f, "Iceberg Storage ({})", iceberg_config)
            }
        }
    }
}
