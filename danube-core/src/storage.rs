use crate::message::StreamMessage;
use async_trait::async_trait;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

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

#[derive(Debug, Error)]
pub enum StorageBackendError {
    #[error("Memory storage error: {0}")]
    Memory(String),

    #[error("Disk storage error: {0}")]
    Disk(String),

    #[error("Managed storage error: {0}")]
    Managed(String),
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
        }
    }
}

// --- WAL-first Persistent Storage primitives (non-breaking addition) ---

/// Start position for a subscription/reader.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum StartPosition {
    /// Start from the current tail (latest offset + 1)
    Latest,
    /// Start from a concrete offset (inclusive)
    Offset(u64),
}

#[derive(Debug, Error)]
pub enum PersistentStorageError {
    #[error("I/O error: {0}")]
    Io(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Cloud storage error: {0}")]
    Cloud(String),

    #[error("Metadata store error: {0}")]
    Metadata(String),

    #[error("Unsupported: {0}")]
    Unsupported(String),

    #[error("Other: {0}")]
    Other(String),
}

/// A boxed async stream of messages for a topic.
pub type TopicStream =
    Pin<Box<dyn Stream<Item = Result<StreamMessage, PersistentStorageError>> + Send>>;

#[async_trait]
pub trait PersistentStorage: Send + Sync + std::fmt::Debug + 'static {
    /// Append a message to a topic and return the assigned offset.
    async fn append_message(
        &self,
        topic_name: &str,
        msg: StreamMessage,
    ) -> Result<u64, PersistentStorageError>;

    /// Create a streaming reader starting from the provided position.
    async fn create_reader(
        &self,
        topic_name: &str,
        start: StartPosition,
    ) -> Result<TopicStream, PersistentStorageError>;

    /// Acknowledge internal checkpoints (used by background uploader/compactor).
    async fn ack_checkpoint(
        &self,
        topic_name: &str,
        up_to_offset: u64,
    ) -> Result<(), PersistentStorageError>;

    /// Optionally force a flush of buffered data.
    async fn flush(&self, topic_name: &str) -> Result<(), PersistentStorageError>;
}
