use crate::message::StreamMessage;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
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
pub struct ManagedConfig {
    pub endpoint: String,
    pub use_tls: bool,
    pub ca_file: String,
    pub connection_timeout: usize,
}

impl Display for ManagedConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ManagedConfig(endpoint: {}, connection_timeout: {})",
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
    #[serde(rename = "disk")]
    Disk {
        config: DiskConfig,
        cache: CacheConfig,
    },
    #[serde(rename = "managed")]
    Managed {
        config: ManagedConfig,
        cache: CacheConfig,
    },
}

impl Display for StorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageConfig::InMemory { cache } => {
                write!(f, "InMemory with {}", cache)
            }
            StorageConfig::Disk { config, cache } => {
                write!(f, "{} with {}", config, cache)
            }
            StorageConfig::Managed { config, cache } => {
                write!(f, "{} with {}", config, cache)
            }
        }
    }
}
