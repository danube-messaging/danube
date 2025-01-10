use crate::message::StreamMessage;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait StorageBackend: Send + Sync + std::fmt::Debug + 'static {
    async fn get_segment(
        &self,
        id: usize,
    ) -> Result<Option<Arc<RwLock<Segment>>>, Box<dyn std::error::Error + Send + Sync>>;
    async fn put_segment(
        &self,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn remove_segment(
        &self,
        id: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
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
}

impl Segment {
    pub fn new(id: usize, capacity: usize) -> Self {
        Self {
            id,
            close_time: 0,
            messages: Vec::with_capacity(capacity),
            current_size: 0,
        }
    }

    pub fn is_full(&self, max_size: usize) -> bool {
        self.current_size >= max_size
    }

    pub fn add_message(&mut self, message: StreamMessage) {
        self.current_size += message.size();
        self.messages.push(message);
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
pub struct S3Config {
    pub bucket: String,
    pub region: String,
}

impl Display for S3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "S3Config(bucket: {}, region: {})",
            self.bucket, self.region
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum StorageConfig {
    #[serde(rename = "inmemory")]
    InMemory,
    #[serde(rename = "disk")]
    Disk(DiskConfig),
    #[serde(rename = "s3")]
    S3(S3Config),
}

impl Display for StorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageConfig::InMemory => write!(f, "InMemory"),
            StorageConfig::Disk(config) => write!(f, "{}", config),
            StorageConfig::S3(config) => write!(f, "{}", config),
        }
    }
}
