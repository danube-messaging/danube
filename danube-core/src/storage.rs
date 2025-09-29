use crate::message::StreamMessage;
use async_trait::async_trait;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use thiserror::Error;

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

/// A persistent storage interface for a topic.
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
