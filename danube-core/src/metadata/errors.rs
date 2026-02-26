use serde_json;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, MetadataError>;

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("Watch stream error: {0}")]
    WatchError(String),

    #[error("Storage backend error: {0}")]
    StorageError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Transport error: {0}")]
    TransportError(String),

    #[error("Watch operation timed out")]
    WatchTimeout,

    #[error("Watch channel closed")]
    WatchChannelClosed,

    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),

    #[error("Key already exists: {0}")]
    KeyExists(String),

    #[error("Watch cancelled")]
    WatchCancelled,

    #[error("Operation not supported by backend")]
    UnsupportedOperation,

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Unknown error occurred: {0}")]
    Unknown(String),
}
