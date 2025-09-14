use thiserror::Error;

/// Result type for Iceberg storage operations
pub type Result<T> = std::result::Result<T, IcebergStorageError>;

/// Errors that can occur in the Iceberg storage backend
#[derive(Debug, Error)]
pub enum IcebergStorageError {
    #[error("Write-Ahead Log error: {0}")]
    Wal(String),

    #[error("Iceberg catalog error: {0}")]
    Catalog(String),

    #[error("Object store error: {0}")]
    ObjectStore(String),

    #[error("Arrow/Parquet error: {0}")]
    Arrow(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Task join error: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("Channel send error")]
    ChannelSend,

    #[error("Channel receive error")]
    ChannelReceive,
}

impl From<iceberg::Error> for IcebergStorageError {
    fn from(err: iceberg::Error) -> Self {
        IcebergStorageError::Catalog(err.to_string())
    }
}

impl From<arrow_schema::ArrowError> for IcebergStorageError {
    fn from(err: arrow_schema::ArrowError) -> Self {
        IcebergStorageError::Arrow(err.to_string())
    }
}
