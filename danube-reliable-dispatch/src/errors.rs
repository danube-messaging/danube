use danube_core::storage::StorageBackendError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ReliableDispatchError>;

#[derive(Debug, Error)]
pub enum ReliableDispatchError {
    #[error("Segment error: {0}")]
    SegmentError(String),

    #[error("Segment not found: {0}")]
    SegmentNotFound(usize),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Acknowledgment error: {0}")]
    AcknowledgmentError(String),

    #[error("Subscription error: {0}")]
    SubscriptionError(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("No messages available")]
    NoMessagesAvailable,

    #[error("Max retries exceeded")]
    MaxRetriesExceeded,
}

impl From<StorageBackendError> for ReliableDispatchError {
    fn from(error: StorageBackendError) -> Self {
        ReliableDispatchError::StorageError(error.to_string())
    }
}
