use danube_core::storage::StorageBackendError;
use thiserror::Error;
use tonic::codegen::http::uri;

//pub type Result<T> = std::result::Result<T, DiskError>;

#[derive(Debug, Error)]
pub enum PersistentStorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("unable to parse the address: {0}")]
    UrlParseError(#[from] uri::InvalidUri),
}

impl From<PersistentStorageError> for StorageBackendError {
    fn from(err: PersistentStorageError) -> Self {
        match err {
            PersistentStorageError::Io(e) => StorageBackendError::Disk(e.to_string()),
            PersistentStorageError::Bincode(e) => StorageBackendError::Disk(e.to_string()),
            PersistentStorageError::Transport(e) => StorageBackendError::Managed(e.to_string()),
            PersistentStorageError::UrlParseError(e) => StorageBackendError::Managed(e.to_string()),
        }
    }
}
