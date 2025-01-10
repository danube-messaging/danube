use danube_core::storage::StorageBackendError;
use thiserror::Error;

//pub type Result<T> = std::result::Result<T, DiskError>;

#[derive(Debug, Error)]
pub enum DiskError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

impl From<DiskError> for StorageBackendError {
    fn from(err: DiskError) -> Self {
        match err {
            DiskError::Io(e) => StorageBackendError::Disk(e.to_string()),
            DiskError::Bincode(e) => StorageBackendError::Disk(e.to_string()),
        }
    }
}
