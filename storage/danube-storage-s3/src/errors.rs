use aws_sdk_s3::primitives::ByteStreamError;
use danube_core::storage::StorageBackendError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, S3Error>;

#[derive(Debug, Error)]
pub enum S3Error {
    #[error("AWS SDK error: {0}")]
    AWS(#[from] aws_sdk_s3::Error),
    #[error("AWS Other error: {0}")]
    AWSOther(String),
    #[error("AWS Client error: {0}")]
    AWSClient(String),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("ByteStream error: {0}")]
    ByteStream(#[from] ByteStreamError),
}

impl From<S3Error> for StorageBackendError {
    fn from(err: S3Error) -> Self {
        match err {
            S3Error::AWS(e) => StorageBackendError::S3(e.to_string()),
            S3Error::AWSOther(e) => StorageBackendError::S3(e),
            S3Error::AWSClient(e) => StorageBackendError::S3(e),
            S3Error::Bincode(e) => StorageBackendError::S3(e.to_string()),
            S3Error::Io(e) => StorageBackendError::S3(e.to_string()),
            S3Error::ByteStream(e) => StorageBackendError::S3(e.to_string()),
        }
    }
}
