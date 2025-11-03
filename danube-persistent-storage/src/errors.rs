use thiserror::Error;
use tonic::codegen::http::uri;
use tonic::Status;

//pub type Result<T> = std::result::Result<T, DiskError>;

#[derive(Debug, Error)]
pub enum PersistentStorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    #[error("Bincode encode error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),
    #[error("Bincode decode error: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("unable to parse the address: {0}")]
    UrlParseError(#[from] uri::InvalidUri),
    #[error("gRPC status error: {0}")]
    Status(#[from] Status),
}

// Phase D: legacy StorageBackendError removed. Provide a convenient conversion to tonic::Status.
impl From<PersistentStorageError> for Status {
    fn from(err: PersistentStorageError) -> Self {
        Status::internal(err.to_string())
    }
}
