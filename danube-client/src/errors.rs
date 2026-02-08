use thiserror::Error;
use tonic::codegen::http::uri;

pub type Result<T> = std::result::Result<T, DanubeError>;

#[derive(Debug, Error)]
pub enum DanubeError {
    #[error("transport error: {0}")]
    TonicTransportError(#[from] tonic::transport::Error),

    #[error("gRPC error: {0}")]
    FromStatus(tonic::Status),

    #[error("unable to parse the address: {0}")]
    UrlParseError(#[from] uri::InvalidUri),

    #[error("unable to parse the address")]
    ParseError,

    #[error("unable to load the certificate: {0}")]
    IoError(#[from] std::io::Error),

    #[error("unable to perform operation: {0}")]
    Unrecoverable(String),

    #[error("schema error: {0}")]
    SchemaError(String),

    #[error("invalid token")]
    InvalidToken,
}

impl DanubeError {
    pub fn extract_status(&self) -> Option<&tonic::Status> {
        match self {
            DanubeError::FromStatus(status) => Some(status),
            _ => None,
        }
    }
}
