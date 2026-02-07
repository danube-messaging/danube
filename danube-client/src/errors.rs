use base64::prelude::*;
use prost::Message;
use thiserror::Error;
use tonic::{codegen::http::uri, Status};
use tracing::warn;

use danube_core::proto::{ErrorMessage, ErrorType};

pub type Result<T> = std::result::Result<T, DanubeError>;

#[derive(Debug, Error)]
pub enum DanubeError {
    #[error("transport error: {0}")]
    TonicTransportError(#[from] tonic::transport::Error),

    #[error("from status error: {0}, with message: {1:?}")]
    FromStatus(tonic::Status, Option<ErrorMessage>),

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
            DanubeError::FromStatus(status, _) => Some(status),
            _ => None,
        }
    }
}

pub(crate) fn decode_error_details(status: &Status) -> Option<ErrorMessage> {
    if let Some(metadata_value) = status.metadata().get_bin("error-message-bin") {
        let base64_buffer = metadata_value.as_encoded_bytes();

        // Decode the base64 string to get the original bytes
        let buffer = match BASE64_STANDARD_NO_PAD.decode(base64_buffer) {
            Ok(buf) => buf,
            Err(err) => {
                warn!(error = %err, "error decoding base64 error metadata");
                return None;
            }
        };

        // Decode the protobuf message directly from the metadata bytes
        match ErrorMessage::decode(&buffer[..]) {
            Ok(error_message) => Some(error_message),
            Err(err) => {
                warn!(error = %err, "error decoding error message");
                None
            }
        }
    } else {
        None
    }
}

// A helper function to convert i32 to ErrorType.
#[allow(dead_code)]
pub(crate) fn error_type_from_i32(value: i32) -> Option<ErrorType> {
    match value {
        0 => Some(ErrorType::UnknownError),
        1 => Some(ErrorType::InvalidTopicName),
        2 => Some(ErrorType::TopicNotFound),
        3 => Some(ErrorType::ServiceNotReady),
        4 => Some(ErrorType::ProducerAlreadyExists),
        5 => Some(ErrorType::SubscribePermissionDenied),
        6 => Some(ErrorType::SubscriptionNotFound),
        _ => None,
    }
}
