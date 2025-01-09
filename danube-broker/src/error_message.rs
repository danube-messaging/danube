use prost::Message;
use tonic::{metadata::MetadataValue, Code, Status};

use danube_core::proto::{ErrorMessage, ErrorType};

pub(crate) fn create_error_status(
    code: Code,
    error_type: ErrorType,
    error_string: &str,
    redirect_to: Option<&str>,
) -> Status {
    let error_message = ErrorMessage {
        error_type: error_type.into(),
        error_message: error_string.to_string(),
        redirect_to: redirect_to.map(|s| s.to_string()).unwrap_or_default(),
    };

    // Serialize error message to a binary format
    let mut buffer = Vec::new();
    error_message.encode(&mut buffer).unwrap();

    // Attach serialized error details as metadata
    let metadata_value = MetadataValue::from_bytes(&buffer);

    let mut status = Status::new(code, error_string);
    status
        .metadata_mut()
        .insert_bin("error-message-bin", metadata_value);

    status
}
