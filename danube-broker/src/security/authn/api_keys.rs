use crate::security::error::SecurityError;
use tonic::metadata::MetadataMap;

pub(crate) const INTERNAL_BROKER_HEADER: &str = "x-danube-internal-broker";

pub(crate) fn metadata_internal_broker(
    metadata: &MetadataMap,
) -> Result<Option<&str>, SecurityError> {
    match metadata.get(INTERNAL_BROKER_HEADER) {
        Some(value) => value
            .to_str()
            .map(Some)
            .map_err(|_| SecurityError::InvalidMetadata),
        None => Ok(None),
    }
}
