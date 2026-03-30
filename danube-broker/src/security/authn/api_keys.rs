use crate::auth::AuthConfig;
use crate::security::error::SecurityError;
use crate::security::principal::Principal;
use tonic::metadata::MetadataMap;

pub(crate) const SERVICE_ACCOUNT_API_KEY_HEADER: &str = "x-danube-api-key";
pub(crate) const INTERNAL_BROKER_HEADER: &str = "x-danube-internal-broker";

pub(crate) fn metadata_api_key(metadata: &MetadataMap) -> Result<Option<&str>, SecurityError> {
    match metadata.get(SERVICE_ACCOUNT_API_KEY_HEADER) {
        Some(value) => value
            .to_str()
            .map(Some)
            .map_err(|_| SecurityError::InvalidMetadata),
        None => Ok(None),
    }
}

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

pub(crate) fn authenticate_service_account(
    auth: &AuthConfig,
    api_key: &str,
) -> Result<Principal, SecurityError> {
    auth.service_account_for_api_key(api_key)
        .map(|credential| Principal::ServiceAccount {
            name: credential.name.clone(),
        })
        .ok_or(SecurityError::InvalidApiKey)
}
