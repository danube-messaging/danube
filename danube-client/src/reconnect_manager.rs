use crate::{
    errors::{DanubeError, Result},
    retry_manager::RetryManager,
    DanubeClient,
};
use tonic::{metadata::MetadataValue, transport::Uri};

/// Unified reconnection and authentication management
#[derive(Debug, Clone)]
pub struct ReconnectManager {
    pub retry_manager: RetryManager,
}

impl ReconnectManager {
    pub fn new(_client: DanubeClient, max_retries: usize, base_backoff_ms: u64, max_backoff_ms: u64) -> Self {
        Self {
            retry_manager: RetryManager::new(max_retries, base_backoff_ms, max_backoff_ms),
        }
    }

    /// Check if error is retryable
    pub fn is_retryable(&self, error: &DanubeError) -> bool {
        self.retry_manager.is_retryable_error(error)
    }

    /// Insert authentication token into request
    pub async fn insert_auth_token<T>(
        client: &DanubeClient,
        request: &mut tonic::Request<T>,
        addr: &Uri,
    ) -> Result<()> {
        if let Some(api_key) = &client.cnx_manager.connection_options.api_key {
            let token = client.auth_service.get_valid_token(addr, api_key).await?;
            let token_metadata = MetadataValue::try_from(format!("Bearer {}", token))
                .map_err(|_| DanubeError::InvalidToken)?;
            request.metadata_mut().insert("authorization", token_metadata);
        }
        Ok(())
    }
}
