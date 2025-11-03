use crate::{
    errors::{decode_error_details, DanubeError, Result},
    DanubeClient,
};
use rand::{rng, Rng};
use std::time::Duration;
use tonic::{metadata::MetadataValue, transport::Uri, Code, Status};

/// Centralized retry and reconnection management with backoff, jitter, and authentication
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RetryManager {
    max_retries: usize,
    base_backoff_ms: u64,
    max_backoff_ms: u64,
}

impl RetryManager {
    pub fn new(max_retries: usize, base_backoff_ms: u64, max_backoff_ms: u64) -> Self {
        Self {
            max_retries: if max_retries == 0 { 5 } else { max_retries },
            base_backoff_ms: if base_backoff_ms == 0 {
                200
            } else {
                base_backoff_ms
            },
            max_backoff_ms: if max_backoff_ms == 0 {
                5_000
            } else {
                max_backoff_ms
            },
        }
    }

    /// Get the maximum number of retries
    pub fn max_retries(&self) -> usize {
        self.max_retries
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
            request
                .metadata_mut()
                .insert("authorization", token_metadata);
        }
        Ok(())
    }

    /// Check if an error is retryable based on status codes and error types
    pub fn is_retryable_error(&self, error: &DanubeError) -> bool {
        match error {
            DanubeError::FromStatus(status, error_message) => {
                // Transport-level retryable errors
                let transport_retryable = matches!(
                    status.code(),
                    Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted
                );

                // Service-level retryable errors (SERVICE_NOT_READY = 3)
                let service_retryable = error_message
                    .as_ref()
                    .map(|msg| msg.error_type == 3)
                    .unwrap_or(false);

                transport_retryable || service_retryable
            }
            _ => false,
        }
    }

    /// Calculate linear backoff with jitter
    pub fn calculate_backoff(&self, attempt: usize) -> Duration {
        // Linear backoff: base * (attempt + 1), capped at max
        let linear = self.base_backoff_ms.saturating_mul(attempt as u64 + 1);
        let backoff = linear.min(self.max_backoff_ms);
        let jitter = rng().random_range(backoff / 2..=backoff); // 50-100% jitter
        Duration::from_millis(jitter)
    }
}

/// Convert gRPC Status to DanubeError with proper error details
pub fn status_to_danube_error(status: Status) -> DanubeError {
    let error_message = decode_error_details(&status);
    DanubeError::FromStatus(status, error_message)
}
