use std::time::Duration;
use tokio::time::sleep;
use rand::{thread_rng, Rng};
use tonic::{Code, Status};
use crate::errors::{decode_error_details, DanubeError};

/// Centralized retry management with exponential backoff and jitter
#[derive(Debug, Clone)]
pub struct RetryManager {
    max_retries: usize,
    base_backoff_ms: u64,
    max_backoff_ms: u64,
}

impl RetryManager {
    pub fn new(max_retries: usize, base_backoff_ms: u64, max_backoff_ms: u64) -> Self {
        Self {
            max_retries: if max_retries == 0 { 5 } else { max_retries },
            base_backoff_ms: if base_backoff_ms == 0 { 200 } else { base_backoff_ms },
            max_backoff_ms: if max_backoff_ms == 0 { 5_000 } else { max_backoff_ms },
        }
    }

    /// Execute an operation with retry logic
    pub async fn retry_with_backoff<F, Fut, T>(&self, mut operation: F) -> Result<T, DanubeError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, DanubeError>>,
    {
        let mut attempts = 0;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    attempts += 1;
                    
                    if !self.is_retryable_error(&error) || attempts > self.max_retries {
                        return Err(error);
                    }

                    let backoff = self.calculate_backoff(attempts - 1);
                    sleep(backoff).await;
                }
            }
        }
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

    /// Calculate exponential backoff with full jitter
    pub fn calculate_backoff(&self, attempt: usize) -> Duration {
        let pow = 1u64.checked_shl(attempt.min(16) as u32).unwrap_or(u64::MAX);
        let exp = self.base_backoff_ms.saturating_mul(pow);
        let backoff = exp.min(self.max_backoff_ms);
        let jitter = thread_rng().gen_range(0..=backoff);
        Duration::from_millis(jitter)
    }
}

/// Convert gRPC Status to DanubeError with proper error details
pub fn status_to_danube_error(status: Status) -> DanubeError {
    let error_message = decode_error_details(&status);
    DanubeError::FromStatus(status, error_message)
}
