use crate::{
    errors::{DanubeError, Result},
    DanubeClient,
};
use rand::{rng, Rng};
use std::time::Duration;
use tonic::{transport::Uri, Code, Status};

/// Default maximum number of retry attempts before giving up.
const DEFAULT_MAX_RETRIES: usize = 5;
/// Default base backoff duration in milliseconds (used for linear backoff calculation).
const DEFAULT_BASE_BACKOFF_MS: u64 = 200;
/// Default maximum backoff cap in milliseconds.
const DEFAULT_MAX_BACKOFF_MS: u64 = 5_000;

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
            max_retries: if max_retries == 0 {
                DEFAULT_MAX_RETRIES
            } else {
                max_retries
            },
            base_backoff_ms: if base_backoff_ms == 0 {
                DEFAULT_BASE_BACKOFF_MS
            } else {
                base_backoff_ms
            },
            max_backoff_ms: if max_backoff_ms == 0 {
                DEFAULT_MAX_BACKOFF_MS
            } else {
                max_backoff_ms
            },
        }
    }

    /// Get the maximum number of retries
    pub fn max_retries(&self) -> usize {
        self.max_retries
    }

    /// Insert authentication token into request.
    /// Delegates to [`AuthService::insert_token_if_needed`].
    pub async fn insert_auth_token<T>(
        client: &DanubeClient,
        request: &mut tonic::Request<T>,
        addr: &Uri,
    ) -> Result<()> {
        client
            .auth_service
            .insert_token_if_needed(
                client.cnx_manager.connection_options.api_key.as_deref(),
                request,
                addr,
            )
            .await
    }

    /// Insert proxy routing header into request when proxy mode is active.
    /// The proxy uses this header to route the gRPC call to the correct broker.
    pub fn insert_proxy_header<T>(request: &mut tonic::Request<T>, broker_url: &Uri, proxy: bool) {
        if proxy {
            if let Ok(value) = broker_url.to_string().parse() {
                request.metadata_mut().insert("x-danube-broker-url", value);
            }
        }
    }

    /// Check if an error is retryable based on gRPC status codes.
    ///
    /// Retryable codes:
    /// - `Unavailable` — broker not reachable or topic moved (ServiceNotReady)
    /// - `DeadlineExceeded` — request timed out
    /// - `ResourceExhausted` — rate limited / overloaded
    pub fn is_retryable_error(&self, error: &DanubeError) -> bool {
        match error {
            DanubeError::FromStatus(status) => matches!(
                status.code(),
                Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted
            ),
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

/// Convert gRPC Status to DanubeError
pub fn status_to_danube_error(status: Status) -> DanubeError {
    DanubeError::FromStatus(status)
}
