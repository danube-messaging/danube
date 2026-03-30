use crate::{
    connection_manager::ConnectionManager,
    errors::{DanubeError, Result},
};

use danube_core::proto::{auth_service_client::AuthServiceClient, AuthRequest, AuthResponse};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use tonic::{metadata::MetadataValue, transport::Uri, Request, Response};

/// Assumed token validity duration in seconds.
const TOKEN_EXPIRY_SECS: u64 = 3600;
const SERVICE_ACCOUNT_API_KEY_HEADER: &str = "x-danube-api-key";
const INTERNAL_BROKER_HEADER: &str = "x-danube-internal-broker";

/// The `AuthService` struct provides methods for authenticating clients with the Danube messaging system.
#[derive(Debug, Clone)]
pub struct AuthService {
    cnx_manager: Arc<ConnectionManager>,
    token: Arc<Mutex<Option<String>>>,
    token_expiry: Arc<Mutex<Option<Instant>>>,
}

impl AuthService {
    pub fn new(cnx_manager: Arc<ConnectionManager>) -> Self {
        AuthService {
            cnx_manager,
            token: Arc::new(Mutex::new(None)),
            token_expiry: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn authenticate_client(&self, addr: &Uri, api_key: &str) -> Result<String> {
        let grpc_cnx = self.cnx_manager.get_connection(addr, addr).await?;

        let mut client = AuthServiceClient::new(grpc_cnx.grpc_cnx.clone());

        let request = Request::new(AuthRequest {
            api_key: api_key.to_string(),
        });

        let response: Response<AuthResponse> = client
            .authenticate(request)
            .await
            .map_err(|status| DanubeError::FromStatus(status))?;
        let token = response.into_inner().token;

        // Store the token and its expiry time
        let expiry = Instant::now() + Duration::from_secs(TOKEN_EXPIRY_SECS);
        {
            let mut token_guard = self.token.lock().await;
            let mut expiry_guard = self.token_expiry.lock().await;
            *token_guard = Some(token.clone());
            *expiry_guard = Some(expiry);
        }

        Ok(token)
    }

    pub async fn get_valid_token(&self, addr: &Uri, api_key: &str) -> Result<String> {
        let now = Instant::now();

        // Fast path: return cached token if still valid (locks released immediately)
        {
            let token_guard = self.token.lock().await;
            let expiry_guard = self.token_expiry.lock().await;

            if let Some(expiry) = *expiry_guard {
                if now < expiry {
                    if let Some(token) = &*token_guard {
                        return Ok(token.clone());
                    }
                }
            }
        } // locks dropped here before the async call

        // Slow path: refresh token without holding any locks
        let new_token = self.authenticate_client(addr, api_key).await?;

        Ok(new_token)
    }

    async fn cached_token(&self) -> Option<String> {
        let now = Instant::now();
        let token_guard = self.token.lock().await;
        let expiry_guard = self.token_expiry.lock().await;

        if let Some(expiry) = *expiry_guard {
            if now < expiry {
                return token_guard.clone();
            }
        }

        None
    }

    /// Insert an authentication token into a gRPC request if an API key is configured.
    ///
    /// This is the single entry point for auth token insertion across the client.
    /// It fetches (or reuses a cached) JWT token and attaches it as a Bearer token
    /// in the request metadata.
    pub async fn insert_token_if_needed<T>(
        &self,
        api_key: Option<&str>,
        request: &mut tonic::Request<T>,
        _addr: &Uri,
    ) -> Result<()> {
        if let Some(token) = self.cached_token().await {
            let token_metadata = MetadataValue::try_from(format!("Bearer {}", token))
                .map_err(|_| DanubeError::InvalidToken)?;
            request
                .metadata_mut()
                .insert("authorization", token_metadata);
        } else if let Some(api_key) = api_key {
            let api_key_metadata =
                MetadataValue::try_from(api_key).map_err(|_| DanubeError::InvalidToken)?;
            request
                .metadata_mut()
                .insert(SERVICE_ACCOUNT_API_KEY_HEADER, api_key_metadata);
        }

        if let Some(internal_broker) = self.cnx_manager.connection_options.internal_broker.as_deref()
        {
            let internal_broker_metadata = MetadataValue::try_from(internal_broker)
                .map_err(|_| DanubeError::InvalidToken)?;
            request
                .metadata_mut()
                .insert(INTERNAL_BROKER_HEADER, internal_broker_metadata);
        }
        Ok(())
    }
}
