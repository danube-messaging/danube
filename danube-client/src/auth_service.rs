use crate::{
    connection_manager::ConnectionManager,
    errors::{DanubeError, Result},
};

use danube_core::proto::{auth_service_client::AuthServiceClient, AuthRequest, AuthResponse};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use tonic::{transport::Uri, Request, Response};

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
            .map_err(|status| DanubeError::FromStatus(status, None))?;
        let token = response.into_inner().token;

        // Store the token and its expiry time
        let expiry = Instant::now() + Duration::from_secs(3600); // Assuming token is valid for 1 hour
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
        let mut token_guard = self.token.lock().await;
        let mut expiry_guard = self.token_expiry.lock().await;

        if let Some(expiry) = *expiry_guard {
            if now < expiry {
                if let Some(token) = &*token_guard {
                    return Ok(token.clone());
                }
            }
        }

        // Token is expired or not present, renew it
        let new_token = self.authenticate_client(addr, api_key).await?;
        *token_guard = Some(new_token.clone());
        *expiry_guard = Some(now + Duration::from_secs(3600)); // Assuming token is valid for 1 hour

        Ok(new_token)
    }
}
