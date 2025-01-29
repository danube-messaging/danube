use crate::{
    connection_manager::ConnectionManager,
    errors::{DanubeError, Result},
};

use danube_core::proto::{auth_service_client::AuthServiceClient, AuthRequest, AuthResponse};
use std::sync::Arc;
use tonic::{transport::Uri, Request, Response};

// HealthCheckService is used to validate that the producer/consumer are still served by the connected broker
#[derive(Debug, Clone)]
pub(crate) struct AuthService {
    cnx_manager: Arc<ConnectionManager>,
}

impl AuthService {
    pub fn new(cnx_manager: Arc<ConnectionManager>) -> Self {
        AuthService { cnx_manager }
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

        Ok(token)
    }
}
