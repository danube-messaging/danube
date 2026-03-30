use crate::security::authn::claims::Claims;
use crate::security::authn::jwt::create_token;
use crate::broker_server::DanubeServerImpl;

use danube_core::proto::{auth_service_server::AuthService, AuthRequest, AuthResponse};

use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};
use tracing::Level;

#[tonic::async_trait]
impl AuthService for DanubeServerImpl {
    // finds topic to broker assignment
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn authenticate(
        &self,
        request: Request<AuthRequest>,
    ) -> std::result::Result<Response<AuthResponse>, tonic::Status> {
        let req = request.into_inner();

        let credential = self
            .auth
            .service_account_for_api_key(&req.api_key)
            .ok_or_else(|| Status::invalid_argument("Invalid API key"))?;

        let jwt_config = self
            .auth
            .jwt_config()
            .ok_or_else(|| Status::failed_precondition("JWT support is not enabled"))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("System clock is invalid"))?
            .as_secs();

        let claims = Claims {
            iss: jwt_config.issuer.clone(),
            exp: now + jwt_config.expiration_time,
            sub: credential.name.clone(),
            principal_type: "service_account".to_string(),
            principal_name: credential.name.clone(),
        };

        let token = match create_token(&claims, &jwt_config.secret_key) {
            Ok(token) => token,
            Err(e) => {
                return Err(Status::invalid_argument(format!(
                    "Unable to create JWT token: {}",
                    e
                )));
            }
        };

        let response = AuthResponse { token };
        Ok(Response::new(response))
    }
}
