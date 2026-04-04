use crate::broker_server::DanubeServerImpl;

use danube_core::jwt::{create_token, Claims};
use danube_core::proto::{auth_service_server::AuthService, AuthRequest, AuthResponse};

use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};
use tracing::Level;

#[tonic::async_trait]
impl AuthService for DanubeServerImpl {
    /// Token exchange: validates an existing JWT and issues a fresh short-lived one.
    ///
    /// The `api_key` field in AuthRequest is now treated as an existing JWT token.
    /// This RPC is optional — clients can use long-lived tokens generated offline
    /// via `danube-admin security tokens create` without ever calling this RPC.
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn authenticate(
        &self,
        request: Request<AuthRequest>,
    ) -> std::result::Result<Response<AuthResponse>, tonic::Status> {
        let req = request.into_inner();

        let jwt_config = self
            .auth
            .jwt_config()
            .ok_or_else(|| Status::failed_precondition("JWT support is not enabled"))?;

        // Validate the incoming token to extract the principal identity
        let incoming_claims = danube_core::jwt::validate_token(&req.api_key, &jwt_config.secret_key)
            .map_err(|_| Status::unauthenticated("Invalid token"))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("System clock is invalid"))?
            .as_secs();

        // Issue a fresh short-lived token with the same identity
        let claims = Claims {
            iss: jwt_config.issuer.clone(),
            exp: now + jwt_config.expiration_time,
            sub: incoming_claims.sub,
            principal_type: incoming_claims.principal_type,
            principal_name: incoming_claims.principal_name,
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
