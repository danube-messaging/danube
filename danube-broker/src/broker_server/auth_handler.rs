use crate::auth_jwt::{create_token, Claims};
use crate::broker_server::DanubeServerImpl;
use crate::error_message::create_error_status;

use danube_core::proto::{auth_service_server::AuthService, AuthRequest, AuthResponse, ErrorType};

use tonic::{Code, Request, Response};
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

        // Validate API key
        if self.valid_api_keys.contains(&req.api_key) {
            let claims = Claims {
                iss: "example".to_string(),
                exp: 10000000000, // Set the expiration time
            };
            let token = match create_token(&claims, &self.auth.jwt.as_ref().unwrap().secret_key) {
                Ok(token) => token,
                Err(e) => {
                    let error_string = format!("Unable to create JWT token: {}", e);
                    let status = create_error_status(
                        Code::InvalidArgument,
                        ErrorType::UnknownError,
                        &error_string,
                        None,
                    );
                    return Err(status);
                }
            };

            let response = AuthResponse { token };
            Ok(Response::new(response))
        } else {
            let error_string = format!("Invalid API key");
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::UnknownError,
                &error_string,
                None,
            );
            return Err(status);
        }
    }
}
