use crate::auth::AuthConfig;
use crate::security::authn::api_keys::metadata_internal_broker;
use crate::security::authn::jwt::{broker_parse_bearer_token, broker_validate_token, principal_from_claims};
use crate::security::context::{AuthenticationMethod, SecurityContext};
use crate::security::error::SecurityError;
use crate::security::principal::Principal;
use tonic::{Request, Status};

pub(crate) fn authenticate_request(
    mut request: Request<()>,
    auth: &AuthConfig,
) -> Result<Request<()>, Status> {
    let context = if auth.is_auth_disabled() {
        SecurityContext::anonymous()
    } else if let Some(internal_broker) = metadata_internal_broker(request.metadata())
        .map_err(SecurityError::into_status)?
        .filter(|_| auth.map_internal_broker_identity())
    {
        SecurityContext::authenticated(
            Principal::BrokerInternal {
                name: internal_broker.to_string(),
            },
            AuthenticationMethod::MutualTls,
        )
    } else if let Some(token) = request.metadata().get("authorization") {
        let token = token
            .to_str()
            .map_err(|_| SecurityError::InvalidMetadata.into_status())?;
        let jwt_config = auth
            .jwt_config()
            .ok_or_else(|| SecurityError::JwtNotConfigured.into_status())?;
        let bearer = broker_parse_bearer_token(token).map_err(SecurityError::into_status)?;
        let claims = broker_validate_token(bearer, &jwt_config.secret_key)
            .map_err(|_| SecurityError::InvalidToken.into_status())?;
        let principal = principal_from_claims(&claims);
        SecurityContext::authenticated(principal, AuthenticationMethod::Jwt)
    } else {
        return Err(SecurityError::MissingCredentials.into_status());
    };

    request.extensions_mut().insert(context);
    Ok(request)
}
