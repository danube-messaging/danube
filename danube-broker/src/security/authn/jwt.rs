use danube_core::jwt::Claims;
use crate::security::error::SecurityError;
use crate::security::principal::Principal;

pub(crate) fn broker_validate_token(
    token: &str,
    jwt_secret: &str,
) -> Result<Claims, jsonwebtoken::errors::Error> {
    danube_core::jwt::validate_token(token, jwt_secret)
}

pub(crate) fn broker_parse_bearer_token(header_value: &str) -> Result<&str, SecurityError> {
    danube_core::jwt::parse_bearer_token(header_value).ok_or(SecurityError::InvalidToken)
}

pub(crate) fn principal_from_claims(claims: &Claims) -> Principal {
    match claims.principal_type.as_str() {
        "user" => Principal::User {
            name: claims.principal_name.clone(),
        },
        "broker_internal" => Principal::BrokerInternal {
            name: claims.principal_name.clone(),
        },
        "service_account" => Principal::ServiceAccount {
            name: claims.principal_name.clone(),
        },
        _ => Principal::ServiceAccount {
            name: claims.principal_name.clone(),
        },
    }
}
