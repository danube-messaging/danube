use crate::security::authn::claims::Claims;
use crate::security::error::SecurityError;
use crate::security::principal::Principal;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};

pub(crate) fn create_token(
    claims: &Claims,
    jwt_secret: &str,
) -> Result<String, jsonwebtoken::errors::Error> {
    encode(
        &Header::default(),
        claims,
        &EncodingKey::from_secret(jwt_secret.as_ref()),
    )
}

pub(crate) fn validate_token(
    token: &str,
    jwt_secret: &str,
) -> Result<Claims, jsonwebtoken::errors::Error> {
    let validation = Validation::default();
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(jwt_secret.as_ref()),
        &validation,
    )?;
    Ok(token_data.claims)
}

pub(crate) fn parse_bearer_token(header_value: &str) -> Result<&str, SecurityError> {
    header_value
        .strip_prefix("Bearer ")
        .or_else(|| header_value.strip_prefix("bearer "))
        .ok_or(SecurityError::InvalidToken)
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
