//! Shared JWT types and utilities for Danube authentication.
//!
//! This module provides the canonical `Claims` structure and helper functions
//! for creating and validating JWTs. Used by both `danube-broker` (validation)
//! and `danube-admin` (offline token generation).

use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

/// JWT claims used by Danube for authentication.
///
/// The `sub` field is the principal name (e.g. "my-app"), and `principal_type`
/// determines how the broker maps the identity (e.g. "service_account", "user").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// Token issuer (e.g. "danube-auth")
    pub iss: String,
    /// Expiration time (seconds since UNIX epoch)
    pub exp: u64,
    /// Subject — the principal name
    pub sub: String,
    /// Principal type: "service_account", "user", or "broker_internal"
    pub principal_type: String,
    /// Principal name (same as `sub` by convention)
    pub principal_name: String,
}

/// Create a signed JWT from the given claims.
pub fn create_token(
    claims: &Claims,
    jwt_secret: &str,
) -> Result<String, jsonwebtoken::errors::Error> {
    encode(
        &Header::default(),
        claims,
        &EncodingKey::from_secret(jwt_secret.as_ref()),
    )
}

/// Validate a JWT and return the decoded claims.
pub fn validate_token(
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

/// Extract the bearer token from an "Authorization: Bearer <token>" header value.
pub fn parse_bearer_token(header_value: &str) -> Option<&str> {
    header_value
        .strip_prefix("Bearer ")
        .or_else(|| header_value.strip_prefix("bearer "))
}
