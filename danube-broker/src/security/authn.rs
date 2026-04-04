//! Authentication: principal model, security context, request interceptor, and JWT helpers.
//!
//! This module handles:
//! - **Principal identity** — `Principal` enum (User, ServiceAccount, BrokerInternal, Anonymous)
//! - **Security context** — `SecurityContext` stored in request extensions after authentication
//! - **Request interceptor** — `authenticate_request()` that runs on every gRPC call
//! - **JWT helpers** — principal extraction from JWT claims
//! - **Internal broker header** — mTLS-based broker-to-broker identity

use crate::security::config::AuthConfig;
use danube_core::jwt::Claims;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

// ── Constants ──────────────────────────────────────────────────────────────

const INTERNAL_BROKER_HEADER: &str = "x-danube-internal-broker";

// ── Principal ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Principal {
    User { name: String },
    ServiceAccount { name: String },
    BrokerInternal { name: String },
    Anonymous,
}

impl Principal {
    pub(crate) fn principal_type(&self) -> &'static str {
        match self {
            Principal::User { .. } => "user",
            Principal::ServiceAccount { .. } => "service_account",
            Principal::BrokerInternal { .. } => "broker_internal",
            Principal::Anonymous => "anonymous",
        }
    }

    pub(crate) fn principal_name(&self) -> &str {
        match self {
            Principal::User { name } => name,
            Principal::ServiceAccount { name } => name,
            Principal::BrokerInternal { name } => name,
            Principal::Anonymous => "anonymous",
        }
    }
}

// ── Security Context ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AuthenticationMethod {
    None,
    Jwt,
    MutualTls,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecurityContext {
    pub(crate) principal: Principal,
    pub(crate) authentication_method: AuthenticationMethod,
}

impl SecurityContext {
    pub(crate) fn anonymous() -> Self {
        Self {
            principal: Principal::Anonymous,
            authentication_method: AuthenticationMethod::None,
        }
    }

    pub(crate) fn authenticated(
        principal: Principal,
        authentication_method: AuthenticationMethod,
    ) -> Self {
        Self {
            principal,
            authentication_method,
        }
    }
}

pub(crate) fn get_security_context<T>(request: &Request<T>) -> Result<SecurityContext, Status> {
    request
        .extensions()
        .get::<SecurityContext>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Security context missing"))
}

// ── Security Errors ────────────────────────────────────────────────────────

#[derive(Debug)]
enum SecurityError {
    InvalidMetadata,
    InvalidToken,
    JwtNotConfigured,
    MissingCredentials,
}

impl SecurityError {
    fn into_status(self) -> Status {
        match self {
            SecurityError::InvalidMetadata => {
                Status::unauthenticated("Authentication metadata is invalid")
            }
            SecurityError::InvalidToken => Status::unauthenticated("Invalid token"),
            SecurityError::JwtNotConfigured => {
                Status::failed_precondition("JWT support is not enabled")
            }
            SecurityError::MissingCredentials => {
                Status::unauthenticated("Authentication credentials are missing")
            }
        }
    }
}

// ── Internal Broker Header ─────────────────────────────────────────────────

fn metadata_internal_broker(metadata: &MetadataMap) -> Result<Option<&str>, SecurityError> {
    match metadata.get(INTERNAL_BROKER_HEADER) {
        Some(value) => value
            .to_str()
            .map(Some)
            .map_err(|_| SecurityError::InvalidMetadata),
        None => Ok(None),
    }
}

// ── JWT → Principal Mapping ────────────────────────────────────────────────

fn principal_from_claims(claims: &Claims) -> Principal {
    match claims.principal_type.as_str() {
        "user" => Principal::User {
            name: claims.principal_name.clone(),
        },
        "broker_internal" => Principal::BrokerInternal {
            name: claims.principal_name.clone(),
        },
        _ => Principal::ServiceAccount {
            name: claims.principal_name.clone(),
        },
    }
}

// ── Request Interceptor ────────────────────────────────────────────────────

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
        let bearer = danube_core::jwt::parse_bearer_token(token)
            .ok_or_else(|| SecurityError::InvalidToken.into_status())?;
        let claims = danube_core::jwt::validate_token(bearer, &jwt_config.secret_key)
            .map_err(|_| SecurityError::InvalidToken.into_status())?;
        let principal = principal_from_claims(&claims);
        SecurityContext::authenticated(principal, AuthenticationMethod::Jwt)
    } else {
        return Err(SecurityError::MissingCredentials.into_status());
    };

    request.extensions_mut().insert(context);
    Ok(request)
}
