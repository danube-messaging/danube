//! Authentication: principal model, security context, request interceptor, and JWT helpers.
//!
//! This module handles:
//! - **Principal identity** — `Principal` enum (User, ServiceAccount, BrokerInternal, Anonymous)
//! - **Security context** — `SecurityContext` stored in request extensions after authentication
//! - **Request interceptor** — `authenticate_request()` that runs on every gRPC call
//! - **JWT validation cache** — avoids re-computing HMAC on hot paths (send_message, ack, nack)
//! - **JWT helpers** — principal extraction from JWT claims
//! - **Internal broker header** — mTLS-based broker-to-broker identity

use crate::security::config::AuthConfig;
use danube_core::jwt::Claims;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

// ── Constants ──────────────────────────────────────────────────────────────

const INTERNAL_BROKER_HEADER: &str = "x-danube-internal-broker";

/// How long a validated JWT result is cached before re-validation.
/// This avoids HMAC-SHA256 computation on every gRPC call while ensuring
/// that expired/revoked tokens are caught within a bounded window.
const JWT_CACHE_TTL: Duration = Duration::from_secs(30);

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

// ── JWT Validation Cache ───────────────────────────────────────────────────

/// Caches validated JWT tokens to avoid HMAC-SHA256 re-computation on every
/// gRPC call. This is critical for hot paths like `send_message`, `ack`, and
/// `nack` which are called per-message.
///
/// The cache uses the raw Bearer token string as the key and stores the
/// validated `SecurityContext` with a timestamp. Entries expire after
/// `JWT_CACHE_TTL` (30s), after which the token is re-validated.
///
/// Thread-safe via `DashMap` (lock-free concurrent HashMap).
#[derive(Debug, Clone)]
pub(crate) struct JwtValidationCache {
    cache: Arc<DashMap<String, (SecurityContext, Instant)>>,
}

impl JwtValidationCache {
    pub(crate) fn new() -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
        }
    }

    /// Look up a cached validation result. Returns `None` if the token
    /// is not cached or has expired.
    fn get(&self, token: &str) -> Option<SecurityContext> {
        if let Some(entry) = self.cache.get(token) {
            let (ctx, created_at) = entry.value();
            if created_at.elapsed() < JWT_CACHE_TTL {
                return Some(ctx.clone());
            }
            // Expired — drop the ref before removing
            drop(entry);
            self.cache.remove(token);
        }
        None
    }

    /// Store a validated result in the cache.
    fn insert(&self, token: String, context: SecurityContext) {
        self.cache.insert(token, (context, Instant::now()));
    }
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
    jwt_cache: &JwtValidationCache,
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
        let token_str = token
            .to_str()
            .map_err(|_| SecurityError::InvalidMetadata.into_status())?;

        // Fast path: check the cache first (avoids HMAC on every send_message)
        if let Some(cached_ctx) = jwt_cache.get(token_str) {
            cached_ctx
        } else {
            // Slow path: full JWT validation
            let jwt_config = auth
                .jwt_config()
                .ok_or_else(|| SecurityError::JwtNotConfigured.into_status())?;
            let bearer = danube_core::jwt::parse_bearer_token(token_str)
                .ok_or_else(|| SecurityError::InvalidToken.into_status())?;
            let claims = danube_core::jwt::validate_token(bearer, &jwt_config.secret_key)
                .map_err(|_| SecurityError::InvalidToken.into_status())?;
            let principal = principal_from_claims(&claims);
            let ctx = SecurityContext::authenticated(principal, AuthenticationMethod::Jwt);

            // Cache the result for future requests with the same token
            jwt_cache.insert(token_str.to_string(), ctx.clone());
            ctx
        }
    } else {
        return Err(SecurityError::MissingCredentials.into_status());
    };

    request.extensions_mut().insert(context);
    Ok(request)
}
