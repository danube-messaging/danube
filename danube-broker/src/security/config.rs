use serde::{Deserialize, Serialize};

/// TLS certificate material — shared by all secure connections:
/// - Client-facing gRPC (port 6650): server TLS
/// - Admin gRPC: server TLS
/// - Replicator (inter-broker DLQ): mutual TLS
/// - Raft consensus (port 7650): mutual TLS
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct TlsConfig {
    pub(crate) cert_file: String,
    pub(crate) key_file: String,
    pub(crate) ca_file: String,
}

/// JWT configuration — used for validating client tokens.
/// Generate tokens with: `danube-admin security tokens create --subject my-app --secret-key <key>`
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct JwtConfig {
    pub(crate) secret_key: String,
    pub(crate) issuer: String,
    pub(crate) expiration_time: u64,
}

/// Master security mode switch.
///
/// - `None` — no authentication, no encryption (development only)
/// - `Tls`  — full security: TLS + JWT for clients, mTLS for inter-broker
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AuthMode {
    None,
    Tls,
}

impl Default for AuthMode {
    fn default() -> Self {
        AuthMode::None
    }
}

/// Top-level security configuration.
///
/// When `mode: tls`:
/// - Client ports use TLS (server cert) + JWT (Bearer token)
/// - Replicator uses mTLS (same certs) + internal broker header
/// - Raft transport uses mTLS (same certs, both sides verify)
///
/// When `mode: none`:
/// - All connections are plain HTTP, no authentication
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct AuthConfig {
    #[serde(default)]
    pub(crate) mode: AuthMode,
    pub(crate) tls: Option<TlsConfig>,
    #[serde(default)]
    pub(crate) jwt: Option<JwtConfig>,
    /// JWT subjects that bypass RBAC entirely (super-admin access).
    /// Used for initial cluster bootstrap and break-glass scenarios.
    /// Create tokens: `danube-admin security tokens create --subject <name> --secret-key <key>`
    #[serde(default)]
    pub(crate) super_admins: Vec<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::None,
            tls: None,
            jwt: None,
            super_admins: Vec::new(),
        }
    }
}

impl AuthConfig {
    /// Returns true when security is completely disabled (mode: none).
    pub(crate) fn is_auth_disabled(&self) -> bool {
        self.mode == AuthMode::None
    }

    /// Returns true when full security is enabled (mode: tls).
    pub(crate) fn is_tls_enabled(&self) -> bool {
        self.mode == AuthMode::Tls && self.tls.is_some()
    }

    /// Get the JWT config.
    pub(crate) fn jwt_config(&self) -> Option<JwtConfig> {
        self.jwt.clone()
    }

    /// Whether to map the `x-danube-internal-broker` header to `Principal::BrokerInternal`.
    /// Always true when security is enabled — inter-broker communication is trusted.
    pub(crate) fn map_internal_broker_identity(&self) -> bool {
        !self.is_auth_disabled()
    }
}
