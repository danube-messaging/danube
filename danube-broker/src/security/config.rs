use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct TlsConfig {
    pub(crate) cert_file: String,
    pub(crate) key_file: String,
    pub(crate) ca_file: String,
    pub(crate) verify_client: bool,
}

/// JWT configuration — used for validating client tokens and (optionally)
/// issuing broker-minted short-lived tokens via the AuthService RPC.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct JwtConfig {
    pub(crate) secret_key: String,
    pub(crate) issuer: String,
    pub(crate) expiration_time: u64,
}

/// Service account auth settings (TLS requirements).
/// Credentials are no longer stored in config — use `danube-admin security tokens create`.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ServiceAccountAuthConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) require_tls: bool,
}

impl Default for ServiceAccountAuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            require_tls: false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub(crate) struct ClientAuthConfig {
    #[serde(default)]
    pub(crate) service_accounts: Option<ServiceAccountAuthConfig>,
    #[serde(default)]
    pub(crate) jwt: Option<JwtConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub(crate) struct InternalAuthConfig {
    #[serde(default)]
    pub(crate) mtls_required: bool,
    #[serde(default)]
    pub(crate) map_peer_identity_to_broker_internal: bool,
    #[serde(default)]
    pub(crate) secure_replicator: bool,
    #[serde(default)]
    pub(crate) secure_raft_transport: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AuthMode {
    None,
    Tls,
}

impl Default for AuthMode {
    fn default() -> Self {
        AuthMode::Tls
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct AuthConfig {
    #[serde(default)]
    pub(crate) mode: AuthMode,
    pub(crate) tls: Option<TlsConfig>,
    /// Top-level JWT config (preferred). Falls back to clients.jwt for backward compat.
    #[serde(default)]
    pub(crate) jwt: Option<JwtConfig>,
    #[serde(default)]
    pub(crate) clients: Option<ClientAuthConfig>,
    #[serde(default)]
    pub(crate) internal: Option<InternalAuthConfig>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::Tls,
            tls: None,
            jwt: None,
            clients: None,
            internal: None,
        }
    }
}

impl AuthConfig {
    pub(crate) fn is_auth_disabled(&self) -> bool {
        self.mode == AuthMode::None
    }

    /// Get the JWT config, checking top-level `jwt` first, then `clients.jwt` for backward compat.
    pub(crate) fn jwt_config(&self) -> Option<JwtConfig> {
        self.jwt
            .clone()
            .or_else(|| {
                self.clients
                    .as_ref()
                    .and_then(|clients| clients.jwt.as_ref())
                    .cloned()
            })
    }

    pub(crate) fn map_internal_broker_identity(&self) -> bool {
        self.internal
            .as_ref()
            .map(|internal| internal.map_peer_identity_to_broker_internal)
            .unwrap_or(false)
    }
}
