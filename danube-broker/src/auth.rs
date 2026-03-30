use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

fn default_api_key_method() -> String {
    "api_key".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct TlsConfig {
    pub(crate) cert_file: String,
    pub(crate) key_file: String,
    pub(crate) ca_file: String,
    pub(crate) verify_client: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct JwtConfig {
    pub(crate) secret_key: String,
    pub(crate) issuer: String,
    pub(crate) expiration_time: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ServiceAccountCredential {
    pub(crate) name: String,
    pub(crate) api_key: String,
    #[serde(default)]
    pub(crate) disabled: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ServiceAccountAuthConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) require_tls: bool,
    #[serde(default = "default_api_key_method")]
    pub(crate) default_method: String,
    #[serde(default)]
    pub(crate) credentials: Vec<ServiceAccountCredential>,
}

impl Default for ServiceAccountAuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            require_tls: false,
            default_method: default_api_key_method(),
            credentials: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ClientJwtConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) optional: bool,
    pub(crate) secret_key: String,
    pub(crate) issuer: String,
    pub(crate) expiration_time: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub(crate) struct ClientAuthConfig {
    #[serde(default)]
    pub(crate) service_accounts: Option<ServiceAccountAuthConfig>,
    #[serde(default)]
    pub(crate) jwt: Option<ClientJwtConfig>,
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
    TlsWithJwt,
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

    pub(crate) fn jwt_config(&self) -> Option<JwtConfig> {
        self.clients
            .as_ref()
            .and_then(|clients| clients.jwt.as_ref())
            .filter(|jwt| jwt.enabled)
            .map(|jwt| JwtConfig {
                secret_key: jwt.secret_key.clone(),
                issuer: jwt.issuer.clone(),
                expiration_time: jwt.expiration_time,
            })
            .or_else(|| self.jwt.clone())
    }

    pub(crate) fn map_internal_broker_identity(&self) -> bool {
        self.internal
            .as_ref()
            .map(|internal| internal.map_peer_identity_to_broker_internal)
            .unwrap_or(false)
    }

    pub(crate) fn service_account_for_api_key(
        &self,
        api_key: &str,
    ) -> Option<&ServiceAccountCredential> {
        self.clients
            .as_ref()
            .and_then(|clients| clients.service_accounts.as_ref())
            .filter(|service_accounts| service_accounts.enabled)
            .and_then(|service_accounts| {
                service_accounts
                    .credentials
                    .iter()
                    .find(|credential| !credential.disabled && credential.api_key == api_key)
            })
    }
}
