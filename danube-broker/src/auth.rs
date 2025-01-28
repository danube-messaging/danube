use serde::{Deserialize, Serialize};

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
    pub(crate) jwt: Option<JwtConfig>,
}
