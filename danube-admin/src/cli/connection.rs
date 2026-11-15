//! Shared client connection flags, saved connection profiles ("contexts"),
//! and endpoint resolution for every client-side `danube` command.
//!
//! Resolution order for each field:
//!   1. explicit flag (`--endpoint`, `--token`, ...)
//!   2. environment variable (`DANUBE_ENDPOINT`, `DANUBE_TOKEN`, ...) - handled by clap
//!   3. selected context (`--context` / `DANUBE_CONTEXT` / current context in contexts.toml)
//!   4. built-in default (`http://127.0.0.1:6650`, no TLS, no token)

use anyhow::{anyhow, Context as _, Result};
use clap::{Args, ValueHint};
use danube_client::DanubeClient;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::core::GrpcClientConfig;

/// Default broker endpoint used when nothing else is configured.
pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:6650";

/// Connection flags shared by every client command (`topic`, `produce`, `cluster`, ...).
///
/// All flags are `global`, so they can be placed anywhere after the command group:
/// `danube topic list default --endpoint http://broker:6650`.
#[derive(Debug, Args, Clone, Default)]
pub struct ClientArgs {
    /// Broker endpoint [default: http://127.0.0.1:6650]
    #[arg(
        long,
        global = true,
        env = "DANUBE_ENDPOINT",
        value_name = "URL",
        value_hint = ValueHint::Url,
        help_heading = "Connection"
    )]
    pub endpoint: Option<String>,

    /// Saved connection profile to use (see `danube context`)
    #[arg(
        long,
        global = true,
        env = "DANUBE_CONTEXT",
        value_name = "NAME",
        help_heading = "Connection"
    )]
    pub context: Option<String>,

    /// JWT bearer token
    #[arg(
        long,
        global = true,
        env = "DANUBE_TOKEN",
        hide_env_values = true,
        value_name = "JWT",
        help_heading = "Connection"
    )]
    pub token: Option<String>,

    /// Connect with TLS (implied by https:// endpoints)
    #[arg(long, global = true, env = "DANUBE_TLS", help_heading = "Connection")]
    pub tls: bool,

    /// CA certificate (PEM) used to verify the broker
    #[arg(
        long,
        global = true,
        env = "DANUBE_TLS_CA",
        value_name = "FILE",
        value_hint = ValueHint::FilePath,
        help_heading = "Connection"
    )]
    pub tls_ca: Option<String>,

    /// Client certificate (PEM) for mTLS
    #[arg(
        long,
        global = true,
        env = "DANUBE_TLS_CERT",
        value_name = "FILE",
        value_hint = ValueHint::FilePath,
        help_heading = "Connection"
    )]
    pub tls_cert: Option<String>,

    /// Client private key (PEM) for mTLS
    #[arg(
        long,
        global = true,
        env = "DANUBE_TLS_KEY",
        value_name = "FILE",
        value_hint = ValueHint::FilePath,
        help_heading = "Connection"
    )]
    pub tls_key: Option<String>,

    /// TLS server name to verify [default: localhost]
    #[arg(
        long,
        global = true,
        env = "DANUBE_TLS_DOMAIN",
        value_name = "NAME",
        help_heading = "Connection"
    )]
    pub tls_domain: Option<String>,
}

/// Fully resolved connection settings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Connection {
    pub endpoint: String,
    pub token: Option<String>,
    pub tls: bool,
    pub tls_ca: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub tls_domain: Option<String>,
    /// Name of the context the settings were (partially) taken from, if any.
    pub context: Option<String>,
}

impl ClientArgs {
    /// Resolve flags + env + saved context + defaults into a [`Connection`].
    pub fn resolve(&self) -> Result<Connection> {
        let store = ContextStore::load()?;
        self.resolve_with(&store)
    }

    pub(crate) fn resolve_with(&self, store: &ContextStore) -> Result<Connection> {
        let (context_name, profile) = match &self.context {
            Some(name) => {
                let profile = store.contexts.get(name).ok_or_else(|| {
                    anyhow!(
                        "context '{}' not found (available: {}). Create it with `danube context add {} --endpoint <URL>`",
                        name,
                        store.names_or_none(),
                        name
                    )
                })?;
                (Some(name.clone()), Some(profile.clone()))
            }
            None => match &store.current {
                Some(name) => (
                    store.contexts.get(name).map(|_| name.clone()),
                    store.contexts.get(name).cloned(),
                ),
                None => (None, None),
            },
        };
        let profile = profile.unwrap_or_default();

        let endpoint = self
            .endpoint
            .clone()
            .or_else(|| (!profile.endpoint.is_empty()).then(|| profile.endpoint.clone()))
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());

        Ok(Connection {
            endpoint,
            token: self.token.clone().or(profile.token),
            tls: self.tls || profile.tls,
            tls_ca: self.tls_ca.clone().or(profile.tls_ca),
            tls_cert: self.tls_cert.clone().or(profile.tls_cert),
            tls_key: self.tls_key.clone().or(profile.tls_key),
            tls_domain: self.tls_domain.clone().or(profile.tls_domain),
            context: context_name,
        })
    }
}

impl Connection {
    /// Whether TLS should be used for this connection.
    pub fn wants_tls(&self) -> bool {
        self.tls
            || self.endpoint.starts_with("https://")
            || self.tls_ca.is_some()
            || (self.tls_cert.is_some() && self.tls_key.is_some())
    }

    /// Admin gRPC client configuration for this connection.
    pub fn grpc_config(&self) -> GrpcClientConfig {
        GrpcClientConfig {
            endpoint: self.endpoint.clone(),
            request_timeout_ms: 5000,
            enable_tls: self.wants_tls().then_some(true),
            domain: self.tls_domain.clone(),
            ca_path: self.tls_ca.clone(),
            cert_path: self.tls_cert.clone(),
            key_path: self.tls_key.clone(),
            token: self.token.clone(),
        }
    }

    /// Build a data-plane `DanubeClient` (produce / consume) for this connection.
    pub async fn danube_client(&self) -> Result<DanubeClient> {
        let mut service_url = self.endpoint.clone();
        if self.wants_tls() && service_url.starts_with("http://") {
            service_url = service_url.replacen("http://", "https://", 1);
        }
        let mut builder = DanubeClient::builder().service_url(&service_url);

        if self.wants_tls() {
            match (&self.tls_ca, &self.tls_cert, &self.tls_key) {
                (Some(ca), Some(cert), Some(key)) => builder = builder.with_mtls(ca, cert, key)?,
                (Some(ca), _, _) => builder = builder.with_tls(ca)?,
                _ => anyhow::bail!(
                    "TLS requested but no CA certificate was provided (use --tls-ca <FILE> or a context with tls_ca)"
                ),
            }
        }

        if let Some(token) = &self.token {
            builder = builder.with_token(token.clone());
        }

        Ok(builder.build().await?)
    }
}

// ─── Saved contexts ─────────────────────────────────────────────────────────

/// One saved connection profile.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Profile {
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub tls: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_cert: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_domain: Option<String>,
}

/// On-disk `contexts.toml` (client-side only; never read by the server).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextStore {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<String>,
    #[serde(default)]
    pub contexts: BTreeMap<String, Profile>,
}

impl ContextStore {
    /// Directory holding client-side configuration:
    /// `$DANUBE_CONFIG_DIR`, else `$XDG_CONFIG_HOME/danube`, else `~/.config/danube`.
    pub fn config_dir() -> Option<PathBuf> {
        if let Some(dir) = std::env::var_os("DANUBE_CONFIG_DIR").filter(|v| !v.is_empty()) {
            return Some(PathBuf::from(dir));
        }
        if let Some(dir) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
            return Some(PathBuf::from(dir).join("danube"));
        }
        std::env::var_os("HOME")
            .filter(|v| !v.is_empty())
            .map(|home| PathBuf::from(home).join(".config").join("danube"))
    }

    pub fn path() -> Option<PathBuf> {
        Self::config_dir().map(|d| d.join("contexts.toml"))
    }

    /// Load the store; a missing file yields an empty store.
    pub fn load() -> Result<Self> {
        let Some(path) = Self::path() else {
            return Ok(Self::default());
        };
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))
    }

    /// Persist the store (file mode 0600 on unix since it may contain tokens).
    pub fn save(&self) -> Result<PathBuf> {
        let path = Self::path()
            .ok_or_else(|| anyhow!("cannot determine config directory; set DANUBE_CONFIG_DIR"))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let raw = toml::to_string_pretty(self)?;
        std::fs::write(&path, raw).with_context(|| format!("failed to write {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(path)
    }

    pub(crate) fn names_or_none(&self) -> String {
        if self.contexts.is_empty() {
            "none".to_string()
        } else {
            self.contexts.keys().cloned().collect::<Vec<_>>().join(", ")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_with(name: &str, endpoint: &str, current: bool) -> ContextStore {
        let mut store = ContextStore::default();
        store.contexts.insert(
            name.to_string(),
            Profile {
                endpoint: endpoint.to_string(),
                token: Some("ctx-token".to_string()),
                ..Default::default()
            },
        );
        if current {
            store.current = Some(name.to_string());
        }
        store
    }

    #[test]
    fn defaults_without_context() {
        let conn = ClientArgs::default()
            .resolve_with(&ContextStore::default())
            .unwrap();
        assert_eq!(conn.endpoint, DEFAULT_ENDPOINT);
        assert!(conn.token.is_none());
        assert!(conn.context.is_none());
    }

    #[test]
    fn current_context_is_used() {
        let store = store_with("prod", "https://prod:6650", true);
        let conn = ClientArgs::default().resolve_with(&store).unwrap();
        assert_eq!(conn.endpoint, "https://prod:6650");
        assert_eq!(conn.token.as_deref(), Some("ctx-token"));
        assert_eq!(conn.context.as_deref(), Some("prod"));
        assert!(conn.wants_tls());
    }

    #[test]
    fn flags_override_context() {
        let store = store_with("prod", "https://prod:6650", true);
        let args = ClientArgs {
            endpoint: Some("http://local:6650".into()),
            token: Some("flag-token".into()),
            ..Default::default()
        };
        let conn = args.resolve_with(&store).unwrap();
        assert_eq!(conn.endpoint, "http://local:6650");
        assert_eq!(conn.token.as_deref(), Some("flag-token"));
    }

    #[test]
    fn unknown_context_is_an_error() {
        let args = ClientArgs {
            context: Some("missing".into()),
            ..Default::default()
        };
        let err = args.resolve_with(&ContextStore::default()).unwrap_err();
        assert!(err.to_string().contains("context 'missing' not found"));
    }

    #[test]
    fn store_roundtrips_through_toml() {
        let store = store_with("dev", "http://127.0.0.1:6650", true);
        let raw = toml::to_string_pretty(&store).unwrap();
        let back: ContextStore = toml::from_str(&raw).unwrap();
        assert_eq!(store, back);
    }
}

