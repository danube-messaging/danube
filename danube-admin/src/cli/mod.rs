pub mod apply;
pub mod brokers;
pub mod cluster;
pub mod consume;
pub mod namespaces;
pub mod produce;
pub mod schema_helpers;
pub mod schemas;
pub mod security;
pub mod topics;

use anyhow::Result;
use clap::Args;
use danube_client::DanubeClient;

/// Shared TLS / JWT connection flags for data-plane CLI commands (`produce`, `consume`).
#[derive(Debug, Args, Clone)]
pub struct DataAuthArgs {
    /// Enable TLS when connecting to the broker data port
    #[arg(long, env = "DANUBE_TLS", default_value_t = false)]
    pub tls: bool,

    /// Path to CA certificate PEM file for TLS verification
    #[arg(long, env = "DANUBE_CA")]
    pub ca: Option<String>,

    /// Path to client certificate PEM file for mTLS
    #[arg(long, env = "DANUBE_CERT")]
    pub cert: Option<String>,

    /// Path to client private key PEM file for mTLS
    #[arg(long, env = "DANUBE_KEY")]
    pub tls_key: Option<String>,

    /// JWT bearer token for authentication
    #[arg(long, env = "DANUBE_TOKEN")]
    pub token: Option<String>,
}

/// Build a `DanubeClient` with optional TLS/mTLS and JWT authentication.
pub(crate) async fn build_danube_client(
    service_addr: &str,
    auth: &DataAuthArgs,
) -> Result<DanubeClient> {
    let mut builder = DanubeClient::builder().service_url(service_addr);

    let wants_tls = auth.tls
        || service_addr.starts_with("https://")
        || auth.ca.is_some()
        || (auth.cert.is_some() && auth.tls_key.is_some());

    if wants_tls {
        if let (Some(ca), Some(cert), Some(key)) = (&auth.ca, &auth.cert, &auth.tls_key) {
            builder = builder.with_mtls(ca, cert, key)?;
        } else if let Some(ca) = &auth.ca {
            builder = builder.with_tls(ca)?;
        } else {
            anyhow::bail!("TLS requested but --ca certificate path was not provided");
        }
    }

    if let Some(ref token) = auth.token {
        builder = builder.with_token(token.clone());
    }

    Ok(builder.build().await?)
}
