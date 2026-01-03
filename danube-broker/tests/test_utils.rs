//! Shared test utilities for danube-broker integration tests

extern crate danube_client;

use anyhow::Result;
use danube_client::DanubeClient;
use rustls::crypto;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;

static CRYPTO_PROVIDER: OnceCell<()> = OnceCell::const_new();

pub async fn setup_client() -> Result<DanubeClient> {
    CRYPTO_PROVIDER
        .get_or_init(|| async {
            let crypto_provider = crypto::ring::default_provider();
            crypto_provider
                .install_default()
                .expect("Failed to install default CryptoProvider");
        })
        .await;

    let client = DanubeClient::builder()
        .service_url("https://127.0.0.1:6650")
        .with_tls("../cert/ca-cert.pem")?
        .build()
        .await?;

    Ok(client)
}

#[allow(dead_code)]
pub fn unique_topic(prefix: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{}-{}", prefix, now)
}
