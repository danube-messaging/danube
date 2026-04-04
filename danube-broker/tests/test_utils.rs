//! Shared test utilities for danube-broker integration tests
//!
//! By default, connects to a broker running with `auth.mode: none` (no TLS).
//! Override with the `DANUBE_BROKER_ENDPOINT` env var if needed.

extern crate danube_client;

use anyhow::Result;
use danube_client::DanubeClient;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn setup_client() -> Result<DanubeClient> {
    let endpoint =
        std::env::var("DANUBE_BROKER_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6650".into());

    let client = DanubeClient::builder()
        .service_url(&endpoint)
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
