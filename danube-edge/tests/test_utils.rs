//! Shared test utilities for danube-edge integration tests.
//!
//! These tests run against a live cluster + edge broker started by the
//! `edge-replication-e2e.yml` GitHub Actions workflow.
//!
//! Environment variables (set by the workflow):
//!   EDGE_BROKER_ENDPOINT   — e.g. "http://127.0.0.1:6653" (edge broker)
//!   CLOUD_BROKER_ENDPOINT  — e.g. "http://127.0.0.1:6650" (cluster entry point)

extern crate danube_client;

use anyhow::Result;
use danube_client::DanubeClient;
use std::time::{SystemTime, UNIX_EPOCH};

/// Connect to the edge broker (local producers write here).
#[allow(dead_code)]
pub async fn edge_client() -> Result<DanubeClient> {
    let endpoint =
        std::env::var("EDGE_BROKER_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6653".into());

    let client = DanubeClient::builder()
        .service_url(&endpoint)
        .build()
        .await?;

    Ok(client)
}

/// Connect to a cloud cluster broker (consumers read from here).
#[allow(dead_code)]
pub async fn cloud_client() -> Result<DanubeClient> {
    let endpoint =
        std::env::var("CLOUD_BROKER_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6650".into());

    let client = DanubeClient::builder()
        .service_url(&endpoint)
        .build()
        .await?;

    Ok(client)
}

/// Generate a unique topic name under a namespace.
#[allow(dead_code)]
pub fn unique_topic(namespace: &str, prefix: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("/{}/{}-{}", namespace, prefix, now)
}
