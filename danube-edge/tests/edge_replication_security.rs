//! Edge Replication — RBAC & Namespace Scoping
//!
//! Tests that the edge broker's RBAC permissions are correctly enforced:
//! - Edge broker CAN create topics in its allowed namespace (/edge1)
//! - Edge broker CANNOT create topics in other namespaces (/edge2, /default)
//! - Messages only flow for authorized topics
//!
//! Requires: edge-replication-e2e workflow with security enabled.
//! The workflow provisions:
//!   - Namespace /edge1
//!   - JWT token for subject "edge1"
//!   - RBAC binding: edge1 → edge-replicator-role → scoped to namespace /edge1

extern crate danube_client;

use anyhow::Result;
use danube_client::DanubeClient;
use tokio::time::{sleep, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// Helper: build a cloud client with the edge token for direct RBAC testing.
async fn edge_token_cloud_client() -> Result<DanubeClient> {
    let endpoint =
        std::env::var("CLOUD_BROKER_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6650".into());
    let token = std::env::var("EDGE_TOKEN").unwrap_or_default();

    let mut builder = DanubeClient::builder().service_url(&endpoint);
    if !token.is_empty() {
        builder = builder.with_token(&token);
    }
    let client = builder.build().await?;
    Ok(client)
}

/// Test: Edge can produce to /edge1/allowed_topic (allowed namespace).
///
/// The edge broker's token has Produce + Lookup permissions scoped to
/// namespace /edge1. Creating a producer on /edge1/* should succeed.
#[tokio::test]
#[ignore = "requires edge replication e2e workflow"]
async fn edge_allowed_namespace_produce() -> Result<()> {
    let edge = test_utils::edge_client().await?;

    let topic = test_utils::unique_topic("edge1", "allowed");

    let mut producer = edge
        .new_producer()
        .with_topic(&topic)
        .with_name("edge-allowed-producer")
        .with_reliable_dispatch()
        .build()?;

    // This should succeed — edge1 namespace is allowed
    let result = producer.create().await;
    assert!(
        result.is_ok(),
        "edge should be able to create producer on /edge1/*: {:?}",
        result.err()
    );

    sleep(Duration::from_millis(500)).await;

    let send_result = producer.send("allowed-message".as_bytes().to_vec(), None).await;
    assert!(
        send_result.is_ok(),
        "edge should be able to send to /edge1/*: {:?}",
        send_result.err()
    );

    Ok(())
}

/// Test: Edge CANNOT create topics via EdgeReplicatorService on /edge2/* (unauthorized namespace).
///
/// The edge cloud client authenticates to the cluster with a JWT scoped to /edge1.
/// Attempting to create a topic on /edge2 should be denied by RBAC.
///
/// This tests the cluster-side authorization of CreateEdgeTopic RPC.
#[tokio::test]
#[ignore = "requires edge replication e2e workflow"]
async fn edge_denied_namespace_topic_creation() -> Result<()> {
    // Use the edge token to connect directly to the cloud cluster
    // and attempt to create a topic in a forbidden namespace.
    let client = edge_token_cloud_client().await?;

    let topic = test_utils::unique_topic("edge2", "denied");

    // Try to create a producer on /edge2/* — should be denied
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("edge-denied-producer")
        .with_reliable_dispatch()
        .build()?;

    let result = producer.create().await;

    assert!(
        result.is_err(),
        "edge should NOT be able to create producer on /edge2/*"
    );

    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("PermissionDenied") || err_msg.contains("permission"),
        "error should be permission-related, got: {}",
        err_msg
    );

    Ok(())
}

/// Test: Edge CANNOT produce to /default/* (another namespace entirely).
///
/// Even though /default is a standard namespace, the edge token is scoped
/// only to /edge1. Any other namespace should be denied.
#[tokio::test]
#[ignore = "requires edge replication e2e workflow"]
async fn edge_denied_default_namespace() -> Result<()> {
    let client = edge_token_cloud_client().await?;

    let topic = test_utils::unique_topic("default", "edge-intruder");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("edge-intruder-producer")
        .build()?;

    let result = producer.create().await;

    assert!(
        result.is_err(),
        "edge should NOT be able to create producer on /default/*"
    );

    Ok(())
}
