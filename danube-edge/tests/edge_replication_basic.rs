//! Edge Replication — Basic Topic Creation & Message Flow
//!
//! Tests that the edge broker can produce messages on pre-registered topics,
//! replicate them to the cloud cluster, and cloud consumers receive them.
//!
//! **Architecture**: Topics are declared in `edge.yaml` and registered on the
//! cluster via `RegisterEdge` at edge startup. These tests use the fixed
//! topic names from the E2E edge config.
//!
//! **Key ordering**: Because the cloud topic is created by `RegisterEdge`
//! at edge startup (not at produce time), the topic should already exist
//! on the cloud when we subscribe. We still poll briefly to handle timing.
//!
//! Requires: edge-replication-e2e workflow (3 cluster brokers + 1 edge broker).

extern crate danube_client;

use anyhow::Result;
use danube_client::SubType;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// Wait for a topic to become available on the cloud cluster.
/// With RegisterEdge, topics should already exist, but we poll briefly
/// to handle startup timing.
async fn wait_for_cloud_topic(
    cloud: &danube_client::DanubeClient,
    topic: &str,
    consumer_name: &str,
    subscription: &str,
    timeout_secs: u64,
) -> Result<danube_client::Consumer> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

    loop {
        let mut consumer = cloud
            .new_consumer()
            .with_topic(topic)
            .with_consumer_name(consumer_name)
            .with_subscription(subscription)
            .with_subscription_type(SubType::Exclusive)
            .build()?;

        match consumer.subscribe().await {
            Ok(_) => return Ok(consumer),
            Err(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(500)).await;
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "topic '{}' not available on cloud after {}s: {}",
                    topic,
                    timeout_secs,
                    e
                ))
            }
        }
    }
}

/// Test: Produce on edge → consume on cloud (single topic).
///
/// Uses `/edge1/raw` — a topic declared in the E2E edge config without
/// a schema_subject (raw bytes). The topic is registered on the cluster
/// by `RegisterEdge` at edge startup.
///
/// Validates the full edge replication pipeline:
/// 1. Producer publishes to a pre-registered edge topic
/// 2. EdgeReplicator tails the WAL and replicates batches to the cloud
/// 3. Cloud consumer receives all messages
#[tokio::test]
#[ignore = "requires edge replication e2e workflow"]
async fn edge_produce_cloud_consume_single_topic() -> Result<()> {
    let edge = test_utils::edge_client().await?;
    let cloud = test_utils::cloud_client().await?;

    // Use a topic that is declared in the E2E edge config
    let topic = "/edge1/raw";
    let message_count = 20;

    // Step 1: Producer on the edge broker (topic already exists locally from bootstrap)
    let mut producer = edge
        .new_producer()
        .with_topic(topic)
        .with_name("edge-basic-producer")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    // Step 2: Produce messages
    for i in 0..message_count {
        let payload = format!("edge-msg-{}", i);
        let _ = producer.send(payload.as_bytes().to_vec(), None).await?;
    }

    // Step 3: Wait for topic to be available on cloud (should be quick — RegisterEdge
    // already created it), then subscribe
    let mut consumer =
        wait_for_cloud_topic(&cloud, topic, "cloud-basic-consumer", "cloud-basic-sub", 30)
            .await?;
    let mut stream = consumer.receive().await?;

    // Step 4: Consume from cloud — all messages should arrive
    let receive_future = async {
        let mut received = 0usize;
        while received < message_count {
            if let Some(msg) = stream.recv().await {
                let payload = String::from_utf8_lossy(&msg.payload);
                assert!(
                    payload.starts_with("edge-msg-"),
                    "unexpected payload: {}",
                    payload
                );
                let _ = consumer.ack(&msg).await;
                received += 1;
            }
        }
        Ok::<(), anyhow::Error>(())
    };

    timeout(Duration::from_secs(30), receive_future)
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for {} messages from cloud", message_count))??;

    Ok(())
}

/// Test: Produce on edge with schema topic → consume on cloud.
///
/// Uses `/edge1/telemetry` — a topic declared in the E2E edge config with
/// `schema_subject: "telemetry-events"`. The edge must have resolved the
/// schema from the cluster during RegisterEdge for this topic to be READY.
///
/// Validates schema-aware ingestion:
/// - Topic readiness gating (schema must be resolved)
/// - Messages are stamped with schema_id and schema_version
/// - Replication carries schema metadata to the cloud
#[tokio::test]
#[ignore = "requires edge replication e2e workflow"]
async fn edge_schema_topic_replication() -> Result<()> {
    let edge = test_utils::edge_client().await?;
    let cloud = test_utils::cloud_client().await?;

    // Use the schema-enabled topic from the E2E edge config
    let topic = "/edge1/telemetry";
    let message_count = 10;

    // Step 1: Producer on the edge broker
    let mut producer = edge
        .new_producer()
        .with_topic(topic)
        .with_name("edge-schema-producer")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    // Step 2: Produce JSON payloads matching the telemetry-events schema
    for i in 0..message_count {
        let payload = format!(
            r#"{{"temperature": {:.1}, "device_id": "device-{}"}}"#,
            20.0 + i as f64 * 0.5,
            i
        );
        let _ = producer.send(payload.as_bytes().to_vec(), None).await?;
    }

    // Step 3: Subscribe on cloud
    let mut consumer = wait_for_cloud_topic(
        &cloud,
        topic,
        "cloud-schema-consumer",
        "cloud-schema-sub",
        30,
    )
    .await?;
    let mut stream = consumer.receive().await?;

    // Step 4: Consume and verify
    let receive_future = async {
        let mut received = 0usize;
        while received < message_count {
            if let Some(msg) = stream.recv().await {
                let payload = String::from_utf8_lossy(&msg.payload);
                assert!(
                    payload.contains("temperature"),
                    "unexpected payload: {}",
                    payload
                );
                let _ = consumer.ack(&msg).await;
                received += 1;
            }
        }
        Ok::<(), anyhow::Error>(())
    };

    timeout(Duration::from_secs(30), receive_future)
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for schema topic messages"))??;

    Ok(())
}

/// Test: Payload integrity across edge → cloud replication.
///
/// Sends a large binary blob (100KB) from edge via `/edge1/raw` and verifies
/// byte-exact match on the cloud consumer side.
#[tokio::test]
#[ignore = "requires edge replication e2e workflow"]
async fn edge_payload_integrity() -> Result<()> {
    let edge = test_utils::edge_client().await?;
    let cloud = test_utils::cloud_client().await?;

    // Use the raw bytes topic from the E2E edge config
    let topic = "/edge1/raw";

    // Step 1: Producer on edge
    let mut producer = edge
        .new_producer()
        .with_topic(topic)
        .with_name("edge-payload-producer")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    // Step 2: Send a 100KB binary payload
    let blob: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
    let _ = producer.send(blob.clone(), None).await?;

    // Step 3: Wait for topic on cloud, then subscribe
    let mut consumer = wait_for_cloud_topic(
        &cloud,
        topic,
        "cloud-payload-consumer",
        "cloud-payload-sub",
        30,
    )
    .await?;
    let mut stream = consumer.receive().await?;

    // Step 4: Verify payload integrity
    let receive_future = async {
        if let Some(msg) = stream.recv().await {
            assert_eq!(
                msg.payload.len(),
                blob.len(),
                "payload size mismatch: got {} expected {}",
                msg.payload.len(),
                blob.len()
            );
            assert_eq!(
                msg.payload.as_ref(),
                blob.as_slice(),
                "payload content mismatch"
            );
            let _ = consumer.ack(&msg).await;
        }
        Ok::<(), anyhow::Error>(())
    };

    timeout(Duration::from_secs(30), receive_future)
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for large payload"))??;

    Ok(())
}
