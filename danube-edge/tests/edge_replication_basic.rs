//! Edge Replication — Basic Topic Creation & Message Flow
//!
//! Tests that the edge broker can create topics in its allowed namespace,
//! produce messages locally, and those messages appear on the cloud cluster
//! for consumers.
//!
//! Requires: edge-replication-e2e workflow (3 cluster brokers + 1 edge broker).

extern crate danube_client;

use anyhow::Result;
use danube_client::SubType;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// Test: Produce on edge → consume on cloud (single topic).
///
/// Validates the full edge replication pipeline:
/// 1. Producer creates topic on edge broker (local WAL)
/// 2. EdgeReplicator replicates batches to cloud cluster
/// 3. Cloud consumer receives all messages
///
/// This is the most fundamental edge test — if this fails, nothing works.
#[tokio::test]
#[ignore = "requires edge replication e2e workflow"]
async fn edge_produce_cloud_consume_single_topic() -> Result<()> {
    let edge = test_utils::edge_client().await?;
    let cloud = test_utils::cloud_client().await?;

    let topic = test_utils::unique_topic("edge1", "basic");
    let message_count = 20;

    // Producer on the edge broker
    let mut producer = edge
        .new_producer()
        .with_topic(&topic)
        .with_name("edge-basic-producer")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    // Consumer on the cloud cluster
    let mut consumer = cloud
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("cloud-basic-consumer")
        .with_subscription("cloud-basic-sub")
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    // Give the edge replicator time to create the topic on cloud and start tailing
    sleep(Duration::from_secs(2)).await;

    // Produce messages on edge
    for i in 0..message_count {
        let payload = format!("edge-msg-{}", i);
        let _ = producer.send(payload.as_bytes().to_vec(), None).await?;
    }

    // Consume from cloud — all messages should arrive
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

/// Test: Produce on edge → consume on cloud (3 topics simultaneously).
///
/// Validates that the edge replicator handles multiple topics correctly:
/// - Each topic may route to a different cluster broker
/// - All topics replicate concurrently
/// - No cross-topic message leaks
#[tokio::test]
#[ignore = "requires edge replication e2e workflow"]
async fn edge_multi_topic_replication() -> Result<()> {
    let edge = test_utils::edge_client().await?;
    let cloud = test_utils::cloud_client().await?;

    let topics: Vec<String> = (1..=3)
        .map(|i| test_utils::unique_topic("edge1", &format!("multi-{}", i)))
        .collect();
    let msgs_per_topic = 10;

    // Create producers on edge (one per topic)
    let mut producers = Vec::new();
    for (i, topic) in topics.iter().enumerate() {
        let mut p = edge
            .new_producer()
            .with_topic(topic)
            .with_name(&format!("edge-multi-producer-{}", i))
            .with_reliable_dispatch()
            .build()?;
        p.create().await?;
        producers.push(p);
    }

    // Create consumers on cloud (one per topic)
    let mut consumers = Vec::new();
    let mut streams = Vec::new();
    for (i, topic) in topics.iter().enumerate() {
        let mut c = cloud
            .new_consumer()
            .with_topic(topic.clone())
            .with_consumer_name(format!("cloud-multi-consumer-{}", i))
            .with_subscription(format!("cloud-multi-sub-{}", i))
            .with_subscription_type(SubType::Exclusive)
            .build()?;
        c.subscribe().await?;
        let s = c.receive().await?;
        streams.push(s);
        consumers.push(c);
    }

    sleep(Duration::from_secs(2)).await;

    // Produce messages on edge (interleaved across topics)
    for msg_idx in 0..msgs_per_topic {
        for (topic_idx, producer) in producers.iter_mut().enumerate() {
            let payload = format!("topic-{}-msg-{}", topic_idx, msg_idx);
            let _ = producer.send(payload.as_bytes().to_vec(), None).await?;
        }
    }

    // Consume from cloud — each topic should get exactly msgs_per_topic messages
    let receive_future = async {
        for (topic_idx, (stream, consumer)) in
            streams.iter_mut().zip(consumers.iter_mut()).enumerate()
        {
            let mut received = 0usize;
            while received < msgs_per_topic {
                if let Some(msg) = stream.recv().await {
                    let payload = String::from_utf8_lossy(&msg.payload);
                    let expected_prefix = format!("topic-{}-msg-", topic_idx);
                    assert!(
                        payload.starts_with(&expected_prefix),
                        "topic {} got unexpected payload: {}",
                        topic_idx,
                        payload
                    );
                    let _ = consumer.ack(&msg).await;
                    received += 1;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    };

    timeout(Duration::from_secs(60), receive_future)
        .await
        .map_err(|_| {
            anyhow::anyhow!("timed out waiting for multi-topic messages from cloud")
        })??;

    Ok(())
}

/// Test: Payload integrity across edge → cloud replication.
///
/// Sends a large binary blob (100KB) from edge and verifies byte-exact
/// match on the cloud consumer side.
#[tokio::test]
#[ignore = "requires edge replication e2e workflow"]
async fn edge_payload_integrity() -> Result<()> {
    let edge = test_utils::edge_client().await?;
    let cloud = test_utils::cloud_client().await?;

    let topic = test_utils::unique_topic("edge1", "payload");

    let mut producer = edge
        .new_producer()
        .with_topic(&topic)
        .with_name("edge-payload-producer")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    let mut consumer = cloud
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("cloud-payload-consumer")
        .with_subscription("cloud-payload-sub")
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_secs(2)).await;

    // Send a 100KB binary payload
    let blob: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
    let _ = producer.send(blob.clone(), None).await?;

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
