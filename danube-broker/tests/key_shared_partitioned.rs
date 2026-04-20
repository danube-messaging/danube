//! Key-Shared subscription tests for partitioned topics and consumer churn
//!
//! Tests verify that:
//! - Key-Shared works with partitioned topics (same key → same partition)
//! - Consumer join/leave during traffic maintains per-key affinity
//! - Partitioned topics + key routing provides correct distribution

extern crate danube_client;

use anyhow::Result;
use danube_client::SubType;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// What this test validates
///
/// - Scenario: partitioned topic (3 partitions) with Key-Shared subscription.
///   Producer uses `send_with_key()` to route messages deterministically to partitions.
///   Single consumer receives from all partitions.
/// - Expectation: all messages received, messages from different keys may come from
///   different partitions.
///
/// Why this matters
/// - Ensures Key-Shared works with partitioned topics and that `send_with_key()` correctly
///   hashes the key to a specific partition.
#[tokio::test]
async fn key_shared_partitioned_single_consumer() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_part_single");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_part")
        .with_partitions(3)
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("cons_ks_part".to_string())
        .with_subscription("ks_part_sub".to_string())
        .with_subscription_type(SubType::KeyShared)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(400)).await;

    let keys = vec!["user-a", "user-b", "user-c", "user-d", "user-e"];
    let msgs_per_key = 3;
    let total = keys.len() * msgs_per_key;

    for round in 0..msgs_per_key {
        for key in &keys {
            let payload = format!("{}-{}", key, round);
            let _ = producer
                .send_with_key(payload.into_bytes(), None, key)
                .await?;
        }
    }

    let recv_future = async {
        let mut received_payloads: Vec<String> = Vec::new();
        let mut partitions_seen: HashSet<String> = HashSet::new();
        while received_payloads.len() < total {
            if let Some(msg) = stream.recv().await {
                let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                received_payloads.push(payload);
                partitions_seen.insert(msg.msg_id.topic_name.clone());
                let _ = consumer.ack(&msg).await;
            }
        }
        (received_payloads, partitions_seen)
    };

    let (received, partitions) = timeout(Duration::from_secs(15), recv_future)
        .await
        .expect("timeout receiving partitioned key-shared messages");

    assert_eq!(received.len(), total, "should receive all messages");

    // With 5 keys across 3 partitions, we expect to see at least 2 partitions used
    assert!(
        partitions.len() >= 2,
        "messages should span multiple partitions, got {:?}",
        partitions
    );

    Ok(())
}

/// What this test validates
///
/// - Scenario: partitioned topic (3 partitions) with 2 Key-Shared consumers.
///   Verifies per-key affinity across partitions.
/// - Expectation: each key is consistently routed to the same consumer.
///
/// Why this matters
/// - The two-level routing (key→partition, then key→consumer within partition) must
///   be deterministic end-to-end.
#[tokio::test]
async fn key_shared_partitioned_multi_consumer_affinity() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_part_multi");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_part_multi")
        .with_partitions(3)
        .build()?;
    producer.create().await?;

    let sub = "ks_part_multi_sub";
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String)>();

    for i in 0..2 {
        let cname = format!("ks-part-cons-{}", i);
        let topic_c = topic.clone();
        let sub_c = sub.to_string();
        let txc = tx.clone();
        let client_c = test_utils::setup_client().await?;

        tokio::spawn(async move {
            let mut c = client_c
                .new_consumer()
                .with_topic(topic_c)
                .with_consumer_name(cname.clone())
                .with_subscription(sub_c)
                .with_subscription_type(SubType::KeyShared)
                .build()
                .unwrap();
            c.subscribe().await.unwrap();
            let mut stream = c.receive().await.unwrap();
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let _ = c.ack(&msg).await;
                        let _ = txc.send((cname.clone(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    sleep(Duration::from_millis(500)).await;

    let keys = vec!["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];
    let msgs_per_key = 3;
    let total = keys.len() * msgs_per_key;

    for round in 0..msgs_per_key {
        for key in &keys {
            let payload = format!("{}-{}", key, round);
            let _ = producer
                .send_with_key(payload.into_bytes(), None, key)
                .await?;
        }
    }

    let recv_future = async {
        let mut key_to_consumer: HashMap<String, HashSet<String>> = HashMap::new();
        let mut received = 0usize;
        while received < total {
            if let Some((cname, payload)) = rx.recv().await {
                // Extract key from payload: "{key}-{round}"
                let key = payload.rsplit_once('-').map(|(k, _)| k).unwrap_or(&payload);
                key_to_consumer
                    .entry(key.to_string())
                    .or_default()
                    .insert(cname);
                received += 1;
            }
        }
        key_to_consumer
    };

    let key_to_consumer = timeout(Duration::from_secs(20), recv_future)
        .await
        .expect("timeout receiving partitioned multi-consumer messages");

    // Each key should be served by exactly one consumer
    for (key, consumers) in &key_to_consumer {
        assert_eq!(
            consumers.len(),
            1,
            "key '{}' should be served by exactly 1 consumer, got {:?}",
            key,
            consumers
        );
    }

    // Distribution: at least 2 consumers should have received messages
    let all_consumers: HashSet<&String> = key_to_consumer
        .values()
        .flat_map(|s| s.iter())
        .collect();
    assert!(
        all_consumers.len() >= 2,
        "should distribute across consumers, got {:?}",
        all_consumers
    );

    Ok(())
}

/// What this test validates
///
/// - Scenario: Key-Shared with consumer churn. Start with 2 consumers,
///   publish messages, then add a 3rd consumer mid-stream and publish more.
/// - Expectation: all messages are delivered. After churn, keys may be reassigned
///   but each message is delivered exactly once to one consumer.
///
/// Why this matters
/// - Validates that the consistent hash ring properly rebalances when consumers join
///   and that no messages are lost or duplicated during the transition.
#[tokio::test]
async fn key_shared_consumer_churn_join() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_churn_join");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_churn")
        .build()?;
    producer.create().await?;

    let sub = "ks_churn_join_sub";
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String)>();

    // Start with 2 consumers
    for i in 0..2 {
        let cname = format!("ks-churn-cons-{}", i);
        let topic_c = topic.clone();
        let sub_c = sub.to_string();
        let txc = tx.clone();
        let client_c = test_utils::setup_client().await?;

        tokio::spawn(async move {
            let mut c = client_c
                .new_consumer()
                .with_topic(topic_c)
                .with_consumer_name(cname.clone())
                .with_subscription(sub_c)
                .with_subscription_type(SubType::KeyShared)
                .build()
                .unwrap();
            c.subscribe().await.unwrap();
            let mut stream = c.receive().await.unwrap();
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let _ = c.ack(&msg).await;
                        let _ = txc.send((cname.clone(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    sleep(Duration::from_millis(400)).await;

    let keys = vec!["k1", "k2", "k3", "k4", "k5", "k6"];

    // First tranche: 6 keys × 2 messages each = 12
    for round in 0..2 {
        for key in &keys {
            let payload = format!("{}-t1-{}", key, round);
            let _ = producer
                .send_with_key(payload.into_bytes(), None, key)
                .await?;
        }
        sleep(Duration::from_millis(10)).await;
    }

    // Add third consumer mid-stream
    {
        let topic_c = topic.clone();
        let sub_c = sub.to_string();
        let txc = tx.clone();
        let client_c = test_utils::setup_client().await?;

        tokio::spawn(async move {
            let mut c = client_c
                .new_consumer()
                .with_topic(topic_c)
                .with_consumer_name("ks-churn-cons-2".to_string())
                .with_subscription(sub_c)
                .with_subscription_type(SubType::KeyShared)
                .build()
                .unwrap();
            c.subscribe().await.unwrap();
            let mut stream = c.receive().await.unwrap();
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let _ = c.ack(&msg).await;
                        let _ = txc.send(("ks-churn-cons-2".to_string(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    sleep(Duration::from_millis(300)).await;

    // Second tranche: 6 keys × 2 messages each = 12
    for round in 0..2 {
        for key in &keys {
            let payload = format!("{}-t2-{}", key, round);
            let _ = producer
                .send_with_key(payload.into_bytes(), None, key)
                .await?;
        }
        sleep(Duration::from_millis(10)).await;
    }

    let total = keys.len() * 4; // 2 tranches × 2 rounds each × 6 keys = 24

    let recv_future = async {
        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut received = 0usize;
        while received < total {
            if let Some((cname, payload)) = rx.recv().await {
                *counts.entry(cname).or_insert(0) += 1;
                seen.insert(payload);
                received += 1;
            }
        }
        (counts, seen)
    };

    let (counts, seen) = timeout(Duration::from_secs(20), recv_future)
        .await
        .expect("timeout receiving churn key-shared messages");

    // All unique messages should be delivered
    assert_eq!(
        seen.len(),
        total,
        "all unique messages should be received, got {}",
        seen.len()
    );

    // At least the initial 2 consumers should have received messages
    let active_consumers: Vec<&String> = counts.keys().filter(|k| counts[*k] > 0).collect();
    assert!(
        active_consumers.len() >= 2,
        "at least 2 consumers should have received messages, got {:?}",
        counts
    );

    Ok(())
}

/// What this test validates
///
/// - Scenario: reliable Key-Shared with partitioned topic (2 partitions).
///   Two consumers, same subscription. Verifies reliable dispatch + partitioning + key routing.
/// - Expectation: all messages delivered and acked, per-key affinity maintained.
///
/// Why this matters
/// - The full reliable path with partitioned topics is the most complex configuration.
///   This test ensures all layers work together: partition selection, reliable WAL,
///   InFlightWindow, consistent hashing, and ack tracking.
#[tokio::test]
async fn key_shared_reliable_partitioned() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_rel_part");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_rel_part")
        .with_partitions(2)
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    let sub = "ks_rel_part_sub";
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String)>();

    for i in 0..2 {
        let cname = format!("ks-rp-cons-{}", i);
        let topic_c = topic.clone();
        let sub_c = sub.to_string();
        let txc = tx.clone();
        let client_c = test_utils::setup_client().await?;

        tokio::spawn(async move {
            let mut c = client_c
                .new_consumer()
                .with_topic(topic_c)
                .with_consumer_name(cname.clone())
                .with_subscription(sub_c)
                .with_subscription_type(SubType::KeyShared)
                .build()
                .unwrap();
            c.subscribe().await.unwrap();
            let mut stream = c.receive().await.unwrap();
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let _ = c.ack(&msg).await;
                        let _ = txc.send((cname.clone(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    sleep(Duration::from_millis(500)).await;

    let keys = vec!["acc-100", "acc-200", "acc-300", "acc-400"];
    let msgs_per_key = 4;
    let total = keys.len() * msgs_per_key;

    for round in 0..msgs_per_key {
        for key in &keys {
            let payload = format!("{}-{}", key, round);
            let _ = producer
                .send_with_key(payload.into_bytes(), None, key)
                .await?;
        }
    }

    let recv_future = async {
        let mut key_to_consumer: HashMap<String, HashSet<String>> = HashMap::new();
        let mut received = 0usize;
        while received < total {
            if let Some((cname, payload)) = rx.recv().await {
                let key = payload.rsplit_once('-').map(|(k, _)| k).unwrap_or(&payload);
                key_to_consumer
                    .entry(key.to_string())
                    .or_default()
                    .insert(cname);
                received += 1;
            }
        }
        key_to_consumer
    };

    let key_to_consumer = timeout(Duration::from_secs(25), recv_future)
        .await
        .expect("timeout receiving reliable partitioned key-shared messages");

    // Per-key affinity
    for (key, consumers) in &key_to_consumer {
        assert_eq!(
            consumers.len(),
            1,
            "key '{}' should be served by 1 consumer, got {:?}",
            key,
            consumers
        );
    }

    Ok(())
}
