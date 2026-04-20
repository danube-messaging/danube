//! Key-Shared subscription basic tests (non-reliable and reliable, single + multi consumer)
//!
//! Tests verify that:
//! - Key-Shared subscription type works end-to-end
//! - Same routing key is always delivered to the same consumer
//! - Different keys can be distributed across consumers
//! - Both non-reliable and reliable dispatch paths work correctly

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
/// - Scenario: one producer sends messages with routing keys to a single KeyShared consumer
///   (non-reliable).
/// - Expectation: the consumer receives all messages.
///
/// Why this matters
/// - Baseline sanity for Key-Shared subscription wiring with a single consumer.
#[tokio::test]
async fn key_shared_basic_single_consumer_non_reliable() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_basic_nr_single");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_basic_nr")
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("cons_ks_basic_nr".to_string())
        .with_subscription("ks_basic_nr_sub".to_string())
        .with_subscription_type(SubType::KeyShared)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(300)).await;

    let keys = vec!["user-1", "user-2", "user-3"];
    let total = 9;
    for i in 0..total {
        let key = keys[i % keys.len()];
        let payload = format!("msg-{}-{}", key, i);
        let _ = producer
            .send_with_key(payload.into_bytes(), None, key)
            .await?;
    }

    let recv_future = async {
        let mut received = Vec::new();
        while received.len() < total {
            if let Some(msg) = stream.recv().await {
                let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                received.push(payload);
                let _ = consumer.ack(&msg).await;
            }
        }
        received
    };

    let received = timeout(Duration::from_secs(10), recv_future)
        .await
        .expect("timeout receiving key-shared messages");

    assert_eq!(received.len(), total, "should receive all messages");
    Ok(())
}

/// What this test validates
///
/// - Scenario: one producer, two KeyShared consumers on the SAME subscription (non-reliable).
///   Messages with the same key go to the same consumer.
/// - Expectation: per-key affinity — all messages for a given key are received by the same consumer.
///
/// Why this matters
/// - The defining property of Key-Shared: same key → same consumer.
#[tokio::test]
async fn key_shared_per_key_affinity_non_reliable() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_affinity_nr");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_affinity_nr")
        .build()?;
    producer.create().await?;

    let sub = "ks_affinity_nr_sub";

    // Create and subscribe consumers from the SAME client
    let mut consumers = Vec::new();
    for i in 0..2 {
        let cname = format!("ks-cons-{}", i);
        let mut c = client
            .new_consumer()
            .with_topic(topic.clone())
            .with_consumer_name(cname.clone())
            .with_subscription(sub.to_string())
            .with_subscription_type(SubType::KeyShared)
            .build()?;
        c.subscribe().await?;
        let stream = c.receive().await?;
        consumers.push((cname, c, stream));
    }

    sleep(Duration::from_millis(400)).await;

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String, String)>();

    // Spawn receiver tasks
    for (cname, mut cons, mut stream) in consumers.into_iter() {
        let txc = tx.clone();
        tokio::spawn(async move {
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        // Extract the key from the payload: "msg-{key}-{i}"
                        let key = payload.split('-').nth(1).unwrap_or("unknown").to_string();
                        let _ = cons.ack(&msg).await;
                        let _ = txc.send((cname.clone(), key, payload));
                    }
                    None => break,
                }
            }
        });
    }

    let keys = vec!["alpha", "beta", "gamma", "delta"];
    let msgs_per_key = 5;
    let total = keys.len() * msgs_per_key;

    for round in 0..msgs_per_key {
        for key in &keys {
            let payload = format!("msg-{}-{}", key, round);
            let _ = producer
                .send_with_key(payload.into_bytes(), None, key)
                .await?;
        }
        sleep(Duration::from_millis(20)).await;
    }

    // Collect: map each key to the consumer that received it
    let recv_future = async {
        let mut key_to_consumer: HashMap<String, HashSet<String>> = HashMap::new();
        let mut received = 0usize;
        while received < total {
            if let Some((cname, key, _payload)) = rx.recv().await {
                key_to_consumer
                    .entry(key)
                    .or_default()
                    .insert(cname);
                received += 1;
            }
        }
        key_to_consumer
    };

    let key_to_consumer = timeout(Duration::from_secs(15), recv_future)
        .await
        .expect("timeout receiving key-shared affinity messages");

    // Assert: each key was handled by exactly ONE consumer (per-key affinity)
    for (key, consumers) in &key_to_consumer {
        assert_eq!(
            consumers.len(),
            1,
            "key '{}' should be served by exactly 1 consumer, got {:?}",
            key,
            consumers
        );
    }

    // Assert: messages were distributed across at least 2 consumers (parallel processing)
    let all_consumers: HashSet<&String> = key_to_consumer
        .values()
        .flat_map(|s| s.iter())
        .collect();
    assert!(
        all_consumers.len() >= 2,
        "messages should be distributed across multiple consumers, got {:?}",
        all_consumers
    );

    Ok(())
}

/// What this test validates
///
/// - Scenario: reliable producer + two Key-Shared consumers, same subscription.
///   All messages are acked. Verifies per-key affinity under the reliable path.
/// - Expectation: same key → same consumer. All messages delivered and acked.
///
/// Why this matters
/// - The reliable path uses InFlightWindow and SubscriptionEngine.
///   This test ensures the full reliable Key-Shared pipeline works end-to-end.
#[tokio::test]
async fn key_shared_reliable_per_key_affinity() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_affinity_rel");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_affinity_rel")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    let sub = "ks_affinity_rel_sub";

    // Create and subscribe consumers from the SAME client before spawning tasks
    let mut consumers = Vec::new();
    for i in 0..2 {
        let cname = format!("ks-rel-cons-{}", i);
        let mut c = client
            .new_consumer()
            .with_topic(topic.clone())
            .with_consumer_name(cname.clone())
            .with_subscription(sub.to_string())
            .with_subscription_type(SubType::KeyShared)
            .build()?;
        c.subscribe().await?;
        let stream = c.receive().await?;
        consumers.push((cname, c, stream));
    }

    sleep(Duration::from_millis(500)).await;

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String, String)>();

    // Spawn receiver tasks
    for (cname, mut cons, mut stream) in consumers.into_iter() {
        let txc = tx.clone();
        tokio::spawn(async move {
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let key = payload.split('-').nth(1).unwrap_or("unknown").to_string();
                        let _ = cons.ack(&msg).await;
                        let _ = txc.send((cname.clone(), key, payload));
                    }
                    None => break,
                }
            }
        });
    }

    let keys = vec![
        "order", "payment", "shipping", "refund", "invoice",
        "receipt", "delivery", "tracking", "return", "exchange",
    ];
    let msgs_per_key = 3;
    let total = keys.len() * msgs_per_key;

    for round in 0..msgs_per_key {
        for key in &keys {
            let payload = format!("msg-{}-{}", key, round);
            let _ = producer
                .send_with_key(payload.into_bytes(), None, key)
                .await?;
        }
        // Give the reliable dispatcher time to process between rounds
        sleep(Duration::from_millis(100)).await;
    }

    let recv_future = async {
        let mut key_to_consumer: HashMap<String, HashSet<String>> = HashMap::new();
        let mut per_key_order: HashMap<String, Vec<String>> = HashMap::new();
        let mut received = 0usize;
        while received < total {
            if let Some((cname, key, payload)) = rx.recv().await {
                key_to_consumer
                    .entry(key.clone())
                    .or_default()
                    .insert(cname);
                per_key_order.entry(key).or_default().push(payload);
                received += 1;
            }
        }
        (key_to_consumer, per_key_order)
    };

    let (key_to_consumer, per_key_order) = timeout(Duration::from_secs(30), recv_future)
        .await
        .expect("timeout receiving reliable key-shared messages");

    // Assert per-key affinity
    for (key, consumers) in &key_to_consumer {
        assert_eq!(
            consumers.len(),
            1,
            "key '{}' should be served by exactly 1 consumer, got {:?}",
            key,
            consumers
        );
    }

    // Assert per-key ordering: messages for each key should arrive in send order
    for (key, payloads) in &per_key_order {
        for (i, payload) in payloads.iter().enumerate() {
            let expected = format!("msg-{}-{}", key, i);
            assert_eq!(
                payload, &expected,
                "key '{}': message {} out of order, expected '{}' got '{}'",
                key, i, expected, payload
            );
        }
    }

    // Assert distribution
    let all_consumers: HashSet<&String> = key_to_consumer
        .values()
        .flat_map(|s| s.iter())
        .collect();
    let mut per_consumer_count: HashMap<String, usize> = HashMap::new();
    for consumers in key_to_consumer.values() {
        for c in consumers {
            *per_consumer_count.entry(c.clone()).or_insert(0) += 1;
        }
    }
    eprintln!("Reliable distribution: consumers={:?} key_map={:?}", per_consumer_count, key_to_consumer);
    assert!(
        all_consumers.len() >= 2,
        "messages should be distributed across multiple consumers, got {:?}",
        per_consumer_count
    );

    Ok(())
}

/// What this test validates
///
/// - Scenario: reliable Key-Shared with out-of-order acks. Uses a single consumer
///   that receives multiple messages but acks them in reverse order.
/// - Expectation: all messages are delivered and acked successfully.
///
/// Why this matters
/// - The InFlightWindow must handle out-of-order acks and advance the safe cursor
///   only when contiguous offsets are confirmed.
#[tokio::test]
async fn key_shared_reliable_out_of_order_ack() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_ooo_ack");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_ooo_ack")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("cons_ks_ooo_ack".to_string())
        .with_subscription("ks_ooo_ack_sub".to_string())
        .with_subscription_type(SubType::KeyShared)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(400)).await;

    // Send messages with DIFFERENT keys so they all dispatch in parallel
    let total = 10;
    for i in 0..total {
        let key = format!("key-{}", i);
        let payload = format!("ooo-msg-{}", i);
        let _ = producer
            .send_with_key(payload.into_bytes(), None, &key)
            .await?;
    }

    // Receive all messages first, then ack in reverse order
    let recv_future = async {
        let mut msgs = Vec::new();
        while msgs.len() < total {
            if let Some(msg) = stream.recv().await {
                msgs.push(msg);
            }
        }

        // Ack in reverse order (last received first)
        for msg in msgs.iter().rev() {
            let _ = consumer.ack(msg).await;
        }

        msgs.len()
    };

    let count = timeout(Duration::from_secs(15), recv_future)
        .await
        .expect("timeout receiving out-of-order ack messages");

    assert_eq!(count, total, "should receive all messages");

    Ok(())
}
