//! Partitioned variants for queue (Shared) and pub-sub fan-out (Exclusive)

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
/// - Scenario: a producer writes to a partitioned topic (3 partitions). Three consumers share the
///   SAME subscription with `SubType::Shared` (queue semantics).
/// - Expectations:
///   - Partition coverage: across all deliveries we should see messages from each partition name
///     (e.g., `{topic}-part-0/1/2`).
///   - Distribution: messages are split across the three consumers near-evenly, even though they
///     originate from different partitions.
/// - Example: with 60 messages and 3 consumers, each should receive roughly 20 messages overall,
///   within a small tolerance due to per-partition scheduling.
///
/// Why this matters
/// - In Danube, a single logical consumer merges streams from all partitions. Under Shared
///   subscriptions, the broker distributes messages across the group to scale processing.
///
#[tokio::test]
async fn partitioned_queue_shared_distribution_and_partition_coverage() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/part_queue_shared");

    // Partitioned producer
    let partitions = 3;
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_part_queue_shared")
        .with_partitions(partitions)
        .build()?;
    producer.create().await?;

    // Three shared consumers on same subscription
    let sub_name = "part-queue-shared-sub";
    let mut consumers = Vec::new();
    for i in 0..3 {
        let cname = format!("pq-cons-{}", i);
        let mut cons = client
            .new_consumer()
            .with_topic(topic.clone())
            .with_consumer_name(cname.clone())
            .with_subscription(sub_name.to_string())
            .with_subscription_type(SubType::Shared)
            .build()?;
        cons.subscribe().await?;
        let stream = cons.receive().await?;
        consumers.push((cname, cons, stream));
    }

    sleep(Duration::from_millis(500)).await;

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String, String)>();
    // (consumer_name, payload, topic_name)

    for (cname, mut cons, mut stream) in consumers.into_iter() {
        let txc = tx.clone();
        tokio::spawn(async move {
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.clone()).unwrap();
                        let topic_name = msg.msg_id.topic_name.clone();
                        let _ = cons.ack(&msg).await;
                        let _ = txc.send((cname.clone(), payload, topic_name));
                    }
                    None => break,
                }
            }
        });
    }

    let total = 60; // multiple of 3 partitions and 3 consumers
    for i in 0..total {
        let body = format!("pm{}", i);
        let _ = producer.send(body.clone().into_bytes(), None).await?;
    }

    let recv_future = async move {
        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut parts: HashSet<String> = HashSet::new();
        let mut received = 0usize;
        while received < total {
            if let Some((cname, _payload, topic_name)) = rx.recv().await {
                *counts.entry(cname).or_insert(0) += 1;
                parts.insert(topic_name);
                received += 1;
            }
        }
        (counts, parts)
    };

    let (counts, parts) = timeout(Duration::from_secs(20), recv_future)
        .await
        .expect("timeout receiving partitioned queue messages");

    // Partition coverage
    for i in 0..partitions {
        let expected = format!("{}-part-{}", topic, i);
        assert!(parts.contains(&expected), "missing partition {}", expected);
    }

    // Distribution (allow variance due to per-partition scheduling and timing)
    for i in 0..3 {
        let cname = format!("pq-cons-{}", i);
        let got = counts.get(&cname).cloned().unwrap_or(0);
        let expected = total / 3;
        let tolerance = std::cmp::max(2, expected / 5); // allow +/- max(2, 20% of expected)
        assert!(
            (got as isize - expected as isize).abs() <= tolerance as isize,
            "{} should receive near-equal share: got {}, expected ~{} (+/-{})",
            cname,
            got,
            expected,
            tolerance
        );
    }

    Ok(())
}

/// What this test validates
///
/// - Scenario: a producer writes to a partitioned topic (3 partitions). Three consumers each use a
///   UNIQUE `SubType::Exclusive` subscription (fan-out/broadcast semantics).
/// - Expectations:
///   - Every consumer receives ALL messages (no sharing across consumers).
///   - Partition coverage observed across the received set.
/// - Example: if 45 messages are produced, each of the 3 consumers should receive 45.
///
/// Why this matters
/// - Unique Exclusive subscriptions model independent readers of the same topic. Fan-out must
///   preserve complete delivery to each subscriber regardless of partitioning.
///
#[tokio::test]
async fn partitioned_pubsub_fanout_exclusive_full_receipt() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/part_fanout_excl");

    // Partitioned producer
    let partitions = 3;
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_part_fanout_excl")
        .with_partitions(partitions)
        .build()?;
    producer.create().await?;

    // Three exclusive consumers, each with unique subscription name
    let mut consumers = Vec::new();
    for i in 0..3 {
        let cname = format!("pfe-cons-{}", i);
        let sub = format!("pfe-sub-{}", i);
        let mut cons = client
            .new_consumer()
            .with_topic(topic.clone())
            .with_consumer_name(cname.clone())
            .with_subscription(sub)
            .with_subscription_type(SubType::Exclusive)
            .build()?;
        cons.subscribe().await?;
        let stream = cons.receive().await?;
        consumers.push((cname, cons, stream));
    }

    sleep(Duration::from_millis(500)).await;

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String, String)>();

    for (cname, mut cons, mut stream) in consumers.into_iter() {
        let txc = tx.clone();
        tokio::spawn(async move {
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.clone()).unwrap();
                        let topic_name = msg.msg_id.topic_name.clone();
                        let _ = cons.ack(&msg).await;
                        let _ = txc.send((cname.clone(), payload, topic_name));
                    }
                    None => break,
                }
            }
        });
    }

    let total = 45;
    let expected_payloads: Vec<String> = (0..total).map(|i| format!("pm{}", i)).collect();
    for body in &expected_payloads {
        let _ = producer.send(body.clone().into_bytes(), None).await?;
    }

    let recv_future = async move {
        let mut per_consumer: HashMap<String, HashSet<String>> = HashMap::new();
        let mut part_seen: HashSet<String> = HashSet::new();
        let need = total;
        let mut done = 0usize;
        while done < 3 {
            if let Some((cname, payload, topic_name)) = rx.recv().await {
                part_seen.insert(topic_name);
                let entry = per_consumer.entry(cname.clone()).or_default();
                entry.insert(payload);
                if entry.len() == need {
                    done += 1;
                }
            }
        }
        (per_consumer, part_seen)
    };

    let (per_consumer, parts_seen) = timeout(Duration::from_secs(25), recv_future)
        .await
        .expect("timeout receiving partitioned fanout messages");

    // Partition coverage (seen across the fleet)
    for i in 0..partitions {
        let expected = format!("{}-part-{}", topic, i);
        assert!(
            parts_seen.contains(&expected),
            "missing partition {}",
            expected
        );
    }

    // All consumers received all payloads
    for i in 0..3 {
        let cname = format!("pfe-cons-{}", i);
        let got = per_consumer.get(&cname).cloned().unwrap_or_default();
        assert_eq!(got.len(), total, "{} should receive all messages", cname);
        for body in &expected_payloads {
            assert!(got.contains(body), "{} missing {}", cname, body);
        }
    }

    Ok(())
}
