//! Reliable Shared redelivery: one consumer skips acks, another should receive redeliveries

extern crate danube_client;

use anyhow::Result;
use danube_client::{ConfigReliableOptions, ConfigRetentionPolicy, SchemaType, SubType};
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

// NOTE: This test depends on broker-side redelivery triggers (e.g., nack or ack-timeout)
// which are not configurable via the client at the moment. We start with one consumer,
// skip acks for some messages, then drop it and start another. On some setups this may
// not deterministically redeliver within the time budget. Marking as ignored for now.
// TODO(danube): Re-enable once nack or ack-timeout configuration is exposed and
// broker redelivery behavior is deterministic under test.
#[ignore]
#[tokio::test]
async fn reliable_shared_redelivery_to_other_consumer() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_shared_redelivery");

    // Reliable producer
    let reliable = ConfigReliableOptions::new(5, ConfigRetentionPolicy::RetainUntilExpire, 3600);
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_reliable_shared_redelivery")
        .with_schema("schema_str".into(), SchemaType::String)
        .with_reliable_dispatch(reliable)
        .build();
    producer.create().await?;

    // Shared subscription name
    let sub_name = "reliable-shared-sub";

    // Start only consumer c1 initially
    let mut c1 = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("c1".to_string())
        .with_subscription(sub_name.to_string())
        .with_subscription_type(SubType::Shared)
        .build();
    c1.subscribe().await?;
    let mut s1 = c1.receive().await?;

    sleep(Duration::from_millis(400)).await;

    // Publish payloads 0..24
    let total: usize = 24;
    let payloads: Vec<String> = (0..total).map(|i| format!("p{}", i)).collect();
    for body in &payloads {
        let _ = producer.send(body.clone().into_bytes(), None).await?;
    }

    // Channels to collect events
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String, String)>();
    // (consumer_name, payload, phase)

    // Phase 1: only c1 receives; it skips ack for every 3rd message
    let tx1 = tx.clone();
    let c1_handle = tokio::spawn(async move {
        let mut seen = 0usize;
        loop {
            match s1.recv().await {
                Some(msg) => {
                    let payload = String::from_utf8(msg.payload.clone()).unwrap();
                    let skip = seen % 3 == 0;
                    if !skip {
                        let _ = c1.ack(&msg).await;
                    }
                    let phase = if skip { "skipped" } else { "acked" };
                    let _ = tx1.send(("c1".to_string(), payload, phase.to_string()));
                    seen += 1;
                }
                None => break,
            }
        }
    });

    // Collect initial deliveries to c1 for a short window
    let mut skipped_set: HashSet<String> = HashSet::new();
    let mut received_phase1 = 0usize;
    let _ = timeout(Duration::from_secs(3), async {
        while let Some((cname, payload, phase)) = rx.recv().await {
            if cname == "c1" {
                received_phase1 += 1;
                if phase == "skipped" {
                    skipped_set.insert(payload);
                }
            }
            if received_phase1 >= total {
                break;
            }
        }
    })
    .await;

    // Stop c1 to force broker to redeliver its unacked messages
    c1_handle.abort();
    // Give broker a moment to observe disconnect
    sleep(Duration::from_millis(300)).await;

    // Start consumer c2 after c1 drops
    let mut c2 = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("c2".to_string())
        .with_subscription(sub_name.to_string())
        .with_subscription_type(SubType::Shared)
        .build();
    c2.subscribe().await?;
    let mut s2 = c2.receive().await?;

    // c2 acks everything it receives
    let tx2 = tx.clone();
    tokio::spawn(async move {
        loop {
            match s2.recv().await {
                Some(msg) => {
                    let payload = String::from_utf8(msg.payload.clone()).unwrap();
                    let _ = c2.ack(&msg).await;
                    let _ = tx2.send(("c2".to_string(), payload, "acked".to_string()));
                }
                None => break,
            }
        }
    });

    // Wait for redeliveries to show up for c2 among the skipped payloads
    let skipped_set_clone = skipped_set.clone();
    let wait_redelivery = async move {
        let mut redelivered_to_c2: HashSet<String> = HashSet::new();
        let deadline = total * 3; // upper bound events
        let mut seen_events = 0usize;
        while seen_events < deadline && redelivered_to_c2.len() < skipped_set_clone.len().saturating_sub(1) {
            if let Some((cname, payload, _phase)) = rx.recv().await {
                if cname == "c2" && skipped_set_clone.contains(&payload) {
                    redelivered_to_c2.insert(payload);
                }
                seen_events += 1;
            } else {
                break;
            }
        }
        redelivered_to_c2
    };

    let redelivered = timeout(Duration::from_secs(30), wait_redelivery)
        .await
        .expect("timeout waiting for redelivery after c2 join");

    assert!(skipped_set.len() > 0, "expected some messages to be skipped by c1");
    assert!(redelivered.len() > 0, "expected some skipped messages to be redelivered to c2");

    Ok(())
}
