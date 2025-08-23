//! Pub-Sub fan-out test: Exclusive with unique subscription per consumer

extern crate danube_client;

use anyhow::Result;
use danube_client::{SchemaType, SubType};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// What this test validates
///
/// - Scenario: one producer sends N messages; three consumers subscribe with UNIQUE subscription
///   names and `SubType::Exclusive` (classic pub-sub fan-out).
/// - Expectation: every consumer receives ALL messages (no load sharing), because each unique
///   subscription is an independent stream.
/// - Example: with 24 messages and 3 unique Exclusive subscriptions, each consumer should receive 24.
///
/// Why this matters
/// - Pub-sub fan-out is the core broadcast primitive: adding subscribers should not reduce what
///   the others receive. This test ensures no message is dropped or shared between unique subscriptions.
///
#[tokio::test]
async fn pubsub_fanout_exclusive_unique_subscriptions() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/fanout_exclusive");

    // Producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_fanout_exclusive")
        .with_schema("schema_str".into(), SchemaType::String)
        .build();
    producer.create().await?;

    // Consumers: 3 consumers, each with unique Exclusive subscription name
    let mut consumers = Vec::new();
    for i in 0..3 {
        let cname = format!("fanout-cons-{}", i);
        let sub_name = format!("fanout-sub-{}", i);
        let mut cons = client
            .new_consumer()
            .with_topic(topic.clone())
            .with_consumer_name(cname.clone())
            .with_subscription(sub_name)
            .with_subscription_type(SubType::Exclusive)
            .build();
        cons.subscribe().await?;
        let stream = cons.receive().await?;
        consumers.push((cname, cons, stream));
    }

    sleep(Duration::from_millis(400)).await;

    // Channels to gather per-consumer receipts
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String)>();

    for (cname, mut cons, mut stream) in consumers.into_iter() {
        let txc = tx.clone();
        tokio::spawn(async move {
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.clone()).unwrap();
                        let _ = cons.ack(&msg).await; // ack
                        let _ = txc.send((cname.clone(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    // Publish messages
    let total: usize = 24;
    let expected_payloads: Vec<String> = (0..total).map(|i| format!("m{}", i)).collect();
    for body in &expected_payloads {
        let _ = producer.send(body.clone().into_bytes(), None).await?;
    }

    // Collect receipts until each consumer got all messages
    let recv_future = async {
        let mut per_consumer: HashMap<String, Vec<String>> = HashMap::new();
        let needed_per_consumer = total;
        let mut consumers_done = 0usize;
        while consumers_done < 3 {
            if let Some((cname, payload)) = rx.recv().await {
                let entry = per_consumer.entry(cname.clone()).or_default();
                entry.push(payload);
                if entry.len() == needed_per_consumer {
                    consumers_done += 1;
                }
            }
        }
        per_consumer
    };

    let per_consumer = timeout(Duration::from_secs(20), recv_future)
        .await
        .expect("timeout receiving fan-out messages");

    // Assert every consumer received the full set
    for i in 0..3 {
        let cname = format!("fanout-cons-{}", i);
        let got = per_consumer.get(&cname).cloned().unwrap_or_default();
        assert_eq!(got.len(), total, "{} should receive all messages", cname);
        for body in &expected_payloads {
            assert!(got.contains(body), "{} missing message {}", cname, body);
        }
    }

    Ok(())
}
