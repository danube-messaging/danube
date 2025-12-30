//! Queue semantics test: Shared subscription with multiple consumers

extern crate danube_client;

use anyhow::Result;
use danube_client::SubType;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// What this test validates
///
/// - Scenario: one producer sends N messages to a topic; three consumers subscribe with the SAME
///   subscription name and `SubType::Shared` (queue semantics).
/// - Expectation: messages are distributed across consumers (approximately round-robin). Each message
///   should be delivered to exactly one consumer. We assert near-equal counts per consumer.
/// - Example: with 36 messages and 3 consumers, each consumer should receive ~12 messages.
///
/// Why this matters
/// - Shared subscriptions implement queue semantics (work is split among consumers). This ensures
///   horizontal scaling where adding consumers increases parallelism without duplicating deliveries.
///
#[tokio::test]
async fn queue_shared_round_robin_distribution() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/queue_shared");

    // Producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_queue_shared")
        .build();
    producer.create().await?;

    // Consumers: 3 consumers, same subscription name
    let sub_name = "queue-shared-sub";
    let mut consumers = Vec::new();
    for i in 0..3 {
        let cname = format!("queue-cons-{}", i);
        let mut cons = client
            .new_consumer()
            .with_topic(topic.clone())
            .with_consumer_name(cname.clone())
            .with_subscription(sub_name.to_string())
            .with_subscription_type(SubType::Shared)
            .build();
        cons.subscribe().await?;
        let stream = cons.receive().await?;
        consumers.push((cname, cons, stream));
    }

    sleep(Duration::from_millis(400)).await;

    // Broadcast sender to collect all deliveries
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String)>();

    // Spawn receivers
    for (cname, mut cons, mut stream) in consumers.into_iter() {
        let txc = tx.clone();
        tokio::spawn(async move {
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.clone()).unwrap();
                        // ack
                        let _ = cons.ack(&msg).await;
                        let _ = txc.send((cname.clone(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    // Send messages
    let total: usize = 36; // divisible by 3 for round-robin assertion simplicity
    for i in 0..total {
        let body = format!("m{}", i);
        let _ = producer.send(body.clone().into_bytes(), None).await?;
    }

    // Collect until all received
    let mut counts: HashMap<String, usize> = HashMap::new();
    let recv_future = async {
        let mut received = 0usize;
        while received < total {
            if let Some((cname, _payload)) = rx.recv().await {
                *counts.entry(cname).or_insert(0) += 1;
                received += 1;
            }
        }
        counts
    };

    let counts = timeout(Duration::from_secs(15), recv_future)
        .await
        .expect("timeout receiving queue messages");

    // Assert near round-robin: expect exactly total/3 each (since perfect conditions)
    for i in 0..3 {
        let cname = format!("queue-cons-{}", i);
        let got = counts.get(&cname).cloned().unwrap_or(0);
        assert_eq!(got, total / 3, "{} should receive equal share", cname);
    }

    Ok(())
}
