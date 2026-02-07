//! Consolidated basic subscription tests for Shared and Exclusive

extern crate danube_client;

use anyhow::Result;
use danube_client::SubType;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

// Helper to run a basic subscription scenario with a single producer and single consumer
async fn run_basic_subscription(topic_prefix: &str, sub_type: SubType) -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic(topic_prefix);

    // Producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_basic")
        .build()?;
    producer.create().await?;

    // Consumer
    let consumer_name = match sub_type {
        SubType::Exclusive => "consumer_exclusive",
        SubType::Shared => "consumer_shared",
        SubType::FailOver => "consumer_failover",
    };
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(format!("test_subscription_{}", consumer_name))
        .with_subscription_type(sub_type)
        .build()?;
    consumer.subscribe().await?;
    let mut message_stream = consumer.receive().await?;

    sleep(Duration::from_millis(300)).await;

    let _ = producer
        .send("Hello Danube".as_bytes().to_vec(), None)
        .await?;

    let receive_future = async {
        if let Some(stream_message) = message_stream.recv().await {
            let payload = String::from_utf8(stream_message.payload.clone()).unwrap();
            assert_eq!(payload, "Hello Danube");
            let _ = consumer.ack(&stream_message).await?;
        }
        Ok::<(), anyhow::Error>(())
    };

    let _ = timeout(Duration::from_secs(10), receive_future).await?;

    Ok(())
}

#[tokio::test]
/// What this test validates
///
/// - Scenario: one producer and one consumer using a `SubType::Shared` subscription name.
/// - Expectation: the consumer receives the published message and acks it successfully.
/// - Example: sending "Hello Danube" should be received as-is by the consumer.
///
/// Why this matters
/// - Baseline sanity for Shared subscription wiring (auth, subscribe, receive, ack) without
///   distribution concerns.
async fn basic_subscription_shared() -> Result<()> {
    run_basic_subscription("/default/sub_basic_shared", SubType::Shared).await
}

#[tokio::test]
/// What this test validates
///
/// - Scenario: one producer and one consumer using a `SubType::Exclusive` subscription name.
/// - Expectation: the consumer receives the published message and acks it successfully.
/// - Example: sending "Hello Danube" should be received as-is by the consumer.
///
/// Why this matters
/// - Baseline sanity for Exclusive subscription wiring; other consumers would be blocked from the
///   same subscription, but this single-consumer path ensures the flow works end-to-end.
async fn basic_subscription_exclusive() -> Result<()> {
    run_basic_subscription("/default/sub_basic_exclusive", SubType::Exclusive).await
}
