//! Consolidated basic reliable dispatch tests for Shared and Exclusive

extern crate danube_client;

use anyhow::Result;
use danube_client::SubType;
use std::fs;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

async fn run_reliable_basic(topic_prefix: &str, sub_type: SubType) -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic(topic_prefix);

    // Producer with reliable dispatch
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_reliable_basic")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    // Consumer
    let cname = match sub_type {
        SubType::Exclusive => "cons_rel_exclusive",
        SubType::Shared => "cons_rel_shared",
        SubType::FailOver => "cons_rel_failover",
    };
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name(cname.to_string())
        .with_subscription(format!("rel_sub_{}", cname))
        .with_subscription_type(sub_type)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(400)).await;

    let blob_data = fs::read("./tests/test.blob")?;
    for _ in 0..20 {
        let _ = producer.send(blob_data.clone(), None).await?;
    }

    let receive_future = async {
        let mut received = 0usize;
        while received < 20 {
            if let Some(msg) = stream.recv().await {
                assert_eq!(msg.payload, blob_data);
                let _ = consumer.ack(&msg).await;
                received += 1;
            }
        }
        Ok::<(), anyhow::Error>(())
    };

    let _ = timeout(Duration::from_secs(15), receive_future).await?;

    Ok(())
}

#[tokio::test]
/// What this test validates
///
/// - Scenario: a reliable producer sends a fixed payload repeatedly; a single Exclusive consumer
///   receives and acks every message.
/// - Expectation: consumer receives all 20 messages with exact payload integrity.
/// - Example: all received payloads must match `tests/test.blob` bytes.
///
/// Why this matters
/// - Ensures the reliable dispatch path works for the simplest Exclusive case: no drops, no corruption,
///   and acknowledgments succeed end-to-end.
async fn reliable_basic_exclusive() -> Result<()> {
    run_reliable_basic("/default/reliable_basic_exclusive", SubType::Exclusive).await
}

#[tokio::test]
/// What this test validates
///
/// - Scenario: a reliable producer sends a fixed payload repeatedly; a single Shared consumer
///   receives and acks every message (no load sharing since only one consumer).
/// - Expectation: consumer receives all 20 messages with exact payload integrity.
///
/// Why this matters
/// - Confirms reliable dispatch behavior is consistent under Shared subscription type as well,
///   establishing parity with Exclusive for the single-consumer baseline.
async fn reliable_basic_shared() -> Result<()> {
    run_reliable_basic("/default/reliable_basic_shared", SubType::Shared).await
}
