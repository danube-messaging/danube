//! Consolidated basic partitioned subscription tests for Shared and Exclusive

extern crate danube_client;

use anyhow::Result;
use danube_client::{SchemaType, SubType};
use std::collections::HashSet;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

async fn run_partitioned_basic(topic_prefix: &str, sub_type: SubType, partitions: usize) -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic(topic_prefix);

    // Partitioned producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_part_basic")
        .with_schema("my_schema".into(), SchemaType::String)
        .with_partitions(partitions)
        .build();
    producer.create().await?;

    // Single consumer (sub_type)
    let cname = match sub_type {
        SubType::Exclusive => "cons_part_exclusive",
        SubType::Shared => "cons_part_shared",
        SubType::FailOver => "cons_part_failover",
    };
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name(cname.to_string())
        .with_subscription(format!("sub_part_{}", cname))
        .with_subscription_type(sub_type)
        .build();
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(300)).await;

    let messages = vec!["Hello Danube 1", "Hello Danube 2", "Hello Danube 3"];
    for msg in &messages {
        let _ = producer.send(msg.as_bytes().to_vec(), None).await?;
    }

    let receive_future = async {
        let mut received = Vec::new();
        let mut parts = HashSet::new();
        while let Some(m) = stream.recv().await {
            let payload = String::from_utf8(m.payload.clone()).unwrap();
            received.push(payload);
            parts.insert(m.msg_id.topic_name.clone());
            let _ = consumer.ack(&m).await;
            if received.len() == messages.len() { break; }
        }
        (received, parts)
    };

    let (received_messages, parts) = timeout(Duration::from_secs(10), receive_future)
        .await?
        ;

    // Validate counts
    assert_eq!(received_messages.len(), messages.len(), "Not all messages were received");
    for expected in &messages {
        assert!(received_messages.contains(&expected.to_string()), "Missing message {}", expected);
    }

    // Partition coverage across received set
    for i in 0..partitions {
        let expected = format!("{}-part-{}", topic, i);
        assert!(parts.contains(&expected), "missing partition {}", expected);
    }

    Ok(())
}

#[tokio::test]
/// What this test validates
///
/// - Scenario: a single consumer with `SubType::Exclusive` reads from a partitioned topic (3 parts).
/// - Expectations:
///   - The consumer receives all produced messages.
///   - Partition coverage across the received set includes all `{topic}-part-0..2`.
/// - Example: produce 3 messages; consumer should receive all 3 and we should observe all partitions
///   among `msg_id.topic_name` over time.
///
/// Why this matters
/// - Confirms that a single consumer transparently merges streams from all partitions in Exclusive mode.
async fn partitioned_basic_exclusive() -> Result<()> {
    run_partitioned_basic("/default/part_basic_excl", SubType::Exclusive, 3).await
}

#[tokio::test]
/// What this test validates
///
/// - Scenario: a single consumer with `SubType::Shared` reads from a partitioned topic (3 parts).
/// - Expectations:
///   - The consumer receives all produced messages (no load sharing because there's only one consumer).
///   - Partition coverage across the received set includes all `{topic}-part-0..2`.
///
/// Why this matters
/// - Confirms that Shared subscription mode also merges all partitions for a single consumer and that
///   partitioning does not affect the ability to receive the complete stream.
async fn partitioned_basic_shared() -> Result<()> {
    run_partitioned_basic("/default/part_basic_shared", SubType::Shared, 3).await
}
