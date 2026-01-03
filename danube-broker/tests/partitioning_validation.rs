//! # Partitioning Validation Tests
//!
//! This test file validates topic partitioning functionality in Danube.
//!
//! ## Tests:
//! - `partitioning_validation`: Tests that partitioned topics work correctly by creating
//!   a partitioned topic, sending messages to it, and verifying that messages are received
//!   with the correct partition information in their message IDs.

extern crate danube_client;

use anyhow::Result;
use danube_client::DanubeClient;
use rustls::crypto;
use tokio::sync::OnceCell;
use tokio::time::{sleep, timeout, Duration};

static CRYPTO_PROVIDER: OnceCell<()> = OnceCell::const_new();

async fn setup_client() -> Result<DanubeClient> {
    CRYPTO_PROVIDER
        .get_or_init(|| async {
            let crypto_provider = crypto::ring::default_provider();
            crypto_provider
                .install_default()
                .expect("Failed to install default CryptoProvider");
        })
        .await;

    let client = DanubeClient::builder()
        .service_url("https://127.0.0.1:6650")
        .with_tls("../cert/ca-cert.pem")?
        .build()
        .await?;

    Ok(client)
}

// Admin not needed for this test variant

#[tokio::test]
/// What this test validates
///
/// - Scenario: verify topic name semantics for non-partitioned topics and partition coverage for
///   partitioned topics (3 partitions).
/// - Expectations:
///   - For a non-partitioned topic, `msg_id.topic_name` matches the base topic (no `-part-` suffix).
///   - For a partitioned topic, received messages include all partition names `{topic}-part-0..2`.
/// - Example: produce a couple of messages to the partitioned topic and observe that the set of
///   `msg_id.topic_name` values spans all partitions.
///
/// Why this matters
/// - Confirms the broker/client surface correct topic identifiers in message IDs and that partitioned
///   producers dispatch across all partitions so consumers can observe coverage.
async fn partitioning_validation() -> Result<()> {
    let danube_client = setup_client().await?;

    // Non-partitioned topic via producer/consumer and validate topic_name in messages
    let np_topic = "/default/partitioning_validation_np";
    let mut np_producer = danube_client
        .new_producer()
        .with_topic(np_topic)
        .with_name("producer_part_np_lookup")
        .build();
    np_producer.create().await?;
    let mut np_consumer = danube_client
        .new_consumer()
        .with_topic(np_topic.to_string())
        .with_consumer_name("consumer_np".to_string())
        .with_subscription("sub_np".to_string())
        .build();
    np_consumer.subscribe().await?;
    let mut np_stream = np_consumer.receive().await?;
    sleep(Duration::from_millis(200)).await;
    let _ = np_producer.send(b"np".to_vec(), None).await?;
    let np_recv = timeout(Duration::from_secs(5), async { np_stream.recv().await }).await?;
    let np_msg = np_recv.expect("expected np message");
    assert_eq!(np_msg.msg_id.topic_name, np_topic);

    // Partitioned topic via producer with partitions
    let p_topic = "/default/partitioning_validation";
    let partitions = 3;
    let mut p_producer = danube_client
        .new_producer()
        .with_topic(p_topic)
        .with_name("producer_part_lookup")
        .with_partitions(partitions)
        .build();
    p_producer.create().await?;
    let mut p_consumer = danube_client
        .new_consumer()
        .with_topic(p_topic.to_string())
        .with_consumer_name("consumer_p".to_string())
        .with_subscription("sub_p".to_string())
        .build();
    p_consumer.subscribe().await?;
    let mut p_stream = p_consumer.receive().await?;
    sleep(Duration::from_millis(300)).await;
    for i in 0..(partitions * 2) {
        let _ = p_producer
            .send(format!("p{}", i).into_bytes(), None)
            .await?;
    }
    let mut seen_parts = std::collections::HashSet::new();
    let _ = timeout(Duration::from_secs(10), async {
        while let Some(msg) = p_stream.recv().await {
            seen_parts.insert(msg.msg_id.topic_name.clone());
            if seen_parts.len() == partitions {
                break;
            }
        }
    })
    .await;
    for i in 0..partitions {
        let expected = format!("{}-part-{}", p_topic, i);
        assert!(
            seen_parts.contains(&expected),
            "missing partition {}",
            expected
        );
    }

    Ok(())
}
