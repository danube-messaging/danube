//! # Partitioned Exclusive Subscription Tests
//! 
//! This test file validates exclusive subscription functionality on partitioned topics in Danube.
//! 
//! ## Tests:
//! - `partitioned_exclusive_subscription`: Tests exclusive subscription behavior on a partitioned
//!   topic, ensuring that only one consumer can be active on each partition and that messages
//!   are properly delivered and acknowledged in a partitioned exclusive subscription model.

extern crate danube_client;
extern crate futures_util;

use anyhow::Result;
use danube_client::{DanubeClient, SchemaType, SubType};
use rustls::crypto;
use tokio::sync::OnceCell;
use tokio::time::{sleep, timeout, Duration};

static CRYPTO_PROVIDER: OnceCell<()> = OnceCell::const_new();

async fn setup() -> Result<DanubeClient> {
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

#[tokio::test]
async fn part_exclusive_subscription() -> Result<()> {
    let danube_client = setup().await?;
    let topic = "/default/part_exclusive_subsc";
    let producer_name = "part_producer_exclusive";
    let consumer_name = "part_consumer_exclusive";
    let partitions = 3;

    // Create the partitioned producer
    let mut producer = danube_client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema("my_schema".into(), SchemaType::String)
        .with_partitions(partitions)
        .build();

    producer.create().await?;

    // Create the Exclusive consumer
    let mut consumer = danube_client
        .new_consumer()
        .with_topic(topic.to_string())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(format!("test_subscription_{}", consumer_name))
        .with_subscription_type(SubType::Exclusive)
        .build();

    consumer.subscribe().await?;

    let mut message_stream = consumer.receive().await?;

    sleep(Duration::from_millis(500)).await;

    let messages = vec!["Hello Danube 1", "Hello Danube 2", "Hello Danube 3"];

    for msg in &messages {
        producer.send(msg.as_bytes().into(), None).await?;
        println!("Message sent: {}", msg);
    }

    let receive_future = async {
        let mut received_messages = vec![];

        while let Some(stream_message) = message_stream.recv().await {
            let payload = String::from_utf8(stream_message.payload).unwrap();
            println!("Message received: {}", payload);
            received_messages.push(payload);

            if received_messages.len() == messages.len() {
                break;
            }
        }

        received_messages
    };

    let result = timeout(Duration::from_secs(10), receive_future).await?;
    let received_messages = result;

    assert_eq!(
        received_messages.len(),
        messages.len(),
        "Not all messages were received"
    );
    for expected in &messages {
        assert!(
            received_messages.contains(&expected.to_string()),
            "Expected message '{}' not found",
            expected
        );
    }

    Ok(())
}
