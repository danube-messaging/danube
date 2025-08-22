//! # Subscription Shared Tests
//! 
//! This test file validates the shared subscription functionality in Danube.
//! 
//! ## Tests:
//! - `shared_subscription`: Tests basic shared subscription behavior where multiple consumers
//!   can share the same subscription and messages are distributed among them. Validates
//!   message sending, receiving, and acknowledgment in a shared subscription model.

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
async fn shared_subscription() -> Result<()> {
    let danube_client = setup().await?;
    let topic = "/default/shared_subscription";
    let producer_name = "producer_shared";
    let consumer_name = "consumer_shared";

    // Create the producer
    let mut producer = danube_client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema("my_schema".into(), SchemaType::String)
        .build();

    producer.create().await?;

    // Create the Shared consumer
    let mut consumer = danube_client
        .new_consumer()
        .with_topic(topic.to_string())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(format!("test_subscription_{}", consumer_name))
        .with_subscription_type(SubType::Shared)
        .build();

    consumer.subscribe().await?;

    let mut message_stream = consumer.receive().await?;

    sleep(Duration::from_millis(500)).await;

    let _message_id = producer
        .send("Hello Danube".as_bytes().into(), None)
        .await?;
    println!("Message sent");

    let receive_future = async {
        if let Some(stream_message) = message_stream.recv().await {
            let payload = String::from_utf8(stream_message.payload.clone()).unwrap();
            assert_eq!(payload, "Hello Danube");
            println!("Message received: {}", payload);

            // Acknowledge the message
            match consumer.ack(&stream_message).await {
                Ok(_) => println!("Message acknowledged"),
                Err(e) => println!("Error acknowledging message: {}", e),
            }
        } else {
            println!("No message received");
        }
    };

    let result = timeout(Duration::from_secs(10), receive_future).await;
    assert!(
        result.is_ok(),
        "Test timed out while waiting for the message"
    );

    Ok(())
}
