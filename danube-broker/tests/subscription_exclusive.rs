extern crate danube_client;
extern crate futures_util;

use anyhow::Result;
use danube_client::{ConnectionOptions, DanubeClient, SchemaType, SubType};
use rustls::crypto;
use tokio::sync::OnceCell;
use tokio::time::{sleep, timeout, Duration};
use tonic::transport::{Certificate, ClientTlsConfig};

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

    let tls_config = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(
        std::fs::read("./cert/ca-cert.pem").unwrap(),
    ));

    let connection_options = ConnectionOptions::new().tls_config(tls_config);

    let client = DanubeClient::builder()
        .service_url("https://127.0.0.1:6650")
        .with_connection_options(connection_options)
        .build()
        .await?;

    Ok(client)
}

#[tokio::test]
async fn test_exclusive_subscription() -> Result<()> {
    let danube_client = setup().await?;
    let topic = "/default/topic_test_exclusive_subscription";
    let producer_name = "test_producer_exclusive";
    let consumer_name = "test_consumer_exclusive";

    // Create the producer
    let mut producer = danube_client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema("my_schema".into(), SchemaType::String)
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
