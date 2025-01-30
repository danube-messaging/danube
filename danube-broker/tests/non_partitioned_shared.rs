extern crate danube_client;
extern crate futures_util;

use anyhow::Result;
use danube_client::{ConnectionOptions, Consumer, DanubeClient, Producer, SchemaType, SubType};
use rustls::crypto;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio::time::{sleep, timeout, Duration};
use tonic::transport::{Certificate, ClientTlsConfig};

static CRYPTO_PROVIDER: OnceCell<()> = OnceCell::const_new();

struct TestSetup {
    client: Arc<DanubeClient>,
}

async fn setup() -> Result<TestSetup> {
    CRYPTO_PROVIDER
        .get_or_init(|| async {
            let crypto_provider = crypto::ring::default_provider();
            crypto_provider
                .install_default()
                .expect("Failed to install default CryptoProvider");
        })
        .await;

    let tls_config = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(
        std::fs::read("../cert/ca-cert.pem").unwrap(),
    ));

    let connection_options = ConnectionOptions::new().tls_config(tls_config);

    let client = Arc::new(
        DanubeClient::builder()
            .service_url("https://127.0.0.1:6650")
            .with_connection_options(connection_options)
            .build()
            .await?,
    );

    Ok(TestSetup { client })
}

async fn setup_producer(
    client: Arc<DanubeClient>,
    topic: &str,
    producer_name: &str,
) -> Result<Producer> {
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema("my_schema".into(), SchemaType::String)
        .build();

    producer.create().await?;
    Ok(producer)
}

async fn setup_consumer(
    client: Arc<DanubeClient>,
    topic: &str,
    consumer_name: &str,
    sub_type: SubType,
) -> Result<Consumer> {
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.to_string())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(format!("test_subscription_{}", consumer_name))
        .with_subscription_type(sub_type)
        .build();

    consumer.subscribe().await?;
    Ok(consumer)
}

#[tokio::test]
async fn test_shared_subscription() -> Result<()> {
    let setup = setup().await?;
    let topic = "/default/topic_test_shared_subscription";

    let producer = setup_producer(setup.client.clone(), topic, "test_producer_shared").await?;

    let mut consumer = setup_consumer(
        setup.client.clone(),
        topic,
        "test_consumer_shared",
        SubType::Shared,
    )
    .await?;

    let mut message_stream = consumer.receive().await?;

    sleep(Duration::from_millis(500)).await;

    let _message_id = producer
        .send("Hello Danube".as_bytes().into(), None)
        .await?;
    println!("Message sent");

    let receive_future = async {
        if let Some(stream_message) = message_stream.recv().await {
            let payload = String::from_utf8(stream_message.payload).unwrap();
            assert_eq!(payload, "Hello Danube");
            println!("Message received: {}", payload);
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
