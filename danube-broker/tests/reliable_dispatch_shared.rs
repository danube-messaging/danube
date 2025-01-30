extern crate danube_client;
extern crate futures_util;

use anyhow::Result;
use danube_client::{
    ConfigReliableOptions, ConfigRetentionPolicy, ConnectionOptions, Consumer, DanubeClient,
    Producer, SubType,
};
use rustls::crypto;
use std::fs;
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

async fn setup_reliable_producer(
    client: Arc<DanubeClient>,
    topic: &str,
    producer_name: &str,
) -> Result<Producer> {
    let reliable_options =
        ConfigReliableOptions::new(5, ConfigRetentionPolicy::RetainUntilExpire, 3600);

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_reliable_dispatch(reliable_options)
        .build();

    producer.create().await?;
    Ok(producer)
}

async fn setup_reliable_consumer(
    client: Arc<DanubeClient>,
    topic: &str,
    consumer_name: &str,
    sub_type: SubType,
) -> Result<Consumer> {
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.to_string())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(format!("reliable_subscription_{}", consumer_name))
        .with_subscription_type(sub_type)
        .build();

    consumer.subscribe().await?;
    Ok(consumer)
}

#[tokio::test]
async fn test_reliable_shared_delivery() -> Result<()> {
    let setup = setup().await?;
    let topic = "/default/topic_test_reliable_shared";
    let blob_data = fs::read("./tests/test.blob")?;

    let producer =
        setup_reliable_producer(setup.client.clone(), topic, "test_producer_reliable_shared")
            .await?;

    let mut consumer = setup_reliable_consumer(
        setup.client.clone(),
        topic,
        "test_consumer_reliable_shared",
        SubType::Shared,
    )
    .await?;

    let mut message_stream = consumer.receive().await?;

    sleep(Duration::from_millis(500)).await;

    for i in 0..20 {
        let blob_clone = blob_data.clone();
        let message_id = producer.send(blob_clone, None).await?;
        println!("Blob message {} sent with id: {}", i, message_id);
    }

    let receive_future = async {
        for i in 0..20 {
            if let Some(stream_message) = message_stream.recv().await {
                assert_eq!(stream_message.payload, blob_data);
                println!("Received blob message #{}", i);
                consumer.ack(&stream_message).await?;
            }
        }
        Ok::<(), anyhow::Error>(())
    };

    let result = timeout(Duration::from_secs(10), receive_future).await;
    assert!(result.is_ok(), "Test timed out while waiting for messages");

    Ok(())
}
