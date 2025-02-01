extern crate danube_client;
extern crate futures_util;

use anyhow::Result;
use danube_client::{ConfigReliableOptions, ConfigRetentionPolicy, DanubeClient, SubType};
use rustls::crypto;
use std::fs;
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
async fn reliable_shared_dispatch() -> Result<()> {
    let danube_client = setup().await?;
    let topic = "/default/reliable_shared";
    let producer_name = "producer_reliable_shared";
    let consumer_name = "consumer_reliable_shared";
    let blob_data = fs::read("./tests/test.blob")?;

    let reliable_options =
        ConfigReliableOptions::new(5, ConfigRetentionPolicy::RetainUntilExpire, 3600);

    let mut producer = danube_client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_reliable_dispatch(reliable_options)
        .build();

    producer.create().await?;

    let mut consumer = danube_client
        .new_consumer()
        .with_topic(topic.to_string())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(format!("reliable_subscription_{}", consumer_name))
        .with_subscription_type(SubType::Shared)
        .build();

    consumer.subscribe().await?;

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
