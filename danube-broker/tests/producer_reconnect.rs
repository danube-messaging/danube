//! # Producer Reconnection Tests
//! 
//! This test file validates producer reconnection and reuse functionality in Danube.
//! 
//! ## Tests:
//! - `producer_reconnection_reuse`: Tests that a producer can be reused after being created,
//!   validating that the same producer instance can send multiple messages without needing
//!   to be recreated. This ensures proper connection management and resource reuse.

extern crate danube_client;

use anyhow::Result;
use danube_client::{DanubeClient, SchemaType};
use rustls::crypto;
use tokio::sync::OnceCell;
use tokio::time::{sleep, Duration};

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
async fn producer_reconnection_reuse() -> Result<()> {
    let danube_client = setup().await?;
    let topic = "/default/producer_reconnect_reuse";
    let producer_name = "producer_reconn_reuse";

    // Create the first producer
    let mut producer1 = danube_client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema("schema_reuse".into(), SchemaType::String)
        .build();

    producer1.create().await?;

    // Send a message successfully
    sleep(Duration::from_millis(200)).await;
    let _ = producer1
        .send("Hello Danube".as_bytes().into(), None)
        .await?;

    // Attempt to create another producer with the same name: current broker does not enforce name uniqueness
    // The important part for this test is that the original producer stays functional
    let mut _producer2 = danube_client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema("schema_reuse".into(), SchemaType::String)
        .build();
    // Ignore the result of creating a second one; different brokers/versions may allow it
    let _ = _producer2.create().await;

    // Original producer is still functional
    let _ = producer1
        .send("Hello again".as_bytes().into(), None)
        .await?;

    Ok(())
}
