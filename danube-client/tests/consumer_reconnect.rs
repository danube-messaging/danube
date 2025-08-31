//! Client crate integration test: consumer reconnects after broker failover

use anyhow::Result;
use danube_client::{DanubeClient, SchemaType};
use rand::{thread_rng, Rng};
use rustls::crypto;
use tokio::time::{sleep, timeout, Duration};

async fn setup_client() -> Result<DanubeClient> {
    static INIT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    INIT.get_or_init(|| async {
        let provider = crypto::ring::default_provider();
        provider
            .install_default()
            .expect("Failed to install default CryptoProvider");
    })
    .await;

    let ca_path = std::env::var("DANUBE_CA_CERT").ok()
        .unwrap_or_else(|| {
            if std::path::Path::new("./cert/ca-cert.pem").exists() {
                "./cert/ca-cert.pem".to_string()
            } else {
                "../cert/ca-cert.pem".to_string()
            }
        });

    let client = DanubeClient::builder()
        .service_url("https://127.0.0.1:6650")
        .with_tls(ca_path)?
        .build()
        .await?;

    Ok(client)
}

#[tokio::test]
async fn consumer_reconnect_after_broker_failover() -> Result<()> {
    // Build client
    let client = setup_client().await?;

    let topic = "/default/client-consumer-reconnect";
    let producer_name = format!("producer-{}", thread_rng().gen::<u32>());
    let consumer_name = format!("consumer-{}", thread_rng().gen::<u32>());
    let subscription = format!("sub-{}", thread_rng().gen::<u32>());

    // Create consumer and start receiving
    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name(&consumer_name)
        .with_subscription(&subscription)
        .build();
    consumer.subscribe().await?;
    let mut rx = consumer.receive().await?;

    // Create producer
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(&producer_name)
        .with_schema("schema_str".into(), SchemaType::String)
        .build();
    producer.create().await?;

    // Send first message and expect to receive it
    let _ = producer.send(b"m1".to_vec(), None).await?;
    let _first = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for first message")
        .expect("channel closed");

    // Background workflow will kill A and start B; allow time for reassignments
    sleep(Duration::from_secs(10)).await;

    // Send second message; producer should reconnect; consumer should resubscribe and receive
    let _ = producer.send(b"m2".to_vec(), None).await?;
    let _second = timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("timed out waiting for second message after failover")
        .expect("channel closed after failover");

    Ok(())
}
