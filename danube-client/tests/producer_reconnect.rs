//! Client crate integration test: producer reconnects after broker failover

use anyhow::Result;
use danube_client::{DanubeClient, SchemaType};
use rand::{thread_rng, Rng};
use rustls::crypto;
use tokio::time::{sleep, Duration};

// no local helpers; CI workflow manages brokers

async fn setup_client() -> Result<DanubeClient> {
    static INIT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    INIT
        .get_or_init(|| async {
            let provider = crypto::ring::default_provider();
            provider
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
async fn producer_reconnect_after_broker_failover() -> Result<()> {
    // Build client
    let client = setup_client().await?;

    // Create producer and send baseline message
    let topic = "/default/client-reconnect-test";
    let producer_name = format!("producer-{}", thread_rng().gen::<u32>());
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(&producer_name)
        .with_schema("schema_reuse".into(), SchemaType::String)
        .build();
    producer.create().await?;

    sleep(Duration::from_millis(200)).await;
    let _ = producer.send(b"hello before failover".to_vec(), None).await?;

    // Workflow will kill A and bring up B in the background.
    // Allow time for failover and reassignment to converge.
    sleep(Duration::from_secs(10)).await;

    // Attempt to send again; client should reconnect and succeed
    let _ = producer
        .send(b"hello after failover".to_vec(), None)
        .await?;

    Ok(())
}
