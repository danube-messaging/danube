//! Client crate integration test: producer reconnects after broker failover

use anyhow::Result;
use danube_client::{DanubeClient, SchemaType};
use rand::{thread_rng, Rng};
use rustls::crypto;
use tokio::time::{sleep, Duration};

mod common;
use common::{kill_broker, run_admin_cli, start_broker};

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
        .with_tls("./cert/ca-cert.pem")?
        .build()
        .await?;

    Ok(client)
}

#[tokio::test]
async fn producer_reconnect_after_broker_failover() -> Result<()> {
    // Start Broker A on 6650 / 50051
    let mut handle_a = start_broker(6650, 50051, 9040, "broker-a-client.log").expect("start broker A");

    // Provision a topic via admin CLI
    std::env::set_var("DANUBE_ADMIN_ENDPOINT", "http://127.0.0.1:50051");
    assert!(run_admin_cli(&["topics", "create", "/default/client-reconnect-test"]))
        ;

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

    // Start Broker B on 6651 / 50052, then kill A to trigger reassignment
    let _handle_b = start_broker(6651, 50052, 9041, "broker-b-client.log").expect("start broker B");

    // Give brokers time to discover each other
    sleep(Duration::from_secs(2)).await;

    // Kill broker A
    kill_broker(&mut handle_a);

    // Allow LoadManager and reassignment to converge
    sleep(Duration::from_secs(5)).await;

    // Attempt to send again; client should reconnect and succeed
    let _ = producer
        .send(b"hello after failover".to_vec(), None)
        .await?;

    Ok(())
}
