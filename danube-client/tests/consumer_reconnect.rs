//! Client crate integration test: consumer reconnects after broker failover

use anyhow::Result;
use danube_client::{DanubeClient, SchemaType};
use rand::{thread_rng, Rng};
use rustls::crypto;
use tokio::time::{sleep, timeout, Duration};

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
async fn consumer_reconnect_after_broker_failover() -> Result<()> {
    // Start Broker A on 6650 / 50051
    let mut handle_a = start_broker(6650, 50051, 9040, "broker-a-client-consumer.log").expect("start broker A");

    // Provision a topic via admin CLI
    std::env::set_var("DANUBE_ADMIN_ENDPOINT", "http://127.0.0.1:50051");
    assert!(run_admin_cli(&["topics", "create", "/default/client-consumer-reconnect"]))
        ;

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

    // Start Broker B and fail Broker A
    let _handle_b = start_broker(6651, 50052, 9041, "broker-b-client-consumer.log").expect("start broker B");
    sleep(Duration::from_secs(2)).await;
    kill_broker(&mut handle_a);

    // Allow reassignment
    sleep(Duration::from_secs(5)).await;

    // Send second message; producer should reconnect; consumer should resubscribe and receive
    let _ = producer.send(b"m2".to_vec(), None).await?;
    let _second = timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("timed out waiting for second message after failover")
        .expect("channel closed after failover");

    Ok(())
}
