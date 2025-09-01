//! Local reconnection tests that can run without broker orchestration

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

    let client = DanubeClient::builder()
        .service_url("https://127.0.0.1:6650")
        .with_tls("../cert/ca-cert.pem")?
        .build()
        .await?;

    Ok(client)
}

#[tokio::test]
#[ignore] // Only run when broker infrastructure is manually started
async fn local_producer_basic_test() -> Result<()> {
    let client = setup_client().await?;
    let topic = "/default/local-test";
    let producer_name = format!("local-producer-{}", thread_rng().gen::<u32>());
    
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(&producer_name)
        .with_schema("test_schema".into(), SchemaType::String)
        .build();
    
    // Test basic producer functionality
    producer.create().await?;
    producer.send(b"test message".to_vec(), None).await?;
    
    println!("✓ Local producer test successful");
    Ok(())
}

#[tokio::test]
#[ignore] // Only run when broker infrastructure is manually started
async fn local_consumer_basic_test() -> Result<()> {
    let client = setup_client().await?;
    let topic = "/default/local-test";
    let producer_name = format!("local-producer-{}", thread_rng().gen::<u32>());
    let consumer_name = format!("local-consumer-{}", thread_rng().gen::<u32>());
    let subscription = format!("local-sub-{}", thread_rng().gen::<u32>());
    
    // Setup consumer
    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name(&consumer_name)
        .with_subscription(&subscription)
        .build();
    
    consumer.subscribe().await?;
    let mut rx = consumer.receive().await?;
    
    // Setup producer
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(&producer_name)
        .with_schema("test_schema".into(), SchemaType::String)
        .build();
    producer.create().await?;
    
    // Test message flow
    producer.send(b"local test message".to_vec(), None).await?;
    let _msg = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for message")
        .expect("channel closed");
    
    println!("✓ Local consumer test successful");
    Ok(())
}
