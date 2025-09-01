//! Enhanced client reconnection tests with better verification

use anyhow::Result;
use danube_client::{DanubeClient, SchemaType};
use rand::{thread_rng, Rng};
use rustls::crypto;
use std::time::Instant;
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

/// Wait for broker failover with timeout and verification
async fn wait_for_failover_completion() -> Result<()> {
    println!("Waiting for broker failover to complete...");
    
    // Wait for initial failover trigger (broker A shutdown)
    sleep(Duration::from_secs(25)).await;
    
    // Verify broker B is accessible by attempting admin connection
    for attempt in 1..=10 {
        match tokio::net::TcpStream::connect("127.0.0.1:50052").await {
            Ok(_) => {
                println!("✓ Broker B admin port accessible after {} attempts", attempt);
                // Additional settling time for topic reassignments
                sleep(Duration::from_secs(3)).await;
                return Ok(());
            }
            Err(_) => {
                if attempt == 10 {
                    return Err(anyhow::anyhow!("Broker B failed to become accessible"));
                }
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn producer_reconnect_after_broker_failover() -> Result<()> {
    let client = setup_client().await?;
    let topic = "/default/client-reconnect-test";
    let producer_name = format!("enhanced-producer-{}", thread_rng().gen::<u32>());
    
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(&producer_name)
        .with_schema("test_schema".into(), SchemaType::String)
        .build();
    
    // Establish initial connection
    producer.create().await?;
    
    // Send pre-failover message with timing
    let start = Instant::now();
    producer.send(b"pre-failover".to_vec(), None).await?;
    let pre_failover_latency = start.elapsed();
    println!("Pre-failover send latency: {:?}", pre_failover_latency);
    
    // Wait for failover with proper verification
    wait_for_failover_completion().await?;
    
    // Test reconnection with retry verification
    let start = Instant::now();
    let result = timeout(
        Duration::from_secs(30), // Generous timeout for reconnection
        producer.send(b"post-failover".to_vec(), None)
    ).await;
    
    match result {
        Ok(Ok(_)) => {
            let post_failover_latency = start.elapsed();
            println!("✓ Post-failover send successful, latency: {:?}", post_failover_latency);
            
            // Verify reconnection took longer (indicating retry logic)
            if post_failover_latency > pre_failover_latency * 2 {
                println!("✓ Reconnection latency indicates retry logic was used");
            }
        }
        Ok(Err(e)) => return Err(anyhow::anyhow!("Send failed after failover: {}", e)),
        Err(_) => return Err(anyhow::anyhow!("Send timed out after failover")),
    }
    
    // Verify continued operation
    for i in 1..=3 {
        producer.send(format!("stability-test-{}", i).into_bytes(), None).await?;
        sleep(Duration::from_millis(100)).await;
    }
    println!("✓ Producer stability verified after reconnection");
    
    Ok(())
}

#[tokio::test]
async fn consumer_reconnect_after_broker_failover() -> Result<()> {
    let client = setup_client().await?;
    let topic = "/default/client-consumer-reconnect";
    let producer_name = format!("test-producer-{}", thread_rng().gen::<u32>());
    let consumer_name = format!("test-consumer-{}", thread_rng().gen::<u32>());
    let subscription = format!("test-sub-{}", thread_rng().gen::<u32>());
    
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
    
    // Test pre-failover message flow
    producer.send(b"pre-failover-msg".to_vec(), None).await?;
    let _pre_msg = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for pre-failover message")
        .expect("channel closed before pre-failover message");
    println!("✓ Pre-failover message received");
    
    // Wait for failover with proper verification
    wait_for_failover_completion().await?;
    
    // Test post-failover message flow with enhanced retry verification
    let start = Instant::now();
    producer.send(b"post-failover-msg".to_vec(), None).await?;
    let _post_msg = timeout(Duration::from_secs(15), rx.recv())
        .await
        .expect("timed out waiting for post-failover message")
        .expect("channel closed after failover");
    
    let reconnect_latency = start.elapsed();
    println!("✓ Post-failover message flow successful, latency: {:?}", reconnect_latency);
    
    // Verify stability with additional messages
    for i in 1..=3 {
        producer.send(format!("stability-test-{}", i).into_bytes(), None).await?;
        let _msg = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("stability test message timeout")
            .expect("stability test channel closed");
    }
    println!("✓ Consumer reconnection stability verified");
    
    Ok(())
}
