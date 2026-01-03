use anyhow::Result;
use danube_client::{DanubeClient, SubType};
use std::time::Duration;
use tokio::time::sleep;

/// Simple example showing basic producer/consumer without schema validation.
/// This demonstrates raw byte message passing.
#[tokio::main]
async fn main() -> Result<()> {
    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .await?;

    let topic = "/default/simple_topic";

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name("simple_producer")
        .build();

    producer.create().await?;
    println!("✅ Producer created");

    // Create consumer
    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name("simple_consumer")
        .with_subscription("simple_subscription")
        .with_subscription_type(SubType::Exclusive)
        .build();

    consumer.subscribe().await?;
    println!("✅ Consumer subscribed");

    // Receive messages
    let mut message_stream = consumer.receive().await?;
    let mut count = 0;

    println!("\n🎧 Listening for messages...\n");

    // Spawn producer task
    let producer_handle = tokio::spawn(async move {
        // Send 5 simple messages
        for i in 1..=5 {
            let message = format!("Hello Danube! Message #{}", i);
            let message_id = producer.send(message.as_bytes().to_vec(), None).await?;
            println!("📤 Sent: '{}' (ID: {})", message, message_id);
            sleep(Duration::from_secs(1)).await;
        }

        Ok::<(), anyhow::Error>(())
    });

    while count < 5 {
        if let Some(message) = message_stream.recv().await {
            let payload = String::from_utf8_lossy(&message.payload);
            println!("📥 Received: '{}' (ID: {:?})", payload, message.msg_id);

            // Acknowledge the message
            consumer.ack(&message).await?;
            count += 1;
        }
    }

    // Wait for producer to finish
    producer_handle.await??;

    println!("\n✅ Demo completed! Sent and received 5 messages");
    Ok(())
}
