use anyhow::Result;
use danube_client::{DanubeClient, SubType};
use serde::Deserialize;

// Define the message structure matching the Avro schema
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct UserEvent {
    user_id: String,
    action: String,
    timestamp: i64,
    metadata: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .await?;

    let topic = "/default/user_events";
    let consumer_name = "user_events_consumer";
    let subscription_name = "user_events_subscription";

    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name(consumer_name)
        .with_subscription(subscription_name)
        .with_subscription_type(SubType::Exclusive)
        .build();

    // Subscribe to the topic
    consumer.subscribe().await?;
    println!("✅ Consumer {} subscribed to {}", consumer_name, topic);

    // Start receiving Avro-encoded messages with automatic deserialization
    let mut message_stream = consumer.receive_typed::<UserEvent>().await?;

    println!("🎧 Listening for user events (Avro schema)...\n");

    while let Some(result) = message_stream.recv().await {
        match result {
            Ok((message, decoded_event)) => {
                // Message is automatically deserialized from Avro
                println!("📥 Received User Event:");
                println!("   User ID: {}", decoded_event.user_id);
                println!("   Action: {}", decoded_event.action);
                println!("   Timestamp: {}", decoded_event.timestamp);
                if let Some(ref metadata) = decoded_event.metadata {
                    println!("   Metadata: {}", metadata);
                }
                println!("   Message ID: {:?}", message.msg_id);
                println!();

                // Acknowledge the message
                if let Err(e) = consumer.ack(&message).await {
                    eprintln!("❌ Failed to acknowledge message: {}", e);
                }
            }
            Err(e) => {
                eprintln!("❌ Failed to deserialize Avro message: {}", e);
            }
        }
    }

    Ok(())
}
