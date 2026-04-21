use anyhow::Result;
use danube_client::{DanubeClient, SubType};
use std::env;

/// Key-Shared consumer — receives messages based on routing key affinity.
///
/// In a Key-Shared subscription, each routing key is assigned to exactly one
/// consumer via consistent hashing. This guarantees:
/// - All messages with the same key go to the same consumer
/// - Messages for a given key are delivered in order
/// - Load is distributed across consumers by key
///
/// Start multiple instances of this consumer to see key distribution in action.
/// Each instance registers with the same subscription but receives different keys.
///
/// Usage:
///   # Terminal 1
///   cargo run --example key_shared_consumer -- consumer_1
///
///   # Terminal 2
///   cargo run --example key_shared_consumer -- consumer_2
///
///   # Terminal 3 — start the producer
///   cargo run --example key_shared_producer
///
/// Expected behavior:
///   consumer_1 might receive: all "payment" and "invoice" messages
///   consumer_2 might receive: all "shipping" and "return" messages
///   (exact distribution depends on consistent hashing of key names)
#[tokio::main]
async fn main() -> Result<()> {
    // Allow naming the consumer via command line for multi-instance demos
    let consumer_name = env::args()
        .nth(1)
        .unwrap_or_else(|| "ks_consumer".to_string());

    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .await?;

    let topic = "/default/orders_topic";
    let subscription = "orders_key_shared";

    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name(&consumer_name)
        .with_subscription(subscription)
        .with_subscription_type(SubType::KeyShared)
        .build()?;

    consumer.subscribe().await?;
    println!(
        "✅ Consumer '{}' subscribed to '{}' (Key-Shared)",
        consumer_name, topic
    );
    println!("🎧 Listening for messages...\n");

    let mut stream = consumer.receive().await?;
    let mut count = 0;

    while let Some(message) = stream.recv().await {
        let payload = String::from_utf8_lossy(&message.payload);
        let key = message.routing_key.as_deref().unwrap_or("<none>");

        println!(
            "📥 [{}] key={:<10} | offset={} | '{}'",
            consumer_name, key, message.msg_id.topic_offset, payload
        );

        consumer.ack(&message).await?;
        count += 1;
    }

    println!(
        "\n✅ Consumer '{}' received {} messages total",
        consumer_name, count
    );
    Ok(())
}
