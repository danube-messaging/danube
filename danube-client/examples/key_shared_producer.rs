use anyhow::Result;
use danube_client::DanubeClient;
use std::time::Duration;
use tokio::time::sleep;

/// Key-Shared producer — sends messages with routing keys.
///
/// Each message is tagged with a routing key that determines which consumer
/// receives it. All messages with the same key are guaranteed to go to the
/// same consumer within a Key-Shared subscription.
///
/// This example simulates an e-commerce system where different order events
/// (payments, shipping, returns) are published with the event type as the
/// routing key.
///
/// Run this alongside `key_shared_consumer.rs` to see key-based distribution.
#[tokio::main]
async fn main() -> Result<()> {
    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .await?;

    let topic = "/default/orders_topic";
    let producer_name = "orders_producer";

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_reliable_dispatch()
        .build()?;

    producer.create().await?;
    println!("✅ Producer '{}' created on topic '{}'", producer_name, topic);
    println!();

    // Simulate order events with different routing keys.
    // In a Key-Shared subscription, all "payment" events go to the same
    // consumer, all "shipping" events go to the same consumer, etc.
    let events = [
        ("payment",  "Payment received for order #1001"),
        ("shipping", "Order #1001 shipped via express"),
        ("payment",  "Payment received for order #1002"),
        ("return",   "Return request for order #998"),
        ("shipping", "Order #1002 shipped via standard"),
        ("payment",  "Payment received for order #1003"),
        ("invoice",  "Invoice generated for order #1001"),
        ("return",   "Return approved for order #998"),
        ("shipping", "Order #1003 shipped via express"),
        ("invoice",  "Invoice generated for order #1002"),
    ];

    for (key, message) in &events {
        let msg_id = producer
            .send_with_key(message.as_bytes().to_vec(), None, key)
            .await?;

        println!("📤 Sent [key={:<10}]: '{}' (ID: {})", key, message, msg_id);
        sleep(Duration::from_millis(300)).await;
    }

    println!();
    println!("✅ All {} events sent!", events.len());
    println!();
    println!("💡 Tip: Start 2+ consumers with `key_shared_consumer` to see");
    println!("   how messages are distributed by routing key.");

    Ok(())
}
