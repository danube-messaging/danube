use anyhow::Result;
use danube_client::{DanubeClient, SubType};

/// Key-Shared consumer with key filtering — only receives messages matching
/// specific routing key patterns.
///
/// Key filters use glob syntax:
///   - "payment"       → exact match
///   - "ship*"         → matches "shipping", "shipment", etc.
///   - "eu-west-?"     → matches "eu-west-1", "eu-west-2", etc.
///   - "*"             → matches everything (default if no filter set)
///
/// This example shows how to set up specialized consumers that each handle
/// a subset of event types, giving you explicit control over key routing
/// instead of relying on consistent hashing.
///
/// Usage — run all three terminals:
///
///   # Terminal 1 — payments consumer (handles payment + invoice keys)
///   cargo run --example key_shared_filtered_consumer -- payments
///
///   # Terminal 2 — logistics consumer (handles shipping + return keys)
///   cargo run --example key_shared_filtered_consumer -- logistics
///
///   # Terminal 3 — start the producer
///   cargo run --example key_shared_producer
///
/// Expected behavior:
///   "payments"  consumer receives: all "payment" and "invoice" messages
///   "logistics" consumer receives: all "shipping" and "return" messages
///   Messages with keys not matching any filter (if any) are round-robin'd
///   among consumers whose filter includes "*" or handled by hash fallback.
#[tokio::main]
async fn main() -> Result<()> {
    let role = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "payments".to_string());

    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .await?;

    let topic = "/default/orders_topic";
    let subscription = "orders_filtered";

    // Set up key filters based on the consumer's role.
    // Each consumer only receives messages whose routing key matches its filters.
    let (consumer_name, filters) = match role.as_str() {
        "payments" => (
            "consumer_payments",
            vec!["payment".to_string(), "invoice".to_string()],
        ),
        "logistics" => (
            "consumer_logistics",
            vec!["ship*".to_string(), "return".to_string()],
        ),
        other => {
            eprintln!("Unknown role '{}'. Use 'payments' or 'logistics'.", other);
            std::process::exit(1);
        }
    };

    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name(consumer_name)
        .with_subscription(subscription)
        .with_subscription_type(SubType::KeyShared)
        .with_key_filters(filters.clone())
        .build()?;

    consumer.subscribe().await?;
    println!(
        "✅ Consumer '{}' subscribed to '{}' (Key-Shared, filters: {:?})",
        consumer_name, topic, filters
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
