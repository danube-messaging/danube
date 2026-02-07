use anyhow::Result;
use danube_client::{DanubeClient, SchemaRegistryClient, SchemaType};
use serde::Serialize;
use std::thread;
use std::time::Duration;

// Define the message structure matching the Avro schema
#[derive(Serialize, Debug)]
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
    let producer_name = "user_events_producer";

    // Register Avro schema
    let avro_schema = r#"
    {
        "type": "record",
        "name": "UserEvent",
        "namespace": "com.example.events",
        "fields": [
            {"name": "user_id", "type": "string"},
            {"name": "action", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "metadata", "type": ["null", "string"], "default": null}
        ]
    }
    "#;

    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    // Register the schema and get schema ID
    let schema_id = schema_client
        .register_schema("user-events")
        .with_type(SchemaType::Avro)
        .with_schema_data(avro_schema.as_bytes())
        .execute()
        .await?;

    println!("✅ Registered Avro schema with ID: {}", schema_id);

    // Create producer with schema reference
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema_subject("user-events")
        .build()?;

    producer.create().await?;
    println!(
        "✅ Producer {} created with schema validation",
        producer_name
    );

    // Send messages with Avro schema
    let actions = ["login", "purchase", "logout", "view_product", "add_to_cart"];

    for i in 0..100 {
        let event = UserEvent {
            user_id: format!("user_{}", 1000 + i),
            action: actions[i % actions.len()].to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            metadata: if i % 2 == 0 {
                Some(format!("session_id_{}", i))
            } else {
                None
            },
        };

        // Serialize to Avro format
        let avro_data = serde_json::to_vec(&event)?;

        match producer.send(avro_data, None).await {
            Ok(message_id) => {
                println!(
                    "📤 Sent event: {:?} with message ID: {}",
                    event.action, message_id
                );
            }
            Err(e) => {
                eprintln!("❌ Failed to send message: {}", e);
            }
        }

        thread::sleep(Duration::from_millis(500));
    }

    println!("✅ Sent 10 user events with Avro schema");
    Ok(())
}
