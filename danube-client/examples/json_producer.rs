use anyhow::Result;
use danube_client::{DanubeClient, SchemaRegistryClient, SchemaType};
use serde::Serialize;
use std::thread;
use std::time::Duration;

// Define the message structure
#[derive(Serialize)]
struct MyMessage {
    field1: String,
    field2: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .await?;

    let topic = "/default/test_topic";
    let producer_name = "prod_json";

    // Phase 5: Register schema in schema registry
    let json_schema = r#"{"type": "object", "properties": {"field1": {"type": "string"}, "field2": {"type": "integer"}}}"#;

    let mut schema_client = SchemaRegistryClient::new(&client).await?;
    let schema_id = schema_client
        .register_schema("my-app-events")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(json_schema.as_bytes())
        .execute()
        .await?;

    println!("Registered schema with ID: {}", schema_id);

    // Phase 5: Create producer with schema reference
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema_subject("my-app-events")
        .build();

    producer.create().await?;
    println!("The Producer {} was created", producer_name);

    let mut i = 0;

    while i < 100 {
        let message = MyMessage {
            field1: format!("value{}", i),
            field2: 2020 + i,
        };

        // Serialize to JSON bytes
        let json_bytes = serde_json::to_vec(&message)?;

        let message_id = producer.send(json_bytes, None).await?;
        println!("The Message with id {} was sent", message_id);

        thread::sleep(Duration::from_secs(1));
        i += 1;
    }

    Ok(())
}
