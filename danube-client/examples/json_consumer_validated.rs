use anyhow::Result;
use danube_client::{DanubeClient, SchemaRegistryClient, SubType};
use jsonschema::Validator;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
struct MyMessage {
    field1: String,
    field2: i32,
}

/// Validates that a Rust struct matches the JSON Schema from the registry
///
/// This ensures that the consumer's struct definition matches the schema
/// registered by the producer. If validation fails, the consumer won't start,
/// preventing runtime deserialization errors.
///
/// # Arguments
/// * `schema_client` - Schema registry client
/// * `subject` - Schema subject name (e.g., "my-app-events")
/// * `sample` - Sample instance of the struct to validate
///
/// # Returns
/// Schema version number if validation succeeds
async fn validate_struct_against_registry<T: Serialize>(
    schema_client: &mut SchemaRegistryClient,
    subject: &str,
    sample: &T,
) -> Result<u32> {
    println!("🔍 Fetching schema from registry for subject: {}", subject);

    let schema_response = schema_client.get_latest_schema(subject).await?;

    println!("📋 Retrieved schema version: {}", schema_response.version);
    println!("📋 Schema type: {}", schema_response.schema_type);

    let schema_def: serde_json::Value = serde_json::from_slice(&schema_response.schema_definition)?;

    println!("🔨 Compiling JSON Schema validator...");
    let validator =
        Validator::new(&schema_def).map_err(|e| anyhow::anyhow!("Invalid JSON schema: {}", e))?;

    println!("✅ Validating struct against schema...");
    let sample_json = serde_json::to_value(sample)?;

    if let Err(_validation_error) = validator.validate(&sample_json) {
        eprintln!(
            "\n❌ VALIDATION FAILED: Struct does not match schema v{}",
            schema_response.version
        );
        eprintln!("   The consumer struct definition is incompatible with the registered schema.");
        eprintln!("\n   Validation errors:");
        for error in validator.iter_errors(&sample_json) {
            eprintln!("   - {}", error);
        }
        eprintln!("\n   💡 Fix: Update the MyMessage struct to match the schema in the registry.");
        return Err(anyhow::anyhow!(
            "Struct validation failed - consumer cannot start"
        ));
    }

    println!(
        "✅ Struct validated successfully against schema v{}",
        schema_response.version
    );
    Ok(schema_response.version)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Starting validated JSON consumer example\n");

    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .await?;

    let topic = "/default/test_topic";
    let consumer_name = "cons_json_validated";
    let subscription_name = "subs_json_validated";

    // Step 1: Validate struct against registry schema
    println!("📝 Step 1: Validating consumer struct against schema registry\n");

    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    // ACTUAL VALIDATION - This will fail at startup if struct doesn't match!
    let schema_version = validate_struct_against_registry(
        &mut schema_client,
        "my-app-events",
        &MyMessage {
            field1: "validation_test".to_string(),
            field2: 0,
        },
    )
    .await?;

    println!(
        "\n✅ Consumer validated against schema version: {}",
        schema_version
    );
    println!("   Safe to proceed with typed deserialization\n");

    // Step 2: Create and subscribe consumer
    println!("📝 Step 2: Creating consumer and subscribing to topic\n");

    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name(consumer_name)
        .with_subscription(subscription_name)
        .with_subscription_type(SubType::Exclusive)
        .build()?;

    consumer.subscribe().await?;
    println!("✅ Consumer {} subscribed to {}", consumer_name, topic);

    // Step 3: Receive and deserialize messages
    println!("\n📝 Step 3: Listening for messages...\n");

    let mut message_stream = consumer.receive().await?;

    while let Some(message) = message_stream.recv().await {
        // Deserialize using validated struct
        match serde_json::from_slice::<MyMessage>(&message.payload) {
            Ok(decoded_message) => {
                println!("📥 Received valid message:");
                println!("   field1: {}", decoded_message.field1);
                println!("   field2: {}", decoded_message.field2);
                println!("   Message ID: {:?}", message.msg_id);
                println!();

                // Acknowledge the message
                if let Err(e) = consumer.ack(&message).await {
                    eprintln!("❌ Failed to acknowledge message: {}", e);
                }
            }
            Err(e) => {
                eprintln!("❌ Deserialization failed: {}", e);
                eprintln!(
                    "   This might indicate schema drift - check schema version {}",
                    schema_version
                );
                eprintln!("   Message will NOT be acknowledged (will retry or go to DLQ)");
            }
        }
    }

    Ok(())
}
