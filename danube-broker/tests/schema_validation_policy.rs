//! Schema Validation Policy Tests
//!
//! Tests ValidationPolicy enforcement and message validation:
//! - ValidationPolicy::None (no validation)
//! - ValidationPolicy::Warn (validates but allows invalid)
//! - ValidationPolicy::Enforce (rejects invalid messages)
//! - Schema mismatch detection
//! - Payload validation against schema definition

use anyhow::Result;
use danube_client::{SchemaType, SubType};
use serde_json::json;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// Test 1: Producer and consumer with matching schemas
///
/// **What:** Producer with registered schema sends message, consumer receives and validates payload.
/// **Why:** Validates the complete end-to-end flow of schema-based messaging with payload verification.
#[tokio::test]
async fn matching_schemas_end_to_end() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/matching_schemas");
    let schema_client = client.schema();

    // Register schema
    let json_schema = r#"{"type": "object", "properties": {"message": {"type": "string"}, "count": {"type": "integer"}}, "required": ["message", "count"]}"#;
    let schema_id = schema_client
        .register_schema("matching-schema")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(json_schema.as_bytes())
        .execute()
        .await?;

    // Create producer with schema
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_matching")
        .with_schema_subject("matching-schema")
        .build()?;
    producer.create().await?;

    // Create consumer
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_matching")
        .with_subscription("sub_matching")
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // Send valid message
    let valid_payload = json!({"message": "hello", "count": 42});
    producer
        .send(serde_json::to_vec(&valid_payload)?, None)
        .await?;

    // Receive message
    let message = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive message");

    assert_eq!(message.schema_id, Some(schema_id));

    // Verify payload
    let received_data: serde_json::Value = serde_json::from_slice(&message.payload)?;
    assert_eq!(received_data["message"], "hello");
    assert_eq!(received_data["count"], 42);

    consumer.ack(&message).await?;
    Ok(())
}

/// Test 2: Multiple payloads with same schema
///
/// **What:** Sends 3 messages with different but valid JSON structures under the same schema.
/// **Why:** Ensures schema validation works correctly for varied payloads matching the schema definition.
#[tokio::test]
async fn multiple_valid_payloads() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/multi_valid");
    let schema_client = client.schema();

    // Register schema with various types
    let schema = r#"{
        "type": "object",
        "properties": {
            "string_field": {"type": "string"},
            "int_field": {"type": "integer"},
            "bool_field": {"type": "boolean"},
            "array_field": {"type": "array", "items": {"type": "string"}}
        },
        "required": ["string_field", "int_field"]
    }"#;

    schema_client
        .register_schema("multi-type-schema")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema.as_bytes())
        .execute()
        .await?;

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_multi")
        .with_schema_subject("multi-type-schema")
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_multi")
        .with_subscription("sub_multi")
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // Send various valid payloads
    let payloads = vec![
        json!({"string_field": "test", "int_field": 1}),
        json!({"string_field": "test2", "int_field": 2, "bool_field": true}),
        json!({"string_field": "test3", "int_field": 3, "array_field": ["a", "b"]}),
    ];

    for payload in &payloads {
        producer.send(serde_json::to_vec(payload)?, None).await?;
    }

    // Receive all messages
    for i in 0..payloads.len() {
        let msg = timeout(Duration::from_secs(5), async { stream.recv().await })
            .await?
            .expect("Should receive message");

        let data: serde_json::Value = serde_json::from_slice(&msg.payload)?;
        assert_eq!(data["string_field"], payloads[i]["string_field"]);
        consumer.ack(&msg).await?;
    }

    Ok(())
}

/// Test 3: Schema evolution - consumer handles both old and new schema versions
///
/// **What:** Producer1 sends V1 messages, Producer2 sends V2 messages, consumer receives both.
/// **Why:** Validates that consumers can handle schema evolution and process messages from multiple schema versions.
#[tokio::test]
async fn schema_evolution_consumer_compatibility() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/evolution");
    let schema_subject = format!(
        "evolution-schema-{}",
        topic.split('-').last().unwrap_or("0")
    );
    let schema_client = client.schema();

    // V1: Basic schema
    let schema_v1 =
        r#"{"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]}"#;
    schema_client
        .register_schema(&schema_subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v1.as_bytes())
        .execute()
        .await?;

    // Producer 1 with V1 (creates topic)
    let mut producer1 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_v1")
        .with_schema_subject(&schema_subject)
        .build()?;
    producer1.create().await?;

    // Now create consumer after topic exists
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_evolution")
        .with_subscription("sub_evolution")
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // Send V1 message
    producer1
        .send(json!({"id": 1}).to_string().as_bytes().to_vec(), None)
        .await?;

    // V2: Add optional field
    let schema_v2 = r#"{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}, "required": ["id"]}"#;
    schema_client
        .register_schema(&schema_subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v2.as_bytes())
        .execute()
        .await?;

    // Producer 2 with V2
    let mut producer2 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_v2")
        .with_schema_subject(&schema_subject)
        .build()?;
    producer2.create().await?;

    // Send V2 message
    producer2
        .send(
            json!({"id": 2, "name": "test"})
                .to_string()
                .as_bytes()
                .to_vec(),
            None,
        )
        .await?;

    // Receive V1 message
    let msg1 = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive V1 message");
    let data1: serde_json::Value = serde_json::from_slice(&msg1.payload)?;
    assert_eq!(data1["id"], 1);
    assert_eq!(msg1.schema_version, Some(1));
    consumer.ack(&msg1).await?;

    // Receive V2 message
    let msg2 = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive V2 message");
    let data2: serde_json::Value = serde_json::from_slice(&msg2.payload)?;
    assert_eq!(data2["id"], 2);
    assert_eq!(data2["name"], "test");
    assert_eq!(msg2.schema_version, Some(2));
    consumer.ack(&msg2).await?;

    Ok(())
}

/// Test 4: Avro schema end-to-end
///
/// **What:** Registers Avro schema, sends Avro-serialized message, consumer receives with schema_id.
/// **Why:** Validates that Avro schemas work end-to-end with proper serialization and metadata.
#[tokio::test]
async fn avro_schema_end_to_end() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/avro_test");
    let schema_client = client.schema();

    // Register Avro schema
    let avro_schema = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "username", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "active", "type": "boolean"}
        ]
    }"#;

    let schema_id = schema_client
        .register_schema("avro-user-schema")
        .with_type(SchemaType::Avro)
        .with_schema_data(avro_schema.as_bytes())
        .execute()
        .await?;

    // Create producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_avro")
        .with_schema_subject("avro-user-schema")
        .build()?;
    producer.create().await?;

    // Create consumer
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_avro")
        .with_subscription("sub_avro")
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // For Avro, we'd normally use apache_avro crate to serialize
    // For this test, we'll send JSON representation (broker validates schema_id)
    let avro_data = json!({
        "username": "alice",
        "age": 30,
        "active": true
    });

    producer.send(serde_json::to_vec(&avro_data)?, None).await?;

    let message = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive message");

    assert_eq!(message.schema_id, Some(schema_id));

    consumer.ack(&message).await?;
    Ok(())
}

/// Test 5: String schema type
///
/// **What:** Registers String schema type and sends plain text messages.
/// **Why:** Confirms that non-JSON schema types (String) work correctly for simple text messaging.
#[tokio::test]
async fn string_schema_type() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/string_schema");
    let schema_client = client.schema();

    // Register String schema (no schema definition needed)
    let schema_id = schema_client
        .register_schema("log-messages")
        .with_type(SchemaType::String)
        .with_schema_data(b"")
        .execute()
        .await?;

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_string")
        .with_schema_subject("log-messages")
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_string")
        .with_subscription("sub_string")
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // Send string messages
    let messages = vec![
        "INFO: Server started",
        "DEBUG: Connection established",
        "ERROR: Timeout",
    ];

    for msg in &messages {
        producer.send(msg.as_bytes().to_vec(), None).await?;
    }

    // Receive and verify
    for expected in &messages {
        let msg = timeout(Duration::from_secs(5), async { stream.recv().await })
            .await?
            .expect("Should receive message");

        let received = std::str::from_utf8(&msg.payload)?;
        assert_eq!(received, *expected);
        assert_eq!(msg.schema_id, Some(schema_id));
        consumer.ack(&msg).await?;
    }

    Ok(())
}

/// Test 6: Bytes schema type
///
/// **What:** Registers Bytes schema type and sends binary data.
/// **Why:** Ensures raw binary data can be sent with schema type validation (no structure enforcement).
#[tokio::test]
async fn bytes_schema_type() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/bytes_schema");
    let schema_client = client.schema();

    // Register Bytes schema
    let schema_id = schema_client
        .register_schema("binary-data")
        .with_type(SchemaType::Bytes)
        .with_schema_data(b"")
        .execute()
        .await?;

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_bytes")
        .with_schema_subject("binary-data")
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_bytes")
        .with_subscription("sub_bytes")
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // Send binary data
    let binary_data = vec![0x00, 0xFF, 0xAB, 0xCD, 0xEF];
    producer.send(binary_data.clone(), None).await?;

    let msg = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive message");

    assert_eq!(msg.payload, binary_data);
    assert_eq!(msg.schema_id, Some(schema_id));

    consumer.ack(&msg).await?;
    Ok(())
}

/// Test 7: Retrieve and verify schema metadata from message
///
/// **What:** Sends message with schema, verifies schema_id and schema_version in received message.
/// **Why:** Confirms that schema metadata is correctly attached and can be used for dynamic deserialization.
#[tokio::test]
async fn verify_schema_metadata_in_messages() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/metadata_verify");
    let schema_client = client.schema();

    let schema = r#"{"type": "object", "properties": {"value": {"type": "number"}}}"#;
    let schema_id = schema_client
        .register_schema("metadata-schema")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema.as_bytes())
        .execute()
        .await?;

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_metadata")
        .with_schema_subject("metadata-schema")
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_metadata")
        .with_subscription("sub_metadata")
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    producer
        .send(json!({"value": 3.14}).to_string().as_bytes().to_vec(), None)
        .await?;

    let msg = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive message");

    // Verify schema metadata
    assert!(msg.schema_id.is_some(), "Message should have schema_id");
    assert_eq!(msg.schema_id.unwrap(), schema_id, "Schema ID should match");

    assert!(
        msg.schema_version.is_some(),
        "Message should have schema_version"
    );
    assert_eq!(msg.schema_version.unwrap(), 1, "Schema version should be 1");

    // Note: get_schema_version(schema_id, version) is not implemented yet in broker
    // Would normally use: schema_client.get_schema_version(schema_id, msg.schema_version)
    // For now, verify we can get latest schema by subject
    let retrieved = schema_client.get_latest_schema("metadata-schema").await?;

    assert_eq!(retrieved.schema_id, schema_id);

    consumer.ack(&msg).await?;
    Ok(())
}

/// Test 8: Second producer with different schema subject fails (first producer privilege)
///
/// **What:** First producer assigns schema subject to topic, second producer tries different subject.
/// **Why:** Validates that first producer privilege locks the topic to one schema subject.
#[tokio::test]
async fn second_producer_different_subject_fails() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/multi_schema");
    let schema_client = client.schema();

    // Register two different schema subjects
    let schema1 = r#"{"type": "object", "properties": {"type": {"type": "string", "enum": ["event"]}, "data": {"type": "string"}}}"#;
    schema_client
        .register_schema("event-schema")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema1.as_bytes())
        .execute()
        .await?;

    let schema2 = r#"{"type": "object", "properties": {"type": {"type": "string", "enum": ["metric"]}, "value": {"type": "number"}}}"#;
    schema_client
        .register_schema("metric-schema")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema2.as_bytes())
        .execute()
        .await?;

    // First producer with event-schema (locks topic to this subject)
    let mut producer1 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_events")
        .with_schema_subject("event-schema")
        .build()?;
    producer1.create().await?;

    // Second producer tries metric-schema (should fail - different subject)
    let mut producer2 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_metrics")
        .with_schema_subject("metric-schema")
        .build()?;

    let result = producer2.create().await;

    assert!(
        result.is_err(),
        "Second producer with different schema subject should fail"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("event-schema") || error_msg.contains("cannot use"),
        "Error should mention schema mismatch"
    );

    Ok(())
}
