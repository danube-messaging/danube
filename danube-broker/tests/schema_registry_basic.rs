//! Schema Registry Basic Tests
//!
//! Tests fundamental schema registry operations:
//! - Schema registration (different types)
//! - Producer schema enforcement (registered vs unregistered)
//! - Consumer schema validation
//! - Schema retrieval and versioning

use anyhow::Result;
use danube_client::{SchemaType, SubType};
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// Test 1: Register different schema types and verify they can be retrieved
///
/// **What:** Registers multiple schemas of different types (JSON, Avro, String, Bytes) and retrieves them.
/// **Why:** Validates that the schema registry supports various schema types and can store and return them correctly.
#[tokio::test]
async fn schema_registration_multiple_types() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let schema_client = client.schema();

    // JSON Schema
    let json_schema = r#"{"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}}"#;
    let json_id = schema_client
        .register_schema("user-events-json")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(json_schema.as_bytes())
        .execute()
        .await?;

    // Avro Schema
    let avro_schema = r#"{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}]}"#;
    let avro_id = schema_client
        .register_schema("user-events-avro")
        .with_type(SchemaType::Avro)
        .with_schema_data(avro_schema.as_bytes())
        .execute()
        .await?;

    // String Schema
    let _string_id = schema_client
        .register_schema("log-events-string")
        .with_type(SchemaType::String)
        .with_schema_data(b"")
        .execute()
        .await?;

    // Bytes Schema
    let _bytes_id = schema_client
        .register_schema("binary-events")
        .with_type(SchemaType::Bytes)
        .with_schema_data(b"")
        .execute()
        .await?;

    // Verify retrieval
    let retrieved_json = schema_client.get_latest_schema("user-events-json").await?;
    assert_eq!(retrieved_json.schema_id, json_id);
    assert_eq!(retrieved_json.version, 1);

    let retrieved_avro = schema_client.get_latest_schema("user-events-avro").await?;
    assert_eq!(retrieved_avro.schema_id, avro_id);

    Ok(())
}

/// Test 2: Producer with registered schema succeeds
///
/// **What:** Creates a producer with a registered schema subject and sends a message.
/// **Why:** Validates that producers can successfully use registered schemas for type-safe messaging.
#[tokio::test]
async fn producer_with_registered_schema_succeeds() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/schema_registered");
    let schema_client = client.schema();

    // Register schema FIRST
    let json_schema = r#"{"type": "object", "properties": {"msg": {"type": "string"}}}"#;
    let _schema_id = schema_client
        .register_schema("test-registered-schema")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(json_schema.as_bytes())
        .execute()
        .await?;

    // Create producer with schema reference - should succeed
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_registered")
        .with_schema_subject("test-registered-schema")
        .build()?;

    let result = producer.create().await;
    assert!(
        result.is_ok(),
        "Producer creation should succeed with registered schema"
    );

    // Send a message to verify it works
    let payload = r#"{"msg": "hello"}"#;
    let send_result = producer.send(payload.as_bytes().to_vec(), None).await;
    assert!(send_result.is_ok(), "Message send should succeed");

    Ok(())
}

/// Test 3: Producer with UNREGISTERED schema fails (enforcement)
///
/// **What:** Creates a producer with an unregistered schema subject and attempts to send a message.
/// **Why:** Validates that schema enforcement prevents producers from sending messages with unregistered schemas.
#[tokio::test]
async fn producer_with_unregistered_schema_fails() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/schema_unregistered");

    // Create producer with schema reference that does NOT exist
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_unregistered")
        .with_schema_subject("non-existent-schema")
        .build()?;

    // This should FAIL because schema is not registered
    let result = producer.create().await;
    assert!(
        result.is_err(),
        "Producer creation should fail with unregistered schema"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("not found") || error_msg.contains("validation failed"),
        "Error should mention schema not found"
    );

    Ok(())
}

/// Test 4: Producer without schema works (implicit Bytes schema)
///
/// **What:** Creates a producer without specifying a schema and sends arbitrary bytes.
/// **Why:** Confirms that schema enforcement is opt-in and producers can send raw bytes without schemas.
#[tokio::test]
async fn producer_without_schema_works() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/no_schema");

    // Create producer WITHOUT specifying schema
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_no_schema")
        .build()?;

    let result = producer.create().await;
    assert!(
        result.is_ok(),
        "Producer creation should succeed without schema"
    );

    // Send arbitrary bytes
    let send_result = producer.send(b"any bytes work".to_vec(), None).await;
    assert!(send_result.is_ok(), "Message send should succeed");

    Ok(())
}

/// Test 5: Schema versioning - register, update, verify versions
///
/// **What:** Registers V1 of a schema, then V2, and retrieves the latest version.
/// **Why:** Validates that the registry supports schema evolution and version tracking for the same subject.
#[tokio::test]
async fn schema_versioning_evolution() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let schema_client = client.schema();

    let subject = "versioned-schema-test";

    // Version 1: Initial schema
    let schema_v1 = r#"{"type": "object", "properties": {"name": {"type": "string"}}}"#;
    let id_v1 = schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v1.as_bytes())
        .execute()
        .await?;

    // Version 2: Add optional field (backward compatible)
    let schema_v2 = r#"{"type": "object", "properties": {"name": {"type": "string"}, "email": {"type": "string"}}}"#;
    let id_v2 = schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v2.as_bytes())
        .execute()
        .await?;

    // Allow Raft write to propagate
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // In this schema registry, versions of same subject share the schema_id
    // Versions are tracked separately
    assert_eq!(id_v1, id_v2, "Same subject should have same schema_id");

    // Retrieve latest (should be v2)
    let latest = schema_client.get_latest_schema(subject).await?;
    assert_eq!(latest.schema_id, id_v1); // Same ID for subject
    assert_eq!(latest.version, 2, "Latest version should be 2");

    // Note: get_schema_version(schema_id, version) is not implemented yet
    // The broker requires using get_latest_schema with subject name instead

    Ok(())
}

/// Test 6: Consumer receives messages with schema metadata
///
/// **What:** Producer sends message and consumer verifies schema_id and schema_version are present.
/// **Why:** Ensures schema metadata is attached to messages for downstream validation and deserialization.
#[tokio::test]
async fn consumer_receives_schema_metadata() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/schema_metadata");
    let schema_client = client.schema();

    // Register schema
    let json_schema = r#"{"type": "object", "properties": {"data": {"type": "string"}}}"#;
    let schema_id = schema_client
        .register_schema("metadata-test-schema")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(json_schema.as_bytes())
        .execute()
        .await?;

    // Create producer with schema
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_metadata")
        .with_schema_subject("metadata-test-schema")
        .build()?;
    producer.create().await?;

    // Create consumer
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_metadata")
        .with_subscription("sub_metadata")
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // Send message
    let payload = r#"{"data": "test"}"#;
    producer.send(payload.as_bytes().to_vec(), None).await?;

    // Receive and verify schema metadata
    let message = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive message");

    // Verify schema metadata is present
    if let Some(msg_schema_id) = message.schema_id {
        assert_eq!(msg_schema_id, schema_id, "Schema ID should match");
    } else {
        panic!("Message should have schema_id");
    }

    if let Some(version) = message.schema_version {
        assert_eq!(version, 1, "Schema version should be 1");
    } else {
        panic!("Message should have schema_version");
    }

    consumer.ack(&message).await?;
    Ok(())
}

/// Test 7: Multiple producers same schema subject
///
/// **What:** Creates two producers both using the same registered schema and sends messages.
/// **Why:** Validates that multiple producers can share the same schema for consistent data contracts.
#[tokio::test]
async fn multiple_producers_same_schema() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/multi_prod_schema");
    let schema_client = client.schema();

    // Register shared schema
    let schema = r#"{"type": "object", "properties": {"value": {"type": "integer"}}}"#;
    let _schema_id = schema_client
        .register_schema("shared-schema")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema.as_bytes())
        .execute()
        .await?;

    // Create two producers with same schema
    let mut producer1 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer1_shared")
        .with_schema_subject("shared-schema")
        .build()?;
    producer1.create().await?;

    let mut producer2 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer2_shared")
        .with_schema_subject("shared-schema")
        .build()?;
    producer2.create().await?;

    // Both send messages
    producer1
        .send(r#"{"value": 1}"#.as_bytes().to_vec(), None)
        .await?;
    producer2
        .send(r#"{"value": 2}"#.as_bytes().to_vec(), None)
        .await?;

    Ok(())
}
