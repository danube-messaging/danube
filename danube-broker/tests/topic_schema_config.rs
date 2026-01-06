//! Topic Schema Configuration Tests
//!
//! Tests topic-level schema configuration and validation policies:
//! - Configure topic with schema subject
//! - Set validation policy (None, Warn, Enforce)
//! - Enable/disable payload validation
//! - First producer privilege
//! - Admin override

use anyhow::Result;
use danube_client::{SchemaRegistryClient, SchemaType};

#[path = "test_utils.rs"]
mod test_utils;

/// Test 1: First producer privilege - assigns schema to topic
///
/// **What:** First producer with schema subject automatically configures the topic.
/// **Why:** Validates that the first producer privilege works when no admin configuration exists.
#[tokio::test]
async fn first_producer_assigns_schema() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/first_producer");
    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    // Register schema
    let schema = r#"{"type": "object", "properties": {"msg": {"type": "string"}}}"#;
    schema_client
        .register_schema("first-schema")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema.as_bytes())
        .execute()
        .await?;

    // First producer - should assign schema to topic
    let mut producer1 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("first_producer")
        .with_schema_subject("first-schema")
        .build();
    producer1.create().await?;

    // Second producer with same schema - should succeed
    let mut producer2 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("second_producer")
        .with_schema_subject("first-schema")
        .build();

    let result = producer2.create().await;
    assert!(
        result.is_ok(),
        "Second producer with same schema should succeed"
    );

    Ok(())
}

/// Test 2: Second producer with different schema fails
///
/// **What:** First producer assigns schema, second producer tries different schema.
/// **Why:** Validates that topic schema is enforced after first producer assignment.
#[tokio::test]
async fn second_producer_different_schema_fails() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/schema_mismatch");
    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    // Register two different schemas
    let schema1 = r#"{"type": "object", "properties": {"a": {"type": "string"}}}"#;
    schema_client
        .register_schema("schema-a")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema1.as_bytes())
        .execute()
        .await?;

    let schema2 = r#"{"type": "object", "properties": {"b": {"type": "integer"}}}"#;
    schema_client
        .register_schema("schema-b")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema2.as_bytes())
        .execute()
        .await?;

    // First producer with schema-a
    let mut producer1 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_schema_a")
        .with_schema_subject("schema-a")
        .build();
    producer1.create().await?;

    // Second producer tries to use schema-b (should fail)
    let mut producer2 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_schema_b")
        .with_schema_subject("schema-b")
        .build();

    let result = producer2.create().await;
    assert!(
        result.is_err(),
        "Second producer with different schema should fail"
    );

    Ok(())
}

/// Test 3: Producer without schema can connect but messages may be rejected
///
/// **What:** Topic has schema configured, producer connects without schema.
/// **Why:** Validates that producers without schema are allowed, but schema validation
/// is applied at message level (based on validation policy).
#[tokio::test]
async fn producer_without_schema_can_connect() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/no_schema_allowed");
    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    // Register and assign schema via first producer
    let schema = r#"{"type": "object", "properties": {"x": {"type": "number"}}}"#;
    schema_client
        .register_schema("optional-schema")
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema.as_bytes())
        .execute()
        .await?;

    let mut producer1 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("first_with_schema")
        .with_schema_subject("optional-schema")
        .build();
    producer1.create().await?;

    // Second producer WITHOUT schema (should succeed - schema is optional for producers)
    let mut producer2 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("second_no_schema")
        .build();

    let result = producer2.create().await;
    assert!(
        result.is_ok(),
        "Producer without schema should be allowed to connect. Schema validation happens at message level."
    );

    Ok(())
}

/// Test 4: Multiple schema versions on same topic
///
/// **What:** Topic configured with subject, producers use different versions of that subject.
/// **Why:** Validates that version evolution works within the same schema subject.
#[tokio::test]
async fn topic_allows_different_versions_same_subject() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/version_evolution");
    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    let subject = "evolving-schema";

    // Register V1
    let schema_v1 = r#"{"type": "object", "properties": {"id": {"type": "integer"}}}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v1.as_bytes())
        .execute()
        .await?;

    // First producer with V1
    let mut producer1 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_v1")
        .with_schema_version(subject, 1)
        .build();
    producer1.create().await?;

    // Register V2
    let schema_v2 = r#"{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v2.as_bytes())
        .execute()
        .await?;

    // Second producer with V2 (same subject, different version - should succeed)
    let mut producer2 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_v2")
        .with_schema_version(subject, 2)
        .build();

    let result = producer2.create().await;
    assert!(
        result.is_ok(),
        "Producer with different version of same subject should succeed"
    );

    Ok(())
}
