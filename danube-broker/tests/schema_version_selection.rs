//! Schema Version Selection Tests
//!
//! Tests producer schema version control:
//! - Pin to specific schema version
//! - Use minimum schema version
//! - Version validation and enforcement
//! - Multiple producers with different versions

use anyhow::Result;
use danube_client::{SchemaRegistryClient, SchemaType, SubType};
use serde_json::json;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// Test 1: Producer pinned to specific schema version
/// 
/// **What:** Registers V1 and V2, producer pins to V1, sends message.
/// **Why:** Validates that producers can pin to specific versions instead of using latest.
#[tokio::test]
async fn producer_pin_to_specific_version() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/pin_version");
    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    let subject = "version-pin-test";

    // Register V1
    let schema_v1 = r#"{"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v1.as_bytes())
        .execute()
        .await?;

    // Register V2 (add optional field)
    let schema_v2 = r#"{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}, "required": ["id"]}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v2.as_bytes())
        .execute()
        .await?;

    // Producer explicitly pins to V1 (not latest V2)
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_v1_pinned")
        .with_schema_version(subject, 1)
        .build()?;
    producer.create().await?;

    // Create consumer
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_version")
        .with_subscription("sub_version")
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // Send message (should use V1 schema)
    producer.send(json!({"id": 123}).to_string().as_bytes().to_vec(), None).await?;

    // Verify message has V1 schema version
    let message = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive message");

    assert_eq!(message.schema_version, Some(1), "Should use pinned version 1");
    consumer.ack(&message).await?;

    Ok(())
}

/// Test 2: Producer with minimum version constraint
/// 
/// **What:** Registers V1, V2, V3, producer requires min V2, should use V3 (latest >= V2).
/// **Why:** Validates that producers can enforce minimum version requirements.
#[tokio::test]
async fn producer_minimum_version_uses_latest() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/min_version");
    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    let subject = "min-version-test";

    // Register V1
    let schema_v1 = r#"{"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v1.as_bytes())
        .execute()
        .await?;

    // Register V2
    let schema_v2 = r#"{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}, "required": ["id"]}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v2.as_bytes())
        .execute()
        .await?;

    // Register V3
    let schema_v3 = r#"{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}, "active": {"type": "boolean"}}, "required": ["id"]}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v3.as_bytes())
        .execute()
        .await?;

    // Producer requires minimum V2, should use V3 (latest)
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_min_v2")
        .with_schema_min_version(subject, 2)
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_min")
        .with_subscription("sub_min")
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // Send message (should use V3, which is >= V2)
    producer.send(json!({"id": 456}).to_string().as_bytes().to_vec(), None).await?;

    let message = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive message");

    assert!(
        message.schema_version.unwrap() >= 2,
        "Should use version >= 2 (got {})",
        message.schema_version.unwrap()
    );
    consumer.ack(&message).await?;

    Ok(())
}

/// Test 3: Two producers same topic different versions
/// 
/// **What:** Producer1 uses V1, Producer2 uses V2, both send to same topic.
/// **Why:** Validates that multiple producers can use different versions on the same topic.
#[tokio::test]
async fn multiple_producers_different_versions() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/multi_version");
    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    let subject = "multi-version-subject";

    // Register V1
    let schema_v1 = r#"{"type": "object", "properties": {"value": {"type": "integer"}}, "required": ["value"]}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v1.as_bytes())
        .execute()
        .await?;

    // Register V2
    let schema_v2 = r#"{"type": "object", "properties": {"value": {"type": "integer"}, "label": {"type": "string"}}, "required": ["value"]}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v2.as_bytes())
        .execute()
        .await?;

    // Producer 1 pinned to V1
    let mut producer1 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_v1")
        .with_schema_version(subject, 1)
        .build()?;
    producer1.create().await?;

    // Producer 2 pinned to V2
    let mut producer2 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_v2")
        .with_schema_version(subject, 2)
        .build()?;
    producer2.create().await?;

    // Consumer
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_both")
        .with_subscription("sub_both")
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // Send from both producers
    producer1.send(json!({"value": 1}).to_string().as_bytes().to_vec(), None).await?;
    producer2.send(json!({"value": 2, "label": "test"}).to_string().as_bytes().to_vec(), None).await?;

    // Receive both messages
    let msg1 = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive message 1");
    
    let msg2 = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive message 2");

    // Verify different versions
    let versions = vec![msg1.schema_version.unwrap(), msg2.schema_version.unwrap()];
    assert!(versions.contains(&1), "Should have V1 message");
    assert!(versions.contains(&2), "Should have V2 message");

    consumer.ack(&msg1).await?;
    consumer.ack(&msg2).await?;

    Ok(())
}

/// Test 4: Producer with non-existent version fails
/// 
/// **What:** Attempts to create producer with version 99 that doesn't exist.
/// **Why:** Validates that invalid version numbers are rejected at producer creation.
#[tokio::test]
async fn producer_invalid_version_fails() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/invalid_version");
    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    let subject = "invalid-version-test";

    // Register only V1
    let schema = r#"{"type": "object", "properties": {"x": {"type": "integer"}}}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema.as_bytes())
        .execute()
        .await?;

    // Try to use non-existent V99
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_invalid")
        .with_schema_version(subject, 99)
        .build()?;

    let result = producer.create().await;
    assert!(result.is_err(), "Should fail with invalid version");

    Ok(())
}

/// Test 5: Latest version behavior (no version specified)
/// 
/// **What:** Registers V1, V2, producer uses .with_schema_subject() (no version), should use V2.
/// **Why:** Confirms that not specifying a version defaults to latest.
#[tokio::test]
async fn producer_no_version_uses_latest() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/latest_version");
    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    let subject = "latest-version-test";

    // Register V1
    let schema_v1 = r#"{"type": "object", "properties": {"a": {"type": "string"}}}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v1.as_bytes())
        .execute()
        .await?;

    // Register V2
    let schema_v2 = r#"{"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "integer"}}}"#;
    schema_client
        .register_schema(subject)
        .with_type(SchemaType::JsonSchema)
        .with_schema_data(schema_v2.as_bytes())
        .execute()
        .await?;

    // Producer without version specification (should use latest V2)
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_latest")
        .with_schema_subject(subject)
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_latest")
        .with_subscription("sub_latest")
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    producer.send(json!({"a": "test"}).to_string().as_bytes().to_vec(), None).await?;

    let message = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive message");

    assert_eq!(
        message.schema_version,
        Some(2),
        "Should use latest version (V2)"
    );
    consumer.ack(&message).await?;

    Ok(())
}

/// Test 6: Minimum version exactly at boundary
/// 
/// **What:** Registers V1, V2, V3, producer requires min V3, should use exactly V3.
/// **Why:** Validates that minimum version works when it equals the latest version.
#[tokio::test]
async fn producer_minimum_version_at_boundary() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/min_boundary");
    let mut schema_client = SchemaRegistryClient::new(&client).await?;

    let subject = "boundary-test";

    // Register V1, V2, V3
    for i in 1..=3 {
        let schema = format!(
            r#"{{"type": "object", "properties": {{"v": {{"type": "integer", "const": {}}}}}}}"#,
            i
        );
        schema_client
            .register_schema(subject)
            .with_type(SchemaType::JsonSchema)
            .with_schema_data(schema.as_bytes())
            .execute()
            .await?;
    }

    // Require minimum V3 (which is also latest)
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_boundary")
        .with_schema_min_version(subject, 3)
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_boundary")
        .with_subscription("sub_boundary")
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    producer.send(json!({"v": 3}).to_string().as_bytes().to_vec(), None).await?;

    let message = timeout(Duration::from_secs(5), async { stream.recv().await })
        .await?
        .expect("Should receive message");

    assert_eq!(
        message.schema_version,
        Some(3),
        "Should use exactly V3"
    );
    consumer.ack(&message).await?;

    Ok(())
}
