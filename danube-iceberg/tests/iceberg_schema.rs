//! # Iceberg Schema Conversion Tests
//!
//! Tests the Arrow → Iceberg schema conversion using the `iceberg::arrow`
//! module's built-in `arrow_schema_to_schema_auto_assign_ids` function.
//!
//! Also includes envelope schema tests — the universal fallback schema that
//! every Danube message can be represented as.
//!
//! ## Why this matters
//!
//! When creating an Iceberg table, we must convert our Arrow schema (used for
//! RecordBatch construction) to an Iceberg schema (used by the catalog for
//! table metadata). Incorrect type mapping would cause query engines to
//! misinterpret column data — e.g., reading a Float64 as an Int64 would
//! produce garbage results.
//!
//! ## Design note
//!
//! These tests use the `iceberg::arrow` module directly (same as the production
//! code in `src/iceberg_schema.rs`), validating that the crate's conversion
//! produces the expected Iceberg types for Danube's envelope and registry schemas.

mod common;

use arrow_array::cast::AsArray;
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::spec::{
    NestedField, PrimitiveType, Schema as IcebergSchema, Type,
};
use std::collections::HashMap;

// ============================================================================
// Helper — wraps the crate function, same as src/iceberg_schema.rs
// ============================================================================

fn arrow_to_iceberg_schema(arrow_schema: &ArrowSchema) -> IcebergSchema {
    arrow_schema_to_schema_auto_assign_ids(arrow_schema)
        .expect("arrow to iceberg conversion")
}

fn schema_diff(
    existing: &IcebergSchema,
    new: &IcebergSchema,
) -> Result<Vec<NestedField>, String> {
    let existing_fields: HashMap<&str, &NestedField> = existing
        .as_struct()
        .fields()
        .iter()
        .map(|f| (f.name.as_str(), f.as_ref()))
        .collect();

    let mut new_fields = Vec::new();
    for new_field in new.as_struct().fields() {
        match existing_fields.get(new_field.name.as_str()) {
            Some(existing_field) => {
                if existing_field.field_type != new_field.field_type {
                    return Err(format!(
                        "incompatible type change for field '{}': {:?} -> {:?}",
                        new_field.name, existing_field.field_type, new_field.field_type
                    ));
                }
            }
            None => {
                new_fields.push(new_field.as_ref().clone());
            }
        }
    }
    Ok(new_fields)
}

// ============================================================================
// Arrow → Iceberg conversion tests
// ============================================================================

/// Verifies the type mapping from Arrow to Iceberg for the primitive types
/// used by Danube's envelope and registry schemas.
///
/// The `iceberg::arrow` module handles the actual mapping — we verify
/// the results match expectations for our specific types.
#[test]
fn arrow_to_iceberg_primitive_types() {
    let arrow_schema = ArrowSchema::new(vec![
        Field::new("bool_col", ArrowType::Boolean, false),
        Field::new("int32_col", ArrowType::Int32, false),
        Field::new("int64_col", ArrowType::Int64, false),
        Field::new("float32_col", ArrowType::Float32, false),
        Field::new("float64_col", ArrowType::Float64, false),
        Field::new("string_col", ArrowType::Utf8, false),
        Field::new("binary_col", ArrowType::Binary, false),
    ]);

    let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema);
    let fields: Vec<_> = iceberg_schema.as_struct().fields().iter().collect();

    assert_eq!(fields.len(), 7);
    assert_eq!(*fields[0].field_type, Type::Primitive(PrimitiveType::Boolean));
    assert_eq!(*fields[1].field_type, Type::Primitive(PrimitiveType::Int));
    assert_eq!(*fields[2].field_type, Type::Primitive(PrimitiveType::Long));
    assert_eq!(*fields[3].field_type, Type::Primitive(PrimitiveType::Float));
    assert_eq!(*fields[4].field_type, Type::Primitive(PrimitiveType::Double));
    assert_eq!(*fields[5].field_type, Type::Primitive(PrimitiveType::String));
    assert_eq!(*fields[6].field_type, Type::Primitive(PrimitiveType::Binary));
}

/// Verifies that field names are preserved and IDs are auto-assigned.
///
/// The `iceberg::arrow::arrow_schema_to_schema_auto_assign_ids` function
/// assigns field IDs starting from 1 using level-order traversal.
#[test]
fn arrow_to_iceberg_field_names_and_required() {
    let arrow_schema = ArrowSchema::new(vec![
        Field::new("offset", ArrowType::Int64, false),
        Field::new("temperature", ArrowType::Float64, true),
        Field::new("unit", ArrowType::Utf8, true),
    ]);

    let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema);
    let fields: Vec<_> = iceberg_schema.as_struct().fields().iter().collect();

    assert_eq!(fields[0].name, "offset");
    assert!(fields[0].required, "offset should be required");

    assert_eq!(fields[1].name, "temperature");
    assert!(!fields[1].required, "temperature should be optional (not required)");

    assert_eq!(fields[2].name, "unit");
    assert!(!fields[2].required, "unit should be optional (not required)");
}

/// Verifies that the Danube envelope schema (binary payloads) converts correctly.
///
/// The envelope schema is: offset (Int64), publish_time (Int64),
/// producer_name (Utf8), payload (Binary). This is the most common schema
/// used when messages are not JSON.
#[test]
fn arrow_to_iceberg_envelope_schema() {
    let arrow_schema = ArrowSchema::new(vec![
        Field::new("offset", ArrowType::Int64, false),
        Field::new("publish_time", ArrowType::Int64, false),
        Field::new("producer_name", ArrowType::Utf8, true),
        Field::new("payload", ArrowType::Binary, false),
    ]);

    let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema);
    let fields: Vec<_> = iceberg_schema.as_struct().fields().iter().collect();

    assert_eq!(fields.len(), 4);
    assert_eq!(*fields[0].field_type, Type::Primitive(PrimitiveType::Long));
    assert_eq!(*fields[1].field_type, Type::Primitive(PrimitiveType::Long));
    assert_eq!(*fields[2].field_type, Type::Primitive(PrimitiveType::String));
    assert_eq!(*fields[3].field_type, Type::Primitive(PrimitiveType::Binary));
}

/// Verifies that schema_diff detects new fields (additive schema evolution).
///
/// When a JSON producer adds new fields, the converter needs to detect them
/// and evolve the Iceberg table schema. This test verifies the diff logic.
#[test]
fn schema_diff_detects_new_fields() {
    let existing = arrow_to_iceberg_schema(&ArrowSchema::new(vec![
        Field::new("offset", ArrowType::Int64, false),
        Field::new("temperature", ArrowType::Float64, true),
    ]));

    let new = arrow_to_iceberg_schema(&ArrowSchema::new(vec![
        Field::new("offset", ArrowType::Int64, false),
        Field::new("temperature", ArrowType::Float64, true),
        Field::new("humidity", ArrowType::Int64, true),
    ]));

    let diff = schema_diff(&existing, &new).expect("diff should succeed");
    assert_eq!(diff.len(), 1, "should detect one new field");
    assert_eq!(diff[0].name, "humidity");
}

/// Verifies that schema_diff returns empty when schemas are identical.
#[test]
fn schema_diff_no_changes() {
    let schema = arrow_to_iceberg_schema(&ArrowSchema::new(vec![
        Field::new("offset", ArrowType::Int64, false),
        Field::new("value", ArrowType::Utf8, true),
    ]));

    let diff = schema_diff(&schema, &schema).expect("diff should succeed");
    assert!(diff.is_empty(), "identical schemas should have no diff");
}

/// Verifies that schema_diff rejects incompatible type changes.
///
/// If a field's type changes (e.g., temperature from Float64 to Utf8),
/// that's an incompatible change. The diff should return an error.
#[test]
fn schema_diff_incompatible_type_change() {
    let existing = arrow_to_iceberg_schema(&ArrowSchema::new(vec![
        Field::new("temperature", ArrowType::Float64, true),
    ]));

    let new = arrow_to_iceberg_schema(&ArrowSchema::new(vec![
        Field::new("temperature", ArrowType::Utf8, true),
    ]));

    let result = schema_diff(&existing, &new);
    assert!(result.is_err(), "type change should be rejected");
    let err_msg = result.unwrap_err();
    assert!(
        err_msg.contains("temperature"),
        "error should mention the field name"
    );
}

/// Verifies that an empty Arrow schema produces an empty Iceberg schema.
///
/// Edge case: a topic with no messages yet might have an empty schema.
#[test]
fn arrow_to_iceberg_empty_schema() {
    let arrow_schema = ArrowSchema::empty();
    let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema);
    assert!(
        iceberg_schema.as_struct().fields().is_empty(),
        "empty Arrow schema should produce empty Iceberg schema"
    );
}

// ============================================================================
// Envelope schema tests (the universal fallback)
// ============================================================================

/// Helper: create a DecodedMessage for testing.
struct DecodedMessage {
    offset: u64,
    message: danube_core::message::StreamMessage,
}

fn make_decoded(offset: u64, payload: &[u8]) -> DecodedMessage {
    use bytes::Bytes;
    use std::collections::HashMap;

    DecodedMessage {
        offset,
        message: danube_core::message::StreamMessage {
            request_id: 0,
            msg_id: danube_core::message::MessageID {
                producer_id: 1,
                topic_name: "test".to_string(),
                broker_addr: "localhost".to_string(),
                topic_offset: offset,
            },
            payload: Bytes::from(payload.to_vec()),
            publish_time: 1000 + offset,
            producer_name: format!("producer-{}", offset),
            subscription_name: None,
            attributes: HashMap::new(),
            schema_id: None,
            schema_version: None,
            routing_key: None,
        },
    }
}

/// Verifies that the envelope schema produces correct RecordBatches.
///
/// The envelope is the universal fallback — every Danube message can be
/// represented as: offset | publish_time | producer_name | routing_key |
/// schema_id | schema_version | payload | attributes_json
#[test]
fn envelope_batch_roundtrip() {
    use arrow_array::builder::{BinaryBuilder, Int64Builder, StringBuilder};
    use arrow_array::RecordBatch;
    use std::sync::Arc;

    let messages: Vec<DecodedMessage> = (0..5).map(|i| {
        make_decoded(i, format!("payload-{}", i).as_bytes())
    }).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("offset", ArrowType::Int64, false),
        Field::new("publish_time", ArrowType::Int64, false),
        Field::new("producer_name", ArrowType::Utf8, false),
        Field::new("payload", ArrowType::Binary, false),
    ]));

    let mut offsets = Int64Builder::with_capacity(5);
    let mut publish_times = Int64Builder::with_capacity(5);
    let mut producers = StringBuilder::with_capacity(5, 5 * 32);
    let mut payloads = BinaryBuilder::with_capacity(5, 5 * 64);

    for dm in &messages {
        offsets.append_value(dm.offset as i64);
        publish_times.append_value(dm.message.publish_time as i64);
        producers.append_value(&dm.message.producer_name);
        payloads.append_value(&dm.message.payload);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(offsets.finish()),
            Arc::new(publish_times.finish()),
            Arc::new(producers.finish()),
            Arc::new(payloads.finish()),
        ],
    ).unwrap();

    assert_eq!(batch.num_rows(), 5);
    assert_eq!(batch.num_columns(), 4);

    let read_offsets = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
    for i in 0..5 {
        assert_eq!(read_offsets.value(i), i as i64);
    }

    let read_producers = batch.column(2).as_string::<i32>();
    assert_eq!(read_producers.value(0), "producer-0");
}

/// Verifies that the envelope schema has the expected column types.
#[test]
fn envelope_schema_structure() {
    let schema = ArrowSchema::new(vec![
        Field::new("offset", ArrowType::Int64, false),
        Field::new("publish_time", ArrowType::Int64, false),
        Field::new("producer_name", ArrowType::Utf8, false),
        Field::new("routing_key", ArrowType::Utf8, true),
        Field::new("schema_id", ArrowType::Int64, true),
        Field::new("schema_version", ArrowType::Int64, true),
        Field::new("payload", ArrowType::Binary, false),
        Field::new("attributes_json", ArrowType::Utf8, true),
    ]);

    assert_eq!(schema.fields().len(), 8);
    assert_eq!(*schema.field(0).data_type(), ArrowType::Int64);
    assert_eq!(*schema.field(6).data_type(), ArrowType::Binary);
    assert!(schema.field(3).is_nullable());
    assert!(!schema.field(0).is_nullable());
}
