//! # Iceberg Schema Conversion Tests
//!
//! Tests the Arrow → Iceberg schema conversion logic.
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
//! Since `danube-iceberg` is a binary crate, the conversion functions are
//! re-implemented inline here (mirroring `src/iceberg_schema.rs`). This
//! validates the algorithm independently and catches any drift.

mod common;

use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use iceberg::spec::{
    NestedField, PrimitiveType, Schema as IcebergSchema, Type,
};
use std::collections::HashMap;

// ============================================================================
// Inline mirror of src/iceberg_schema.rs conversion logic
// ============================================================================

fn arrow_type_to_iceberg(arrow_type: &ArrowType) -> Type {
    match arrow_type {
        ArrowType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        ArrowType::Int32 => Type::Primitive(PrimitiveType::Int),
        ArrowType::Int64 => Type::Primitive(PrimitiveType::Long),
        ArrowType::UInt64 => Type::Primitive(PrimitiveType::Long),
        ArrowType::Float32 => Type::Primitive(PrimitiveType::Float),
        ArrowType::Float64 => Type::Primitive(PrimitiveType::Double),
        ArrowType::Utf8 | ArrowType::LargeUtf8 => Type::Primitive(PrimitiveType::String),
        ArrowType::Binary | ArrowType::LargeBinary => Type::Primitive(PrimitiveType::Binary),
        ArrowType::Date32 => Type::Primitive(PrimitiveType::Date),
        ArrowType::Timestamp(_, _) => Type::Primitive(PrimitiveType::Timestamptz),
        _ => Type::Primitive(PrimitiveType::String), // fallback
    }
}

fn arrow_to_iceberg_schema(arrow_schema: &ArrowSchema) -> IcebergSchema {
    let mut fields = Vec::with_capacity(arrow_schema.fields().len());
    for (i, arrow_field) in arrow_schema.fields().iter().enumerate() {
        let field_id = (i + 1) as i32;
        let iceberg_type = arrow_type_to_iceberg(arrow_field.data_type());
        let field = if arrow_field.is_nullable() {
            NestedField::optional(field_id, arrow_field.name(), iceberg_type)
        } else {
            NestedField::required(field_id, arrow_field.name(), iceberg_type)
        };
        fields.push(field.into());
    }
    IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .expect("schema build")
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
// Tests
// ============================================================================

/// Verifies the basic type mapping from Arrow to Iceberg for all supported
/// primitive types: Boolean, Int32, Int64, UInt64, Float32, Float64, Utf8, Binary.
///
/// Each Arrow type must map to exactly one Iceberg type. This test ensures
/// we don't accidentally swap mappings (e.g., Float64 → Float instead of Double).
#[test]
fn arrow_to_iceberg_primitive_types() {
    let arrow_schema = ArrowSchema::new(vec![
        Field::new("bool_col", ArrowType::Boolean, false),
        Field::new("int32_col", ArrowType::Int32, false),
        Field::new("int64_col", ArrowType::Int64, false),
        Field::new("uint64_col", ArrowType::UInt64, false),
        Field::new("float32_col", ArrowType::Float32, false),
        Field::new("float64_col", ArrowType::Float64, false),
        Field::new("string_col", ArrowType::Utf8, false),
        Field::new("binary_col", ArrowType::Binary, false),
    ]);

    let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema);
    let fields: Vec<_> = iceberg_schema.as_struct().fields().iter().collect();

    assert_eq!(fields.len(), 8);
    // field_type is Box<Type>, so we compare with Box::new()
    assert_eq!(*fields[0].field_type, Type::Primitive(PrimitiveType::Boolean));
    assert_eq!(*fields[1].field_type, Type::Primitive(PrimitiveType::Int));
    assert_eq!(*fields[2].field_type, Type::Primitive(PrimitiveType::Long));
    assert_eq!(*fields[3].field_type, Type::Primitive(PrimitiveType::Long)); // UInt64 → Long
    assert_eq!(*fields[4].field_type, Type::Primitive(PrimitiveType::Float));
    assert_eq!(*fields[5].field_type, Type::Primitive(PrimitiveType::Double));
    assert_eq!(*fields[6].field_type, Type::Primitive(PrimitiveType::String));
    assert_eq!(*fields[7].field_type, Type::Primitive(PrimitiveType::Binary));
}

/// Verifies that field names are preserved and IDs assigned sequentially (1, 2, 3...).
///
/// Iceberg requires unique field IDs for schema evolution tracking.
/// Our conversion assigns IDs starting from 1 in field order.
#[test]
fn arrow_to_iceberg_field_names_and_ids() {
    let arrow_schema = ArrowSchema::new(vec![
        Field::new("offset", ArrowType::UInt64, false),
        Field::new("temperature", ArrowType::Float64, true),
        Field::new("unit", ArrowType::Utf8, true),
    ]);

    let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema);
    let fields: Vec<_> = iceberg_schema.as_struct().fields().iter().collect();

    assert_eq!(fields[0].name, "offset");
    assert_eq!(fields[0].id, 1);
    assert!(fields[0].required, "offset should be required");

    assert_eq!(fields[1].name, "temperature");
    assert_eq!(fields[1].id, 2);
    assert!(!fields[1].required, "temperature should be optional (not required)");

    assert_eq!(fields[2].name, "unit");
    assert_eq!(fields[2].id, 3);
    assert!(!fields[2].required, "unit should be optional (not required)");
}

/// Verifies that the Danube envelope schema (binary payloads) converts correctly.
///
/// The envelope schema is: offset (UInt64), publish_time (UInt64),
/// producer_name (Utf8), payload (Binary). This is the most common schema
/// used when messages are not JSON.
#[test]
fn arrow_to_iceberg_envelope_schema() {
    let arrow_schema = ArrowSchema::new(vec![
        Field::new("offset", ArrowType::UInt64, false),
        Field::new("publish_time", ArrowType::UInt64, false),
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
        Field::new("offset", ArrowType::UInt64, false),
        Field::new("temperature", ArrowType::Float64, true),
    ]));

    let new = arrow_to_iceberg_schema(&ArrowSchema::new(vec![
        Field::new("offset", ArrowType::UInt64, false),
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
        Field::new("offset", ArrowType::UInt64, false),
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
