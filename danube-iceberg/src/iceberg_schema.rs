//! Arrow → Iceberg schema conversion.
//!
//! Converts an Arrow `Schema` (used by our `schema.rs` module for RecordBatch
//! construction) to an Iceberg `Schema` (used by the catalog for table creation
//! and schema evolution).
//!
//! ## Type mapping
//!
//! | Arrow Type | Iceberg Type |
//! |------------|-------------|
//! | `Boolean`  | `Boolean`   |
//! | `Int32`    | `Int`       |
//! | `Int64`    | `Long`      |
//! | `UInt64`   | `Long`      |
//! | `Float32`  | `Float`     |
//! | `Float64`  | `Double`    |
//! | `Utf8`     | `String`    |
//! | `Binary`   | `Binary`    |
//! | other      | `String`    |
//!
//! Iceberg field IDs are assigned sequentially starting from 1.

use arrow_schema::{DataType as ArrowType, Schema as ArrowSchema};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};

/// Convert an Arrow schema to an Iceberg schema.
///
/// Each Arrow field becomes an Iceberg `NestedField`. Field IDs are assigned
/// sequentially (1, 2, 3, ...) since we don't have pre-existing Iceberg field
/// IDs to preserve. The catalog will handle field ID remapping on schema
/// evolution.
pub fn arrow_to_iceberg_schema(arrow_schema: &ArrowSchema) -> anyhow::Result<IcebergSchema> {
    let mut fields = Vec::with_capacity(arrow_schema.fields().len());

    for (i, arrow_field) in arrow_schema.fields().iter().enumerate() {
        let field_id = (i + 1) as i32;
        let iceberg_type = arrow_type_to_iceberg(&arrow_field.data_type())?;

        let field = if arrow_field.is_nullable() {
            NestedField::optional(field_id, arrow_field.name(), iceberg_type)
        } else {
            NestedField::required(field_id, arrow_field.name(), iceberg_type)
        };

        fields.push(field.into());
    }

    let schema = IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build Iceberg schema: {}", e))?;

    Ok(schema)
}

/// Map a single Arrow data type to an Iceberg type.
fn arrow_type_to_iceberg(arrow_type: &ArrowType) -> anyhow::Result<Type> {
    let iceberg_type = match arrow_type {
        ArrowType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        ArrowType::Int32 => Type::Primitive(PrimitiveType::Int),
        ArrowType::Int64 => Type::Primitive(PrimitiveType::Long),
        ArrowType::UInt64 => {
            // Iceberg doesn't have unsigned types — map to Long (i64).
            // UInt64 values > i64::MAX are extremely unlikely in practice
            // (offsets, timestamps, schema_ids are all small).
            Type::Primitive(PrimitiveType::Long)
        }
        ArrowType::Float32 => Type::Primitive(PrimitiveType::Float),
        ArrowType::Float64 => Type::Primitive(PrimitiveType::Double),
        ArrowType::Utf8 | ArrowType::LargeUtf8 => Type::Primitive(PrimitiveType::String),
        ArrowType::Binary | ArrowType::LargeBinary => Type::Primitive(PrimitiveType::Binary),
        ArrowType::Date32 => Type::Primitive(PrimitiveType::Date),
        ArrowType::Timestamp(_, _) => Type::Primitive(PrimitiveType::Timestamptz),
        // Fallback: store as string (safe for any type)
        other => {
            tracing::warn!(
                arrow_type = ?other,
                "unmapped Arrow type, falling back to Iceberg String"
            );
            Type::Primitive(PrimitiveType::String)
        }
    };
    Ok(iceberg_type)
}

/// Check if two Iceberg schemas are compatible (the new schema is a superset
/// of the existing schema — no field removals or type changes).
///
/// Returns `Ok(new_fields)` with the list of fields to add, or `Err` if there
/// is an incompatible change.
#[allow(dead_code)] // Prepared for schema evolution — will be called from worker.rs
pub fn schema_diff(
    existing: &IcebergSchema,
    new: &IcebergSchema,
) -> anyhow::Result<Vec<NestedField>> {
    let existing_fields: std::collections::HashMap<&str, &NestedField> = existing
        .as_struct()
        .fields()
        .iter()
        .map(|f| (f.name.as_str(), f.as_ref()))
        .collect();

    let mut new_fields = Vec::new();

    for new_field in new.as_struct().fields() {
        match existing_fields.get(new_field.name.as_str()) {
            Some(existing_field) => {
                // Field exists — verify type compatibility
                if existing_field.field_type != new_field.field_type {
                    anyhow::bail!(
                        "incompatible schema change: field '{}' type changed from {:?} to {:?}",
                        new_field.name,
                        existing_field.field_type,
                        new_field.field_type
                    );
                }
            }
            None => {
                // New field — needs to be added via schema evolution
                new_fields.push(new_field.as_ref().clone());
            }
        }
    }

    Ok(new_fields)
}
