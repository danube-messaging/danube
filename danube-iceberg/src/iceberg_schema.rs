//! Arrow → Iceberg schema conversion.
//!
//! Delegates to the `iceberg::arrow` module for Arrow-to-Iceberg type and
//! schema conversion. This ensures we stay aligned with the upstream type
//! mapping (including nested types, timestamps, decimals, etc.) rather than
//! maintaining our own manual mapping table.
//!
//! The main entry point is [`arrow_to_iceberg_schema`], which wraps
//! `iceberg::arrow::arrow_schema_to_schema_auto_assign_ids` — this variant
//! auto-assigns field IDs starting from 1, which is what we need since our
//! Arrow schemas don't carry pre-existing Iceberg field IDs.

use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::spec::{NestedField, Schema as IcebergSchema};

/// Convert an Arrow schema to an Iceberg schema.
///
/// Uses the `iceberg` crate's built-in conversion which handles all Arrow
/// types (primitives, nested structs, lists, maps, timestamps, decimals,
/// etc.) and auto-assigns sequential field IDs starting from 1.
pub fn arrow_to_iceberg_schema(arrow_schema: &ArrowSchema) -> anyhow::Result<IcebergSchema> {
    arrow_schema_to_schema_auto_assign_ids(arrow_schema)
        .map_err(|e| anyhow::anyhow!("failed to convert Arrow schema to Iceberg: {}", e))
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
