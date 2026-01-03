use crate::schema::avro::AvroCompatibilityChecker;
use crate::schema::json::JsonCompatibilityChecker;
use crate::schema::metadata::{CompatibilityResult, SchemaDefinition};
use crate::schema::protobuf::ProtobufCompatibilityChecker;
use crate::schema::types::CompatibilityMode;
use anyhow::Result;

/// Compatibility checker for schema evolution
#[derive(Debug)]
pub struct CompatibilityChecker;

impl CompatibilityChecker {
    pub fn new() -> Self {
        Self
    }

    /// Check compatibility between old and new schema
    pub fn check(
        &self,
        old_schema: &SchemaDefinition,
        new_schema: &SchemaDefinition,
        mode: CompatibilityMode,
    ) -> Result<CompatibilityResult> {
        // If mode is None, always compatible
        if matches!(mode, CompatibilityMode::None) {
            return Ok(CompatibilityResult::compatible());
        }

        // Schemas must be of the same type
        if std::mem::discriminant(old_schema) != std::mem::discriminant(new_schema) {
            return Ok(CompatibilityResult::incompatible(vec![
                "Schema types must match (cannot change from Avro to JSON, etc.)".to_string(),
            ]));
        }

        match (old_schema, new_schema) {
            (SchemaDefinition::Bytes, SchemaDefinition::Bytes) => {
                Ok(CompatibilityResult::compatible())
            }
            (SchemaDefinition::String, SchemaDefinition::String) => {
                Ok(CompatibilityResult::compatible())
            }
            (SchemaDefinition::Number, SchemaDefinition::Number) => {
                Ok(CompatibilityResult::compatible())
            }

            (SchemaDefinition::Avro(old), SchemaDefinition::Avro(new)) => {
                let checker = AvroCompatibilityChecker::new();
                checker.check(old, new, mode)
            }

            (SchemaDefinition::JsonSchema(old), SchemaDefinition::JsonSchema(new)) => {
                let checker = JsonCompatibilityChecker::new();
                checker.check(old, new, mode)
            }

            (SchemaDefinition::Protobuf(old), SchemaDefinition::Protobuf(new)) => {
                let checker = ProtobufCompatibilityChecker::new();
                checker.check(old, new, mode)
            }

            _ => Ok(CompatibilityResult::incompatible(vec![
                "Mismatched schema types".to_string(),
            ])),
        }
    }

}

impl Default for CompatibilityChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_different_schema_types_incompatible() {
        let checker = CompatibilityChecker::new();

        let result = checker
            .check(
                &SchemaDefinition::String,
                &SchemaDefinition::Number,
                CompatibilityMode::Backward,
            )
            .unwrap();

        assert!(!result.is_compatible);
    }

    // Format-specific tests moved to their respective modules:
    // - avro/avro_compatibility.rs
    // - json/json_compatibility.rs
    // - protobuf/protobuf_compatibility.rs
}
