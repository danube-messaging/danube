use crate::avro::AvroCompatibilityChecker;
use crate::json::JsonCompatibilityChecker;
use crate::metadata::{CompatibilityResult, SchemaDefinition};
use crate::protobuf::ProtobufCompatibilityChecker;
use crate::types::CompatibilityMode;
use anyhow::Result;

/// Compatibility checker for schema evolution
#[derive(Debug)]
pub struct CompatibilityChecker;

impl CompatibilityChecker {
    pub fn new() -> Self { Self }

    pub fn check(&self, old_schema: &SchemaDefinition, new_schema: &SchemaDefinition, mode: CompatibilityMode) -> Result<CompatibilityResult> {
        if matches!(mode, CompatibilityMode::None) {
            return Ok(CompatibilityResult::compatible());
        }
        if std::mem::discriminant(old_schema) != std::mem::discriminant(new_schema) {
            return Ok(CompatibilityResult::incompatible(vec![
                "Schema types must match (cannot change from Avro to JSON, etc.)".to_string(),
            ]));
        }
        match (old_schema, new_schema) {
            (SchemaDefinition::Bytes, SchemaDefinition::Bytes) => Ok(CompatibilityResult::compatible()),
            (SchemaDefinition::String, SchemaDefinition::String) => Ok(CompatibilityResult::compatible()),
            (SchemaDefinition::Number, SchemaDefinition::Number) => Ok(CompatibilityResult::compatible()),
            (SchemaDefinition::Avro(old), SchemaDefinition::Avro(new)) => {
                AvroCompatibilityChecker::new().check(old, new, mode)
            }
            (SchemaDefinition::JsonSchema(old), SchemaDefinition::JsonSchema(new)) => {
                JsonCompatibilityChecker::new().check(old, new, mode)
            }
            (SchemaDefinition::Protobuf(old), SchemaDefinition::Protobuf(new)) => {
                ProtobufCompatibilityChecker::new().check(old, new, mode)
            }
            _ => Ok(CompatibilityResult::incompatible(vec!["Mismatched schema types".to_string()])),
        }
    }
}

impl Default for CompatibilityChecker {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_different_schema_types_incompatible() {
        let checker = CompatibilityChecker::new();
        let result = checker.check(&SchemaDefinition::String, &SchemaDefinition::Number, CompatibilityMode::Backward).unwrap();
        assert!(!result.is_compatible);
    }
}
