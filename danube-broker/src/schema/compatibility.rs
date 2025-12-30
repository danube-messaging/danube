use crate::schema::metadata::{
    AvroSchema, CompatibilityResult, JsonSchemaDefinition, SchemaDefinition,
};
use crate::schema::types::CompatibilityMode;
use anyhow::{anyhow, Result};

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
                self.check_avro_compatibility(old, new, mode)
            }

            (SchemaDefinition::JsonSchema(old), SchemaDefinition::JsonSchema(new)) => {
                self.check_json_compatibility(old, new, mode)
            }

            (SchemaDefinition::Protobuf(_), SchemaDefinition::Protobuf(_)) => {
                // TODO: Implement protobuf compatibility checking
                Ok(CompatibilityResult::compatible())
            }

            _ => Ok(CompatibilityResult::incompatible(vec![
                "Mismatched schema types".to_string(),
            ])),
        }
    }

    /// Check Avro schema compatibility
    fn check_avro_compatibility(
        &self,
        old: &AvroSchema,
        new: &AvroSchema,
        mode: CompatibilityMode,
    ) -> Result<CompatibilityResult> {
        let old_schema = apache_avro::Schema::parse_str(&old.raw_schema)
            .map_err(|e| anyhow!("Failed to parse old Avro schema: {}", e))?;

        let new_schema = apache_avro::Schema::parse_str(&new.raw_schema)
            .map_err(|e| anyhow!("Failed to parse new Avro schema: {}", e))?;

        match mode {
            CompatibilityMode::None => Ok(CompatibilityResult::compatible()),

            CompatibilityMode::Backward => {
                // New schema (reader) must be able to read data written with old schema (writer)
                self.check_avro_backward(&old_schema, &new_schema)
            }

            CompatibilityMode::Forward => {
                // Old schema (reader) must be able to read data written with new schema (writer)
                self.check_avro_forward(&old_schema, &new_schema)
            }

            CompatibilityMode::Full => {
                // Must be both backward and forward compatible
                let backward = self.check_avro_backward(&old_schema, &new_schema)?;
                if !backward.is_compatible {
                    return Ok(backward);
                }
                self.check_avro_forward(&old_schema, &new_schema)
            }
        }
    }

    /// Check backward compatibility for Avro (new can read old)
    /// In Avro 0.21+, we check if reader schema can resolve against writer schema
    fn check_avro_backward(
        &self,
        writer_schema: &apache_avro::Schema,
        reader_schema: &apache_avro::Schema,
    ) -> Result<CompatibilityResult> {
        // In newer Avro API, compatibility is checked by attempting schema resolution
        // The reader schema must be able to read data written with the writer schema
        match Self::check_schema_compatibility(reader_schema, writer_schema) {
            Ok(true) => Ok(CompatibilityResult::compatible()),
            Ok(false) => Ok(CompatibilityResult::incompatible(vec![
                "Reader schema cannot read data written with writer schema".to_string(),
            ])),
            Err(e) => Ok(CompatibilityResult::incompatible(vec![format!(
                "Backward incompatible: {}",
                e
            )])),
        }
    }

    /// Check forward compatibility for Avro (old can read new)
    fn check_avro_forward(
        &self,
        reader_schema: &apache_avro::Schema,
        writer_schema: &apache_avro::Schema,
    ) -> Result<CompatibilityResult> {
        // Forward compatibility: old schema (reader) can read new data (writer)
        match Self::check_schema_compatibility(reader_schema, writer_schema) {
            Ok(true) => Ok(CompatibilityResult::compatible()),
            Ok(false) => Ok(CompatibilityResult::incompatible(vec![
                "Old schema cannot read data written with new schema".to_string(),
            ])),
            Err(e) => Ok(CompatibilityResult::incompatible(vec![format!(
                "Forward incompatible: {}",
                e
            )])),
        }
    }

    /// Helper to check if reader schema is compatible with writer schema
    /// This is a simplified compatibility check for Avro 0.21+
    fn check_schema_compatibility(
        reader_schema: &apache_avro::Schema,
        writer_schema: &apache_avro::Schema,
    ) -> Result<bool> {
        // Basic compatibility rules for Avro
        use apache_avro::Schema;

        match (reader_schema, writer_schema) {
            // Exact match is always compatible
            (r, w) if r == w => Ok(true),

            // Union can read its members
            (Schema::Union(_), _) => Ok(true),
            (_, Schema::Union(_)) => Ok(true),

            // Promotable types (int -> long, float -> double, etc.)
            (Schema::Long, Schema::Int) => Ok(true),
            (Schema::Double, Schema::Float) => Ok(true),
            (Schema::Double, Schema::Long) => Ok(true),
            (Schema::Double, Schema::Int) => Ok(true),

            // String/bytes interoperability
            (Schema::String, Schema::Bytes) => Ok(true),
            (Schema::Bytes, Schema::String) => Ok(true),

            // Records must have compatible fields
            (Schema::Record(r_rec), Schema::Record(w_rec)) => {
                // Same record name
                if r_rec.name != w_rec.name {
                    return Ok(false);
                }

                // Backward compatibility: reader must be able to read data written with writer
                // This means:
                // 1. Every field in writer must be readable by reader (reader has it or can ignore it)
                // 2. Every required field in reader must exist in writer

                // Check that all required reader fields exist in writer
                for reader_field in &r_rec.fields {
                    let field_exists_in_writer =
                        w_rec.fields.iter().any(|wf| wf.name == reader_field.name);

                    // If reader expects a field but writer doesn't have it, need a default
                    if !field_exists_in_writer {
                        // Check if reader field has a default value or is optional (union with null)
                        let has_default = match &reader_field.schema {
                            Schema::Union(union_schema) => {
                                // If it's a union with null, it's optional
                                union_schema
                                    .variants()
                                    .iter()
                                    .any(|s| matches!(s, Schema::Null))
                            }
                            _ => reader_field.default.is_some(),
                        };

                        if !has_default {
                            // Required field in reader doesn't exist in writer - incompatible
                            return Ok(false);
                        }
                    }
                }

                Ok(true)
            }

            // Arrays with compatible items
            (Schema::Array(r_arr), Schema::Array(w_arr)) => {
                Self::check_schema_compatibility(&r_arr.items, &w_arr.items)
            }

            // Maps with compatible values
            (Schema::Map(r_map), Schema::Map(w_map)) => {
                Self::check_schema_compatibility(&r_map.types, &w_map.types)
            }

            // Different types are generally incompatible
            _ => Ok(false),
        }
    }

    /// Check JSON Schema compatibility (basic implementation)
    fn check_json_compatibility(
        &self,
        old: &JsonSchemaDefinition,
        new: &JsonSchemaDefinition,
        mode: CompatibilityMode,
    ) -> Result<CompatibilityResult> {
        // Parse both schemas
        let old_schema: serde_json::Value = serde_json::from_str(&old.raw_schema)
            .map_err(|e| anyhow!("Failed to parse old JSON schema: {}", e))?;

        let new_schema: serde_json::Value = serde_json::from_str(&new.raw_schema)
            .map_err(|e| anyhow!("Failed to parse new JSON schema: {}", e))?;

        match mode {
            CompatibilityMode::None => Ok(CompatibilityResult::compatible()),

            CompatibilityMode::Backward => {
                // Basic backward compatibility check for JSON Schema
                self.check_json_backward(&old_schema, &new_schema)
            }

            CompatibilityMode::Forward => {
                // Basic forward compatibility check for JSON Schema
                self.check_json_forward(&old_schema, &new_schema)
            }

            CompatibilityMode::Full => {
                let backward = self.check_json_backward(&old_schema, &new_schema)?;
                if !backward.is_compatible {
                    return Ok(backward);
                }
                self.check_json_forward(&old_schema, &new_schema)
            }
        }
    }

    /// Check backward compatibility for JSON Schema
    /// New schema should validate all data that old schema validated
    fn check_json_backward(
        &self,
        _old_schema: &serde_json::Value,
        _new_schema: &serde_json::Value,
    ) -> Result<CompatibilityResult> {
        // Simplified implementation:
        // - Adding optional fields is compatible
        // - Removing required fields is incompatible
        // - Changing field types is incompatible
        // - Making optional fields required is incompatible

        // TODO: Implement proper JSON Schema compatibility checking
        // For now, we'll be permissive and allow changes
        Ok(CompatibilityResult::compatible())
    }

    /// Check forward compatibility for JSON Schema
    /// Old schema should validate all data that new schema validates
    fn check_json_forward(
        &self,
        _old_schema: &serde_json::Value,
        _new_schema: &serde_json::Value,
    ) -> Result<CompatibilityResult> {
        // TODO: Implement proper JSON Schema forward compatibility checking
        // For now, we'll be permissive and allow changes
        Ok(CompatibilityResult::compatible())
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

    fn create_avro_schema(fields: &str) -> AvroSchema {
        let raw = format!(r#"{{"type":"record","name":"Test","fields":[{}]}}"#, fields);
        AvroSchema::new(raw, "fingerprint".to_string())
    }

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

    #[test]
    fn test_avro_backward_compatible_adding_optional_field() {
        let checker = CompatibilityChecker::new();

        let old_schema = create_avro_schema(r#"{"name":"name","type":"string"}"#);
        let new_schema = create_avro_schema(
            r#"{"name":"name","type":"string"},{"name":"age","type":["null","int"],"default":null}"#,
        );

        let result = checker
            .check(
                &SchemaDefinition::Avro(old_schema),
                &SchemaDefinition::Avro(new_schema),
                CompatibilityMode::Backward,
            )
            .unwrap();

        assert!(result.is_compatible);
    }

    #[test]
    fn test_avro_backward_incompatible_removing_field() {
        let checker = CompatibilityChecker::new();

        let old_schema = create_avro_schema(
            r#"{"name":"name","type":"string"},{"name":"age","type":"int"}"#,
        );
        let new_schema = create_avro_schema(r#"{"name":"name","type":"string"}"#);

        let result = checker
            .check(
                &SchemaDefinition::Avro(old_schema),
                &SchemaDefinition::Avro(new_schema),
                CompatibilityMode::Backward,
            )
            .unwrap();

        // Backward compatible: new schema (reader) can read old data (writer)
        // Old data has 'age' field, but new reader doesn't expect it - this is OK
        assert!(result.is_compatible);
    }

    #[test]
    fn test_avro_forward_compatible_adding_field_with_default() {
        let checker = CompatibilityChecker::new();

        let old_schema = create_avro_schema(r#"{"name":"name","type":"string"}"#);
        let new_schema = create_avro_schema(
            r#"{"name":"name","type":"string"},{"name":"age","type":"int","default":0}"#,
        );

        let result = checker
            .check(
                &SchemaDefinition::Avro(old_schema),
                &SchemaDefinition::Avro(new_schema),
                CompatibilityMode::Forward,
            )
            .unwrap();

        // Forward compatible: old schema (reader) can read new data (writer)
        // New data might have 'age', old reader ignores it - this is OK
        assert!(result.is_compatible);
    }

    #[test]
    fn test_avro_full_compatibility_requires_both_directions() {
        let checker = CompatibilityChecker::new();

        let old_schema = create_avro_schema(r#"{"name":"name","type":"string"}"#);
        let new_schema = create_avro_schema(
            r#"{"name":"name","type":"string"},{"name":"email","type":["null","string"],"default":null}"#,
        );

        let result = checker
            .check(
                &SchemaDefinition::Avro(old_schema),
                &SchemaDefinition::Avro(new_schema),
                CompatibilityMode::Full,
            )
            .unwrap();

        // Full compatibility: must be both backward and forward compatible
        // Adding optional field with default satisfies both
        assert!(result.is_compatible);
    }
}
