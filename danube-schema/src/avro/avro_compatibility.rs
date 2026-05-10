use crate::metadata::{AvroSchema, CompatibilityResult};
use crate::types::CompatibilityMode;
use anyhow::{anyhow, Result};

/// Avro-specific compatibility checker
#[derive(Debug)]
pub struct AvroCompatibilityChecker;

impl AvroCompatibilityChecker {
    pub fn new() -> Self {
        Self
    }

    /// Check Avro schema compatibility
    pub fn check(
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
                self.check_backward(&old_schema, &new_schema)
            }

            CompatibilityMode::Forward => {
                self.check_forward(&old_schema, &new_schema)
            }

            CompatibilityMode::Full => {
                let backward = self.check_backward(&old_schema, &new_schema)?;
                if !backward.is_compatible {
                    return Ok(backward);
                }
                self.check_forward(&old_schema, &new_schema)
            }
        }
    }

    fn check_backward(
        &self,
        writer_schema: &apache_avro::Schema,
        reader_schema: &apache_avro::Schema,
    ) -> Result<CompatibilityResult> {
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

    fn check_forward(
        &self,
        reader_schema: &apache_avro::Schema,
        writer_schema: &apache_avro::Schema,
    ) -> Result<CompatibilityResult> {
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

    fn check_schema_compatibility(
        reader_schema: &apache_avro::Schema,
        writer_schema: &apache_avro::Schema,
    ) -> Result<bool> {
        use apache_avro::Schema;

        match (reader_schema, writer_schema) {
            (r, w) if r == w => Ok(true),
            (Schema::Union(_), _) => Ok(true),
            (_, Schema::Union(_)) => Ok(true),
            (Schema::Long, Schema::Int) => Ok(true),
            (Schema::Double, Schema::Float) => Ok(true),
            (Schema::Double, Schema::Long) => Ok(true),
            (Schema::Double, Schema::Int) => Ok(true),
            (Schema::String, Schema::Bytes) => Ok(true),
            (Schema::Bytes, Schema::String) => Ok(true),
            (Schema::Record(r_rec), Schema::Record(w_rec)) => {
                if r_rec.name != w_rec.name {
                    return Ok(false);
                }
                for reader_field in &r_rec.fields {
                    let field_exists_in_writer =
                        w_rec.fields.iter().any(|wf| wf.name == reader_field.name);
                    if !field_exists_in_writer {
                        let has_default = match &reader_field.schema {
                            Schema::Union(union_schema) => {
                                union_schema
                                    .variants()
                                    .iter()
                                    .any(|s| matches!(s, Schema::Null))
                            }
                            _ => reader_field.default.is_some(),
                        };
                        if !has_default {
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            }
            (Schema::Array(r_arr), Schema::Array(w_arr)) => {
                Self::check_schema_compatibility(&r_arr.items, &w_arr.items)
            }
            (Schema::Map(r_map), Schema::Map(w_map)) => {
                Self::check_schema_compatibility(&r_map.types, &w_map.types)
            }
            _ => Ok(false),
        }
    }
}

impl Default for AvroCompatibilityChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::AvroSchema;

    fn create_avro_schema(fields: &str) -> AvroSchema {
        let raw = format!(r#"{{"type":"record","name":"Test","fields":[{}]}}"#, fields);
        AvroSchema::new(raw, "fingerprint".to_string())
    }

    #[test]
    fn test_avro_backward_compatible_adding_optional_field() {
        let checker = AvroCompatibilityChecker::new();
        let old_schema = create_avro_schema(r#"{"name":"name","type":"string"}"#);
        let new_schema = create_avro_schema(
            r#"{"name":"name","type":"string"},{"name":"age","type":["null","int"],"default":null}"#,
        );
        let result = checker
            .check(&old_schema, &new_schema, CompatibilityMode::Backward)
            .unwrap();
        assert!(result.is_compatible);
    }

    #[test]
    fn test_avro_backward_incompatible_removing_field() {
        let checker = AvroCompatibilityChecker::new();
        let old_schema = create_avro_schema(
            r#"{"name":"name","type":"string"},{"name":"age","type":"int"}"#,
        );
        let new_schema = create_avro_schema(r#"{"name":"name","type":"string"}"#);
        let result = checker
            .check(&old_schema, &new_schema, CompatibilityMode::Backward)
            .unwrap();
        assert!(result.is_compatible);
    }

    #[test]
    fn test_avro_forward_compatible_adding_field_with_default() {
        let checker = AvroCompatibilityChecker::new();
        let old_schema = create_avro_schema(r#"{"name":"name","type":"string"}"#);
        let new_schema = create_avro_schema(
            r#"{"name":"name","type":"string"},{"name":"age","type":"int","default":0}"#,
        );
        let result = checker
            .check(&old_schema, &new_schema, CompatibilityMode::Forward)
            .unwrap();
        assert!(result.is_compatible);
    }

    #[test]
    fn test_avro_full_compatibility_requires_both_directions() {
        let checker = AvroCompatibilityChecker::new();
        let old_schema = create_avro_schema(r#"{"name":"name","type":"string"}"#);
        let new_schema = create_avro_schema(
            r#"{"name":"name","type":"string"},{"name":"email","type":["null","string"],"default":null}"#,
        );
        let result = checker
            .check(&old_schema, &new_schema, CompatibilityMode::Full)
            .unwrap();
        assert!(result.is_compatible);
    }
}
