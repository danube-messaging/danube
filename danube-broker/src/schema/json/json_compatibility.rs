use crate::schema::metadata::{CompatibilityResult, JsonSchemaDefinition};
use crate::schema::types::CompatibilityMode;
use anyhow::{anyhow, Result};

/// JSON Schema-specific compatibility checker
#[derive(Debug)]
pub struct JsonCompatibilityChecker;

impl JsonCompatibilityChecker {
    pub fn new() -> Self {
        Self
    }

    /// Check JSON Schema compatibility
    pub fn check(
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
                self.check_backward(&old_schema, &new_schema)
            }

            CompatibilityMode::Forward => {
                // Basic forward compatibility check for JSON Schema
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

    /// Check backward compatibility for JSON Schema
    /// New schema (reader) should be able to read data written with old schema (writer)
    /// 
    /// Rules:
    /// - Can add optional fields (new reader can ignore missing fields from old data)
    /// - Cannot remove required fields (new reader expects them, old data won't have them)
    /// - Cannot change field types (incompatible data)
    /// - Cannot make optional fields required (old data may not have them)
    fn check_backward(
        &self,
        old_schema: &serde_json::Value,
        new_schema: &serde_json::Value,
    ) -> Result<CompatibilityResult> {
        let mut errors = Vec::new();

        // Extract properties and required fields
        let old_props = old_schema.get("properties").and_then(|p| p.as_object());
        let new_props = new_schema.get("properties").and_then(|p| p.as_object());
        let old_required = Self::extract_required(old_schema);
        let new_required = Self::extract_required(new_schema);

        // Check 1: All old required fields must still exist in new schema
        for field in &old_required {
            if !new_props.map_or(false, |p| p.contains_key(field)) {
                errors.push(format!(
                    "Backward incompatible: removed required field '{}'",
                    field
                ));
            }
        }

        // Check 2: Fields that were optional cannot become required
        // (unless they didn't exist before, in which case it's adding a new required field)
        for field in &new_required {
            let was_optional = old_props.map_or(false, |p| p.contains_key(field))
                && !old_required.contains(field);
            if was_optional {
                errors.push(format!(
                    "Backward incompatible: made optional field '{}' required",
                    field
                ));
            }
        }

        // Check 3: Field types must not change for existing fields
        if let (Some(old_props), Some(new_props)) = (old_props, new_props) {
            for (field_name, old_prop) in old_props {
                if let Some(new_prop) = new_props.get(field_name) {
                    if !Self::types_compatible(old_prop, new_prop) {
                        errors.push(format!(
                            "Backward incompatible: changed type of field '{}'",
                            field_name
                        ));
                    }
                }
            }
        }

        if errors.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(errors))
        }
    }

    /// Check forward compatibility for JSON Schema
    /// Old schema (reader) should be able to read data written with new schema (writer)
    /// 
    /// Rules:
    /// - Can remove optional fields (old reader can ignore extra fields)
    /// - Cannot add new required fields (old reader doesn't expect them)
    /// - Cannot change field types (incompatible data)
    fn check_forward(
        &self,
        old_schema: &serde_json::Value,
        new_schema: &serde_json::Value,
    ) -> Result<CompatibilityResult> {
        let mut errors = Vec::new();

        // Extract properties and required fields
        let old_props = old_schema.get("properties").and_then(|p| p.as_object());
        let new_props = new_schema.get("properties").and_then(|p| p.as_object());
        let _old_required = Self::extract_required(old_schema);
        let new_required = Self::extract_required(new_schema);

        // Check 1: Cannot add new required fields
        // (old reader doesn't know about them and will fail validation)
        for field in &new_required {
            let is_new_field = !old_props.map_or(false, |p| p.contains_key(field));
            if is_new_field {
                errors.push(format!(
                    "Forward incompatible: added new required field '{}'",
                    field
                ));
            }
        }

        // Check 2: Field types must not change for fields that exist in both
        if let (Some(old_props), Some(new_props)) = (old_props, new_props) {
            for (field_name, old_prop) in old_props {
                if let Some(new_prop) = new_props.get(field_name) {
                    if !Self::types_compatible(old_prop, new_prop) {
                        errors.push(format!(
                            "Forward incompatible: changed type of field '{}'",
                            field_name
                        ));
                    }
                }
            }
        }

        if errors.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(errors))
        }
    }

    /// Extract required fields from a JSON Schema
    fn extract_required(schema: &serde_json::Value) -> Vec<String> {
        schema
            .get("required")
            .and_then(|r| r.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if two field type definitions are compatible
    /// For simplicity, we check if the "type" field is the same
    fn types_compatible(old_type: &serde_json::Value, new_type: &serde_json::Value) -> bool {
        // Extract type from both schemas
        let old_type_val = old_type.get("type");
        let new_type_val = new_type.get("type");

        match (old_type_val, new_type_val) {
            (Some(old_t), Some(new_t)) => old_t == new_t,
            // If type is not specified, consider compatible (permissive)
            _ => true,
        }
    }
}

impl Default for JsonCompatibilityChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::metadata::JsonSchemaDefinition;

    #[test]
    fn test_json_none_mode_always_compatible() {
        let checker = JsonCompatibilityChecker::new();

        let old = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"name": {"type": "string"}}}"#.to_string(),
            "fp1".to_string(),
        );

        let new = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"age": {"type": "integer"}}}"#.to_string(),
            "fp2".to_string(),
        );

        let result = checker.check(&old, &new, CompatibilityMode::None).unwrap();
        assert!(result.is_compatible);
    }

    #[test]
    fn test_json_backward_adding_optional_field() {
        let checker = JsonCompatibilityChecker::new();

        let old = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}"#.to_string(),
            "fp1".to_string(),
        );

        let new = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"name": {"type": "string"}, "email": {"type": "string"}}, "required": ["name"]}"#.to_string(),
            "fp2".to_string(),
        );

        let result = checker.check(&old, &new, CompatibilityMode::Backward).unwrap();
        assert!(result.is_compatible, "Adding optional field should be backward compatible");
    }

    #[test]
    fn test_json_backward_removing_required_field_fails() {
        let checker = JsonCompatibilityChecker::new();

        let old = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}, "required": ["name", "age"]}"#.to_string(),
            "fp1".to_string(),
        );

        let new = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}"#.to_string(),
            "fp2".to_string(),
        );

        let result = checker.check(&old, &new, CompatibilityMode::Backward).unwrap();
        assert!(!result.is_compatible, "Removing required field should fail backward compatibility");
        assert!(result.errors.iter().any(|e| e.contains("removed required field 'age'")));
    }

    #[test]
    fn test_json_backward_making_optional_required_fails() {
        let checker = JsonCompatibilityChecker::new();

        let old = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}, "required": ["name"]}"#.to_string(),
            "fp1".to_string(),
        );

        let new = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}, "required": ["name", "age"]}"#.to_string(),
            "fp2".to_string(),
        );

        let result = checker.check(&old, &new, CompatibilityMode::Backward).unwrap();
        assert!(!result.is_compatible, "Making optional field required should fail backward compatibility");
        assert!(result.errors.iter().any(|e| e.contains("made optional field 'age' required")));
    }

    #[test]
    fn test_json_backward_changing_field_type_fails() {
        let checker = JsonCompatibilityChecker::new();

        let old = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}"#.to_string(),
            "fp1".to_string(),
        );

        let new = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]}"#.to_string(),
            "fp2".to_string(),
        );

        let result = checker.check(&old, &new, CompatibilityMode::Backward).unwrap();
        assert!(!result.is_compatible, "Changing field type should fail backward compatibility");
        assert!(result.errors.iter().any(|e| e.contains("changed type of field 'id'")));
    }

    #[test]
    fn test_json_forward_removing_optional_field() {
        let checker = JsonCompatibilityChecker::new();

        let old = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"name": {"type": "string"}, "nickname": {"type": "string"}}, "required": ["name"]}"#.to_string(),
            "fp1".to_string(),
        );

        let new = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}"#.to_string(),
            "fp2".to_string(),
        );

        let result = checker.check(&old, &new, CompatibilityMode::Forward).unwrap();
        assert!(result.is_compatible, "Removing optional field should be forward compatible");
    }

    #[test]
    fn test_json_forward_adding_required_field_fails() {
        let checker = JsonCompatibilityChecker::new();

        let old = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}"#.to_string(),
            "fp1".to_string(),
        );

        let new = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"name": {"type": "string"}, "email": {"type": "string"}}, "required": ["name", "email"]}"#.to_string(),
            "fp2".to_string(),
        );

        let result = checker.check(&old, &new, CompatibilityMode::Forward).unwrap();
        assert!(!result.is_compatible, "Adding required field should fail forward compatibility");
        assert!(result.errors.iter().any(|e| e.contains("added new required field 'email'")));
    }

    #[test]
    fn test_json_full_compatibility_adding_optional_field() {
        let checker = JsonCompatibilityChecker::new();

        let old = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}, "required": ["id", "name"]}"#.to_string(),
            "fp1".to_string(),
        );

        let new = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}, "description": {"type": "string"}}, "required": ["id", "name"]}"#.to_string(),
            "fp2".to_string(),
        );

        let result = checker.check(&old, &new, CompatibilityMode::Full).unwrap();
        assert!(result.is_compatible, "Adding optional field should pass full compatibility");
    }

    #[test]
    fn test_json_full_compatibility_removing_required_field_fails() {
        let checker = JsonCompatibilityChecker::new();

        let old = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}, "required": ["id", "name"]}"#.to_string(),
            "fp1".to_string(),
        );

        let new = JsonSchemaDefinition::new(
            r#"{"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]}"#.to_string(),
            "fp2".to_string(),
        );

        let result = checker.check(&old, &new, CompatibilityMode::Full).unwrap();
        assert!(!result.is_compatible, "Removing required field should fail full compatibility");
    }
}
