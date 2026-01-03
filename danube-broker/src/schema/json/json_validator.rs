use crate::schema::validator::PayloadValidator;
use anyhow::{anyhow, Result};

/// JSON Schema validator
#[derive(Debug)]
pub struct JsonValidator {
    validator: jsonschema::Validator,
    #[allow(dead_code)]
    raw_schema: String,
}

impl JsonValidator {
    pub fn new(raw_schema: String) -> Result<Self> {
        let schema_value: serde_json::Value = serde_json::from_str(&raw_schema)
            .map_err(|e| anyhow!("Failed to parse JSON schema: {}", e))?;

        let validator = jsonschema::validator_for(&schema_value)
            .map_err(|e| anyhow!("Failed to compile JSON schema: {}", e))?;

        Ok(Self {
            validator,
            raw_schema,
        })
    }
}

impl PayloadValidator for JsonValidator {
    fn validate(&self, data: &[u8]) -> Result<()> {
        // Parse the data as JSON
        let json_value: serde_json::Value =
            serde_json::from_slice(data).map_err(|e| anyhow!("Invalid JSON data: {}", e))?;

        // Validate against the schema
        if self.validator.is_valid(&json_value) {
            Ok(())
        } else {
            let errors: Vec<String> = self
                .validator
                .iter_errors(&json_value)
                .map(|e| e.to_string())
                .collect();
            Err(anyhow!(
                "JSON Schema validation failed: {}",
                errors.join(", ")
            ))
        }
    }

    fn schema_type(&self) -> &str {
        "json_schema"
    }

    fn description(&self) -> String {
        format!(
            "JSON Schema validator: {}",
            self.raw_schema.chars().take(100).collect::<String>()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_schema_validator_validates_structure() {
        let schema = r#"{"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}"#;
        let validator = JsonValidator::new(schema.to_string()).unwrap();

        let valid_data = br#"{"name": "John"}"#;
        assert!(validator.validate(valid_data).is_ok());

        let invalid_type = br#"{"name": 123}"#;
        assert!(validator.validate(invalid_type).is_err());

        let missing_required = br#"{"age": 30}"#;
        assert!(validator.validate(missing_required).is_err());
    }
}
