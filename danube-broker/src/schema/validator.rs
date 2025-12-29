use crate::schema::metadata::SchemaDefinition;
use anyhow::{anyhow, Result};
use std::fmt;

/// Trait for validating message payloads against schema definitions
/// This validates the actual message content, not the schema itself
pub trait PayloadValidator: Send + Sync + fmt::Debug {
    /// Validate message payload data against the schema
    fn validate(&self, payload: &[u8]) -> Result<()>;

    /// Get the schema type name
    #[allow(dead_code)]
    fn schema_type(&self) -> &str;

    /// Get a human-readable description of the validator
    #[allow(dead_code)]
    fn description(&self) -> String {
        format!("{} payload validator", self.schema_type())
    }
}

/// No-op validator for bytes schema
#[derive(Debug)]
pub struct BytesValidator;

impl PayloadValidator for BytesValidator {
    fn validate(&self, _payload: &[u8]) -> Result<()> {
        // Bytes schema accepts any data
        Ok(())
    }

    fn schema_type(&self) -> &str {
        "bytes"
    }
}

/// String validator (UTF-8 validation)
#[derive(Debug)]
pub struct StringValidator;

impl PayloadValidator for StringValidator {
    fn validate(&self, payload: &[u8]) -> Result<()> {
        std::str::from_utf8(payload).map_err(|e| anyhow!("Invalid UTF-8 string: {}", e))?;
        Ok(())
    }

    fn schema_type(&self) -> &str {
        "string"
    }
}

/// Number validator (validates numeric data - int, long, float, double)
#[derive(Debug)]
pub struct NumberValidator;

impl PayloadValidator for NumberValidator {
    fn validate(&self, data: &[u8]) -> Result<()> {
        // Accept 4 bytes (int32/float) or 8 bytes (int64/double)
        match data.len() {
            4 | 8 => Ok(()),
            _ => Err(anyhow!(
                "Invalid number: expected 4 or 8 bytes, got {}",
                data.len()
            )),
        }
    }

    fn schema_type(&self) -> &str {
        "number"
    }
}

/// Avro validator (validates against Avro schema)
#[derive(Debug)]
pub struct AvroValidator {
    schema: apache_avro::Schema,
    #[allow(dead_code)]
    raw_schema: String,
}

impl AvroValidator {
    pub fn new(raw_schema: String) -> Result<Self> {
        let schema = apache_avro::Schema::parse_str(&raw_schema)
            .map_err(|e| anyhow!("Failed to parse Avro schema: {}", e))?;

        Ok(Self { schema, raw_schema })
    }

    #[allow(dead_code)]
    pub fn schema(&self) -> &apache_avro::Schema {
        &self.schema
    }
}

impl PayloadValidator for AvroValidator {
    fn validate(&self, data: &[u8]) -> Result<()> {
        // Try to decode the data using the Avro schema
        let reader = apache_avro::Reader::with_schema(&self.schema, data)
            .map_err(|e| anyhow!("Avro validation failed: {}", e))?;

        // Attempt to read at least one value to ensure it's valid
        for value in reader {
            match value {
                Ok(_) => return Ok(()),
                Err(e) => return Err(anyhow!("Avro validation failed: {}", e)),
            }
        }

        Err(anyhow!("Avro validation failed: no data found"))
    }

    fn schema_type(&self) -> &str {
        "avro"
    }

    fn description(&self) -> String {
        format!(
            "Avro validator: {}",
            self.raw_schema.chars().take(100).collect::<String>()
        )
    }
}

/// JSON Schema validator
#[derive(Debug)]
pub struct JsonSchemaValidator {
    validator: jsonschema::Validator,
    #[allow(dead_code)]
    raw_schema: String,
}

impl JsonSchemaValidator {
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

impl PayloadValidator for JsonSchemaValidator {
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

/// Protobuf validator (future implementation)
#[derive(Debug)]
pub struct ProtobufValidator {
    #[allow(dead_code)]
    message_name: String,
}

impl ProtobufValidator {
    pub fn new(_raw_proto: String, message_name: String) -> Result<Self> {
        // TODO: Implement protobuf validation using prost-reflect
        Ok(Self { message_name })
    }
}

impl PayloadValidator for ProtobufValidator {
    fn validate(&self, _data: &[u8]) -> Result<()> {
        // TODO: Implement actual protobuf validation
        unimplemented!("Protobuf validation not yet implemented")
    }

    fn schema_type(&self) -> &str {
        "protobuf"
    }

    fn description(&self) -> String {
        format!("Protobuf validator: {}", self.message_name)
    }
}

/// Factory for creating validators from schema definitions
pub struct ValidatorFactory;

impl ValidatorFactory {
    /// Create a payload validator for the given schema definition
    pub fn create(schema_def: &SchemaDefinition) -> Result<Box<dyn PayloadValidator>> {
        match schema_def {
            SchemaDefinition::Bytes => Ok(Box::new(BytesValidator)),
            SchemaDefinition::String => Ok(Box::new(StringValidator)),
            SchemaDefinition::Number => Ok(Box::new(NumberValidator)),
            SchemaDefinition::Avro(avro) => {
                let validator = AvroValidator::new(avro.raw_schema.clone())?;
                Ok(Box::new(validator))
            }
            SchemaDefinition::JsonSchema(json) => {
                let validator = JsonSchemaValidator::new(json.raw_schema.clone())?;
                Ok(Box::new(validator))
            }
            SchemaDefinition::Protobuf(proto) => {
                let validator =
                    ProtobufValidator::new(proto.raw_proto.clone(), proto.message_name.clone())?;
                Ok(Box::new(validator))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_validator() {
        let validator = BytesValidator;
        assert!(validator.validate(b"any data").is_ok());
        assert!(validator.validate(&[0, 1, 2, 3]).is_ok());
    }

    #[test]
    fn test_string_validator() {
        let validator = StringValidator;
        assert!(validator.validate(b"valid utf-8").is_ok());
        assert!(validator.validate("hello".as_bytes()).is_ok());
        assert!(validator.validate(&[0xFF, 0xFE]).is_err()); // Invalid UTF-8
    }

    #[test]
    fn test_number_validator() {
        let validator = NumberValidator;
        assert!(validator.validate(&[0u8; 4]).is_ok()); // int32/float
        assert!(validator.validate(&[0u8; 8]).is_ok()); // int64/double
        assert!(validator.validate(&[0u8; 2]).is_err());
        assert!(validator.validate(&[0u8; 9]).is_err());
    }

    #[test]
    fn test_json_schema_validator() {
        let schema = r#"{"type": "object", "properties": {"name": {"type": "string"}}}"#;
        let validator = JsonSchemaValidator::new(schema.to_string()).unwrap();

        let valid_data = br#"{"name": "John"}"#;
        assert!(validator.validate(valid_data).is_ok());

        let invalid_data = br#"{"name": 123}"#;
        assert!(validator.validate(invalid_data).is_err());
    }

    #[test]
    fn test_validator_factory() {
        let schema_def = SchemaDefinition::String;
        let validator = ValidatorFactory::create(&schema_def).unwrap();
        assert_eq!(validator.schema_type(), "string");
    }
}
