use crate::avro::AvroValidator;
use crate::json::JsonValidator;
use crate::metadata::SchemaDefinition;
use crate::protobuf::ProtobufValidator;
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
    fn validate(&self, _payload: &[u8]) -> Result<()> { Ok(()) }
    fn schema_type(&self) -> &str { "bytes" }
}

/// String validator (UTF-8 validation)
#[derive(Debug)]
pub struct StringValidator;

impl PayloadValidator for StringValidator {
    fn validate(&self, payload: &[u8]) -> Result<()> {
        std::str::from_utf8(payload).map_err(|e| anyhow!("Invalid UTF-8 string: {}", e))?;
        Ok(())
    }
    fn schema_type(&self) -> &str { "string" }
}

/// Number validator (validates numeric data - int, long, float, double)
#[derive(Debug)]
pub struct NumberValidator;

impl PayloadValidator for NumberValidator {
    fn validate(&self, data: &[u8]) -> Result<()> {
        match data.len() {
            4 | 8 => Ok(()),
            _ => Err(anyhow!("Invalid number: expected 4 or 8 bytes, got {}", data.len())),
        }
    }
    fn schema_type(&self) -> &str { "number" }
}

/// Factory for creating validators from schema definitions
pub struct ValidatorFactory;

impl ValidatorFactory {
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
                let validator = JsonValidator::new(json.raw_schema.clone())?;
                Ok(Box::new(validator))
            }
            SchemaDefinition::Protobuf(proto) => {
                let validator = ProtobufValidator::new(proto.raw_proto.clone(), proto.message_name.clone())?;
                Ok(Box::new(validator))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{AvroSchema, JsonSchemaDefinition};

    #[test]
    fn test_string_validator_rejects_invalid_utf8() {
        let validator = StringValidator;
        assert!(validator.validate(b"valid utf-8").is_ok());
        assert!(validator.validate("hello".as_bytes()).is_ok());
        assert!(validator.validate(&[0xFF, 0xFE]).is_err());
    }

    #[test]
    fn test_number_validator_enforces_size() {
        let validator = NumberValidator;
        assert!(validator.validate(&[0u8; 4]).is_ok());
        assert!(validator.validate(&[0u8; 8]).is_ok());
        assert!(validator.validate(&[0u8; 2]).is_err());
        assert!(validator.validate(&[0u8; 9]).is_err());
    }

    #[test]
    fn test_validator_factory_creates_correct_validators() {
        let string_def = SchemaDefinition::String;
        let validator = ValidatorFactory::create(&string_def).unwrap();
        assert_eq!(validator.schema_type(), "string");

        let json_def = SchemaDefinition::JsonSchema(JsonSchemaDefinition::new(
            r#"{"type": "string"}"#.to_string(), "fp".to_string(),
        ));
        let validator = ValidatorFactory::create(&json_def).unwrap();
        assert_eq!(validator.schema_type(), "json_schema");

        let avro_def = SchemaDefinition::Avro(AvroSchema::new(
            r#"{"type": "string"}"#.to_string(), "fp".to_string(),
        ));
        let validator = ValidatorFactory::create(&avro_def).unwrap();
        assert_eq!(validator.schema_type(), "avro");
    }
}
