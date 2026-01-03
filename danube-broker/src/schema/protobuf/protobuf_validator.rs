use crate::schema::validator::PayloadValidator;
use anyhow::Result;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protobuf_validator_creation() {
        let validator = ProtobufValidator::new(
            "syntax = \"proto3\"; message Test {}".to_string(),
            "Test".to_string(),
        );
        assert!(validator.is_ok());
    }
}
