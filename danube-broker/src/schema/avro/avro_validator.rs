use crate::schema::validator::PayloadValidator;
use anyhow::{anyhow, Result};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avro_validator_with_record() {
        let schema = r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }"#;

        let validator = AvroValidator::new(schema.to_string()).unwrap();

        // Valid Avro binary data for the User record
        // This would need actual Avro-encoded data to test properly
        // For now, just verify validator creation works
        assert_eq!(validator.schema_type(), "avro");
    }
}
