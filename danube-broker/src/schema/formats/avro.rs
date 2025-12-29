use crate::schema::metadata::AvroSchema;
use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};

/// Handler for Avro schemas
pub struct AvroSchemaHandler;

impl AvroSchemaHandler {
    /// Parse and validate an Avro schema from raw bytes
    pub fn parse(raw_schema_bytes: &[u8]) -> Result<AvroSchema> {
        // Convert bytes to string
        let raw_schema = std::str::from_utf8(raw_schema_bytes)
            .map_err(|e| anyhow!("Invalid UTF-8 in Avro schema: {}", e))?
            .to_string();

        // Parse the schema to validate it
        apache_avro::Schema::parse_str(&raw_schema)
            .map_err(|e| anyhow!("Invalid Avro schema: {}", e))?;

        // Compute fingerprint
        let fingerprint = Self::compute_fingerprint(&raw_schema);

        Ok(AvroSchema::new(raw_schema, fingerprint))
    }

    /// Compute SHA-256 fingerprint of a schema
    pub fn compute_fingerprint(raw_schema: &str) -> String {
        // Normalize the schema by parsing and re-serializing to canonical form
        let canonical = Self::canonicalize(raw_schema).unwrap_or_else(|_| raw_schema.to_string());

        let mut hasher = Sha256::new();
        hasher.update(canonical.as_bytes());
        let result = hasher.finalize();
        format!("sha256:{}", hex::encode(result))
    }

    /// Canonicalize an Avro schema (parse and re-serialize)
    fn canonicalize(raw_schema: &str) -> Result<String> {
        let schema = apache_avro::Schema::parse_str(raw_schema)
            .map_err(|e| anyhow!("Failed to parse schema for canonicalization: {}", e))?;

        // Serialize to canonical JSON form
        let value = serde_json::to_value(&schema)
            .map_err(|e| anyhow!("Failed to serialize schema: {}", e))?;

        // Pretty print for consistency
        serde_json::to_string(&value).map_err(|e| anyhow!("Failed to convert to string: {}", e))
    }

    /// Validate that a schema string is valid Avro
    #[allow(dead_code)]
    pub fn validate(raw_schema: &str) -> Result<()> {
        apache_avro::Schema::parse_str(raw_schema)
            .map_err(|e| anyhow!("Invalid Avro schema: {}", e))?;
        Ok(())
    }

    /// Extract schema name from Avro schema
    #[allow(dead_code)]
    pub fn extract_name(schema: &AvroSchema) -> Option<String> {
        let parsed = apache_avro::Schema::parse_str(&schema.raw_schema).ok()?;

        match parsed {
            apache_avro::Schema::Record(record) => Some(record.name.fullname(None)),
            apache_avro::Schema::Enum(enum_schema) => Some(enum_schema.name.fullname(None)),
            apache_avro::Schema::Fixed(fixed) => Some(fixed.name.fullname(None)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_AVRO_SCHEMA: &str = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }"#;

    const INVALID_AVRO_SCHEMA: &str = r#"{
        "type": "invalid_type",
        "name": "User"
    }"#;

    #[test]
    fn test_parse_valid_schema() {
        let result = AvroSchemaHandler::parse(VALID_AVRO_SCHEMA.as_bytes());
        assert!(result.is_ok());

        let schema = result.unwrap();
        assert!(!schema.fingerprint.is_empty());
        assert!(schema.fingerprint.starts_with("sha256:"));
    }

    #[test]
    fn test_parse_invalid_schema() {
        let result = AvroSchemaHandler::parse(INVALID_AVRO_SCHEMA.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_schema() {
        assert!(AvroSchemaHandler::validate(VALID_AVRO_SCHEMA).is_ok());
        assert!(AvroSchemaHandler::validate(INVALID_AVRO_SCHEMA).is_err());
    }

    #[test]
    fn test_fingerprint_consistency() {
        let fp1 = AvroSchemaHandler::compute_fingerprint(VALID_AVRO_SCHEMA);
        let fp2 = AvroSchemaHandler::compute_fingerprint(VALID_AVRO_SCHEMA);
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_different_schemas_different_fingerprints() {
        let schema1 =
            r#"{"type": "record", "name": "A", "fields": [{"name": "x", "type": "int"}]}"#;
        let schema2 =
            r#"{"type": "record", "name": "B", "fields": [{"name": "y", "type": "string"}]}"#;

        let fp1 = AvroSchemaHandler::compute_fingerprint(schema1);
        let fp2 = AvroSchemaHandler::compute_fingerprint(schema2);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_extract_name() {
        let schema = AvroSchemaHandler::parse(VALID_AVRO_SCHEMA.as_bytes()).unwrap();
        let name = AvroSchemaHandler::extract_name(&schema);
        assert_eq!(name, Some("User".to_string()));
    }
}
