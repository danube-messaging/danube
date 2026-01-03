use crate::schema::metadata::JsonSchemaDefinition;
use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};

/// Handler for JSON schemas
pub struct JsonHandler;

impl JsonHandler {
    /// Parse and validate a JSON Schema from raw bytes
    pub fn parse(raw_schema_bytes: &[u8]) -> Result<JsonSchemaDefinition> {
        // Convert bytes to string
        let raw_schema = std::str::from_utf8(raw_schema_bytes)
            .map_err(|e| anyhow!("Invalid UTF-8 in JSON schema: {}", e))?
            .to_string();

        // Parse as JSON to validate structure
        let schema_value: serde_json::Value = serde_json::from_str(&raw_schema)
            .map_err(|e| anyhow!("Invalid JSON in schema: {}", e))?;

        // Compile the JSON schema validator to ensure it's valid
        jsonschema::validator_for(&schema_value)
            .map_err(|e| anyhow!("Invalid JSON Schema: {}", e))?;

        // Compute fingerprint
        let fingerprint = Self::compute_fingerprint(&raw_schema);

        Ok(JsonSchemaDefinition::new(raw_schema, fingerprint))
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

    /// Canonicalize a JSON Schema (parse and re-serialize in consistent format)
    fn canonicalize(raw_schema: &str) -> Result<String> {
        let value: serde_json::Value = serde_json::from_str(raw_schema)
            .map_err(|e| anyhow!("Failed to parse JSON for canonicalization: {}", e))?;

        // Serialize to compact canonical form (no extra whitespace)
        serde_json::to_string(&value).map_err(|e| anyhow!("Failed to serialize JSON: {}", e))
    }

    /// Validate that a schema string is valid JSON Schema
    #[allow(dead_code)]
    pub fn validate(raw_schema: &str) -> Result<()> {
        let schema_value: serde_json::Value =
            serde_json::from_str(raw_schema).map_err(|e| anyhow!("Invalid JSON: {}", e))?;

        jsonschema::validator_for(&schema_value)
            .map_err(|e| anyhow!("Invalid JSON Schema: {}", e))?;

        Ok(())
    }

    /// Extract title from JSON Schema if present
    #[allow(dead_code)]
    pub fn extract_title(schema: &JsonSchemaDefinition) -> Option<String> {
        let value: serde_json::Value = serde_json::from_str(&schema.raw_schema).ok()?;
        value.get("title")?.as_str().map(|s| s.to_string())
    }

    /// Extract description from JSON Schema if present
    #[allow(dead_code)]
    pub fn extract_description(schema: &JsonSchemaDefinition) -> Option<String> {
        let value: serde_json::Value = serde_json::from_str(&schema.raw_schema).ok()?;
        value.get("description")?.as_str().map(|s| s.to_string())
    }

    /// Get the JSON Schema draft version if specified
    #[allow(dead_code)]
    pub fn get_draft_version(schema: &JsonSchemaDefinition) -> Option<String> {
        let value: serde_json::Value = serde_json::from_str(&schema.raw_schema).ok()?;
        value.get("$schema")?.as_str().map(|s| s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_JSON_SCHEMA: &str = r#"{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "User",
        "type": "object",
        "properties": {
            "name": {
                "type": "string"
            },
            "age": {
                "type": "integer",
                "minimum": 0
            }
        },
        "required": ["name"]
    }"#;

    const MALFORMED_JSON: &str = r#"{
        "type": "object",
        "properties": {
    }"#;

    #[test]
    fn test_parse_valid_schema_generates_fingerprint() {
        let result = JsonHandler::parse(VALID_JSON_SCHEMA.as_bytes());
        assert!(result.is_ok());

        let schema = result.unwrap();
        assert!(!schema.fingerprint.is_empty());
        assert!(schema.fingerprint.starts_with("sha256:"));
        assert!(!schema.raw_schema.is_empty());
    }

    #[test]
    fn test_parse_malformed_json_returns_error() {
        let result = JsonHandler::parse(MALFORMED_JSON.as_bytes());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid JSON"));
    }

    #[test]
    fn test_fingerprint_consistency_for_deduplication() {
        // Critical: Same schema must always produce same fingerprint for deduplication
        let fp1 = JsonHandler::compute_fingerprint(VALID_JSON_SCHEMA);
        let fp2 = JsonHandler::compute_fingerprint(VALID_JSON_SCHEMA);
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_different_schemas_have_different_fingerprints() {
        // Critical: Different schemas must have different fingerprints
        let schema1 = r#"{"type": "object", "properties": {"x": {"type": "integer"}}}"#;
        let schema2 = r#"{"type": "object", "properties": {"y": {"type": "string"}}}"#;

        let fp1 = JsonHandler::compute_fingerprint(schema1);
        let fp2 = JsonHandler::compute_fingerprint(schema2);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_canonicalization_normalizes_whitespace() {
        // Critical: Different formatting should produce same fingerprint after canonicalization
        let schema_pretty = r#"{
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                }
            }
        }"#;

        let schema_compact = r#"{"type":"object","properties":{"name":{"type":"string"}}}"#;

        let fp1 = JsonHandler::compute_fingerprint(schema_pretty);
        let fp2 = JsonHandler::compute_fingerprint(schema_compact);

        assert_eq!(fp1, fp2);
    }
}
