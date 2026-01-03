use anyhow::{Context, Result};
use danube_client::SchemaType;
use std::fs;
use std::path::Path;

/// Load schema file from disk
pub fn load_schema_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    fs::read(path.as_ref())
        .with_context(|| format!("Failed to read schema file: {:?}", path.as_ref()))
}

/// Validate schema format based on schema type
pub fn validate_schema_format(data: &[u8], schema_type: SchemaType) -> Result<()> {
    match schema_type {
        SchemaType::JsonSchema => {
            serde_json::from_slice::<serde_json::Value>(data)
                .context("Invalid JSON schema format")?;
        }
        SchemaType::Avro => {
            serde_json::from_slice::<serde_json::Value>(data)
                .context("Invalid Avro schema format (must be valid JSON)")?;
        }
        SchemaType::Protobuf => {
            String::from_utf8(data.to_vec())
                .context("Invalid Protobuf schema (must be UTF-8 text)")?;
        }
        _ => {
            String::from_utf8(data.to_vec())
                .context("Schema data must be valid UTF-8")?;
        }
    }
    Ok(())
}

/// Pretty-print schema definition
#[allow(dead_code)]
pub fn format_schema(data: &[u8], schema_type: SchemaType) -> Result<String> {
    match schema_type {
        SchemaType::JsonSchema | SchemaType::Avro => {
            let json_value: serde_json::Value = serde_json::from_slice(data)?;
            Ok(serde_json::to_string_pretty(&json_value)?)
        }
        _ => Ok(String::from_utf8_lossy(data).to_string()),
    }
}

/// Check if a file appears to be a valid schema based on extension
#[allow(dead_code)]
pub fn guess_schema_type<P: AsRef<Path>>(path: P) -> Option<SchemaType> {
    path.as_ref()
        .extension()
        .and_then(|ext| ext.to_str())
        .and_then(|ext| match ext.to_lowercase().as_str() {
            "json" => Some(SchemaType::JsonSchema),
            "avsc" => Some(SchemaType::Avro),
            "proto" => Some(SchemaType::Protobuf),
            _ => None,
        })
}
