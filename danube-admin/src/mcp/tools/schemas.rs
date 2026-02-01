//! Schema registry tools

use crate::core::AdminGrpcClient;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct RegisterSchemaParams {
    /// Schema subject name - identifies the schema across versions.
    /// Typically matches topic name or follows pattern "{topic}-key" or "{topic}-value".
    /// Example: "user-events", "user-events-value", "analytics-key"
    pub subject: String,

    /// Schema type determining the format and validation rules.
    /// Options: "json_schema" (JSON Schema draft-07), "avro" (Apache Avro),
    /// "protobuf" (Protocol Buffers), "string" (plain text), "bytes" (binary).
    /// Most common: "json_schema" for structured JSON data.
    pub schema_type: String,

    /// Schema definition as a string.
    /// For json_schema: Valid JSON Schema definition.
    /// For avro: Avro schema in JSON format.
    /// For protobuf: .proto file contents.
    /// Example (json_schema): '{"type":"object","properties":{"name":{"type":"string"}}}'
    pub schema_definition: String,

    /// Human-readable description explaining the schema's purpose and usage.
    /// Appears in schema registry listings for documentation.
    /// Example: "User profile update events schema v2"
    pub description: Option<String>,
}

pub async fn register_schema(
    client: &Arc<AdminGrpcClient>,
    params: RegisterSchemaParams,
) -> String {
    let req = danube_core::proto::danube_schema::RegisterSchemaRequest {
        subject: params.subject.clone(),
        schema_type: params.schema_type.clone(),
        schema_definition: params.schema_definition.into_bytes(),
        description: params.description.unwrap_or_default(),
        created_by: "mcp-server".to_string(),
        tags: vec![],
    };

    match client.register_schema(req).await {
        Ok(response) => {
            let status = if response.is_new_version {
                "New version registered"
            } else {
                "Schema already exists"
            };

            format!(
                "Schema '{}' - {}\n\
                 Schema ID: {}\n\
                 Version: {}\n\
                 Fingerprint: {}",
                params.subject, status, response.schema_id, response.version, response.fingerprint
            )
        }
        Err(e) => format!("Error registering schema: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GetSchemaParams {
    /// Schema subject name to retrieve.
    /// Returns the latest version of the schema.
    /// Example: "user-events", "user-events-value"
    pub subject: String,
}

pub async fn get_schema(client: &Arc<AdminGrpcClient>, params: GetSchemaParams) -> String {
    let req = danube_core::proto::danube_schema::GetLatestSchemaRequest {
        subject: params.subject.clone(),
    };

    match client.get_latest_schema(req).await {
        Ok(response) => {
            let schema_def = String::from_utf8_lossy(&response.schema_definition);

            format!(
                "Schema Subject: {}\n\
                 Schema ID: {}\n\
                 Version: {}\n\
                 Type: {}\n\
                 Compatibility: {}\n\
                 Created: {} (by {})\n\
                 Fingerprint: {}\n\n\
                 Definition:\n{}",
                response.subject,
                response.schema_id,
                response.version,
                response.schema_type,
                response.compatibility_mode,
                chrono::DateTime::from_timestamp(response.created_at as i64, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| "unknown".to_string()),
                response.created_by,
                response.fingerprint,
                schema_def
            )
        }
        Err(e) => format!("Error getting schema: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ListVersionsParams {
    /// Schema subject name to list versions for.
    /// Returns all registered versions in chronological order.
    /// Example: "user-events", "user-events-value"
    pub subject: String,
}

pub async fn list_schema_versions(
    client: &Arc<AdminGrpcClient>,
    params: ListVersionsParams,
) -> String {
    let req = danube_core::proto::danube_schema::ListVersionsRequest {
        subject: params.subject.clone(),
    };

    match client.list_versions(req).await {
        Ok(response) => {
            if response.versions.is_empty() {
                return format!("No versions found for subject '{}'", params.subject);
            }

            let mut output = format!(
                "Found {} version(s) for schema '{}':\n\n",
                response.versions.len(),
                params.subject
            );

            for version_info in &response.versions {
                output.push_str(&format!(
                    "Version {}: Schema ID {}\n\
                     Created: {} (by {})\n\
                     Description: {}\n\
                     Fingerprint: {}\n\n",
                    version_info.version,
                    version_info.schema_id,
                    chrono::DateTime::from_timestamp(version_info.created_at as i64, 0)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_else(|| "unknown".to_string()),
                    version_info.created_by,
                    version_info.description,
                    version_info.fingerprint
                ));
            }

            output
        }
        Err(e) => format!("Error listing versions: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CheckCompatibilityParams {
    /// Schema subject name to check compatibility against.
    /// The new schema will be validated against existing versions of this subject.
    /// Example: "user-events", "user-events-value"
    pub subject: String,

    /// Schema type of the new schema being validated.
    /// Must match the type of existing schemas in the subject.
    /// Options: "json_schema", "avro", "protobuf"
    pub schema_type: String,

    /// New schema definition to validate for compatibility.
    /// Should be the updated schema you want to register.
    /// Example: '{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}}}'
    pub schema_definition: String,
}

pub async fn check_compatibility(
    client: &Arc<AdminGrpcClient>,
    params: CheckCompatibilityParams,
) -> String {
    let req = danube_core::proto::danube_schema::CheckCompatibilityRequest {
        subject: params.subject.clone(),
        new_schema_definition: params.schema_definition.into_bytes(),
        schema_type: params.schema_type,
        compatibility_mode: None,
    };

    match client.check_compatibility(req).await {
        Ok(response) => {
            if response.is_compatible {
                format!("✓ Schema is COMPATIBLE with subject '{}'", params.subject)
            } else {
                let mut output = format!(
                    "✗ Schema is INCOMPATIBLE with subject '{}':\n\n",
                    params.subject
                );
                for (i, error) in response.errors.iter().enumerate() {
                    output.push_str(&format!("  {}. {}\n", i + 1, error));
                }
                output
            }
        }
        Err(e) => format!("Error checking compatibility: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CompatibilityModeParams {
    /// Schema subject name to get or set compatibility mode for.
    /// Example: "user-events", "user-events-value"
    pub subject: String,
}

pub async fn get_compatibility_mode(
    client: &Arc<AdminGrpcClient>,
    params: CompatibilityModeParams,
) -> String {
    let req = danube_core::proto::danube_schema::GetLatestSchemaRequest {
        subject: params.subject.clone(),
    };

    match client.get_latest_schema(req).await {
        Ok(response) => {
            format!(
                "Compatibility mode for subject '{}':\n  Mode: {}",
                params.subject,
                response.compatibility_mode.to_uppercase()
            )
        }
        Err(e) => format!("Error getting compatibility mode: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SetCompatibilityModeParams {
    /// Schema subject name to configure.
    /// Example: "user-events", "user-events-value"
    pub subject: String,

    /// Compatibility mode controlling schema evolution rules.
    /// Options:
    ///   - "none": No compatibility checking (any schema allowed)
    ///   - "backward": New schema can read data written with old schema (add optional fields, remove fields)
    ///   - "forward": Old schema can read data written with new schema (add fields, make optional)
    ///   - "full": Both backward and forward compatible (most restrictive)
    /// Common choice: "backward" for most use cases.
    pub mode: String,
}

pub async fn set_compatibility_mode(
    client: &Arc<AdminGrpcClient>,
    params: SetCompatibilityModeParams,
) -> String {
    let req = danube_core::proto::danube_schema::SetCompatibilityModeRequest {
        subject: params.subject.clone(),
        compatibility_mode: params.mode.to_uppercase(),
    };

    match client.set_compatibility_mode(req).await {
        Ok(response) => {
            if response.success {
                format!(
                    "✓ Compatibility mode set for subject '{}'\n  Mode: {}",
                    params.subject,
                    params.mode.to_uppercase()
                )
            } else {
                format!(
                    "✗ Failed to set compatibility mode for subject '{}': {}",
                    params.subject, response.message
                )
            }
        }
        Err(e) => format!("Error setting compatibility mode: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeleteSchemaVersionParams {
    /// Schema subject name containing the version to delete.
    /// Example: "user-events", "user-events-value"
    pub subject: String,

    /// Specific version number to delete.
    /// Use list_schema_versions to discover available versions.
    /// WARNING: Deleting versions may break topics using this schema.
    /// Example: 2, 5
    pub version: u32,
}

pub async fn delete_schema_version(
    client: &Arc<AdminGrpcClient>,
    params: DeleteSchemaVersionParams,
) -> String {
    let req = danube_core::proto::danube_schema::DeleteSchemaVersionRequest {
        subject: params.subject.clone(),
        version: params.version,
    };

    match client.delete_schema_version(req).await {
        Ok(response) => {
            if response.success {
                format!(
                    "✓ Successfully deleted version {} of schema '{}'",
                    params.version, params.subject
                )
            } else {
                format!(
                    "✗ Failed to delete version {}: {}",
                    params.version, response.message
                )
            }
        }
        Err(e) => format!("Error deleting schema version: {}", e),
    }
}
