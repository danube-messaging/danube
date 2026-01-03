use crate::schema::types::{CompatibilityMode, SchemaType};
use serde::{Deserialize, Serialize};

/// Schema definition wrapping different format-specific schemas
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaDefinition {
    /// Raw bytes schema (no validation)
    Bytes,
    /// String schema (UTF-8 validation only)
    String,
    /// Number schema (supports int, long, float, double)
    Number,
    /// Avro schema
    Avro(AvroSchema),
    /// JSON Schema
    JsonSchema(JsonSchemaDefinition),
    /// Protocol Buffers schema (future)
    Protobuf(ProtobufDefinition),
}

impl SchemaDefinition {
    pub fn schema_type(&self) -> SchemaType {
        match self {
            SchemaDefinition::Bytes => SchemaType::Bytes,
            SchemaDefinition::String => SchemaType::String,
            SchemaDefinition::Number => SchemaType::Number,
            SchemaDefinition::Avro(_) => SchemaType::Avro,
            SchemaDefinition::JsonSchema(_) => SchemaType::JsonSchema,
            SchemaDefinition::Protobuf(_) => SchemaType::Protobuf,
        }
    }
}

/// Avro schema representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvroSchema {
    /// Raw Avro schema JSON
    pub raw_schema: String,
    /// SHA-256 fingerprint of the schema
    pub fingerprint: String,
    // Note: We don't store the parsed apache_avro::Schema here because it's not serializable
    // It will be compiled on-demand by the validator
}

impl AvroSchema {
    pub fn new(raw_schema: String, fingerprint: String) -> Self {
        Self {
            raw_schema,
            fingerprint,
        }
    }
}

/// JSON Schema representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonSchemaDefinition {
    /// Raw JSON Schema as string
    pub raw_schema: String,
    /// SHA-256 fingerprint of the schema
    pub fingerprint: String,
    // Note: jsonschema::Validator is not serializable, will be compiled on-demand
}

impl JsonSchemaDefinition {
    pub fn new(raw_schema: String, fingerprint: String) -> Self {
        Self {
            raw_schema,
            fingerprint,
        }
    }
}

/// Protocol Buffers schema representation (future)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtobufDefinition {
    /// Raw .proto file content
    pub raw_proto: String,
    /// Message type name
    pub message_name: String,
    /// SHA-256 fingerprint of the schema
    pub fingerprint: String,
}

impl ProtobufDefinition {
    #[allow(dead_code)]
    pub fn new(raw_proto: String, message_name: String, fingerprint: String) -> Self {
        Self {
            raw_proto,
            message_name,
            fingerprint,
        }
    }
}

/// A specific version of a schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Version number (starts at 1, increments)
    pub version: u32,
    /// Schema definition for this version
    pub schema_def: SchemaDefinition,
    /// SHA-256 fingerprint for deduplication
    pub fingerprint: String,
    /// When this version was created (Unix timestamp)
    pub created_at: u64,
    /// Who created this version
    pub created_by: String,
    /// Human-readable description
    pub description: String,
    /// Tags for categorization
    pub tags: Vec<String>,
    /// Whether this version is deprecated
    pub is_deprecated: bool,
    /// Deprecation message if deprecated
    pub deprecation_message: Option<String>,
}

impl SchemaVersion {
    pub fn new(
        version: u32,
        schema_def: SchemaDefinition,
        fingerprint: String,
        created_by: String,
        description: String,
    ) -> Self {
        Self {
            version,
            schema_def,
            fingerprint,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            created_by,
            description,
            tags: Vec::new(),
            is_deprecated: false,
            deprecation_message: None,
        }
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }
}

/// Complete metadata for a schema subject
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    /// Globally unique schema ID
    pub id: u64,
    /// Subject name (usually topic name)
    pub subject: String,
    /// All versions of this schema
    pub versions: Vec<SchemaVersion>,
    /// Latest version number
    pub latest_version: u32,
    /// Compatibility mode for this subject
    pub compatibility_mode: CompatibilityMode,
    /// When this subject was created
    pub created_at: u64,
    /// Who created this subject
    pub created_by: String,
    /// When last updated
    pub updated_at: u64,
    /// Topics currently using this schema
    pub topics_using: Vec<String>,
}

impl SchemaMetadata {
    pub fn new(id: u64, subject: String, first_version: SchemaVersion, created_by: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            id,
            subject,
            versions: vec![first_version],
            latest_version: 1,
            compatibility_mode: CompatibilityMode::default(),
            created_at: now,
            created_by,
            updated_at: now,
            topics_using: Vec::new(),
        }
    }

    pub fn add_version(&mut self, version: SchemaVersion) {
        self.latest_version = version.version;
        self.versions.push(version);
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    pub fn get_version(&self, version: u32) -> Option<&SchemaVersion> {
        self.versions.iter().find(|v| v.version == version)
    }

    pub fn get_latest_version(&self) -> Option<&SchemaVersion> {
        self.versions.last()
    }

    pub fn set_compatibility_mode(&mut self, mode: CompatibilityMode) {
        self.compatibility_mode = mode;
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}

/// Result of a compatibility check
#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    pub is_compatible: bool,
    pub errors: Vec<String>,
}

impl CompatibilityResult {
    pub fn compatible() -> Self {
        Self {
            is_compatible: true,
            errors: Vec::new(),
        }
    }

    pub fn incompatible(errors: Vec<String>) -> Self {
        Self {
            is_compatible: false,
            errors,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_version_updates_latest() {
        let schema_def = SchemaDefinition::String;
        let version1 = SchemaVersion::new(
            1,
            schema_def.clone(),
            "fingerprint1".to_string(),
            "test_user".to_string(),
            "V1".to_string(),
        );

        let mut metadata = SchemaMetadata::new(
            1,
            "test-subject".to_string(),
            version1,
            "test_user".to_string(),
        );

        let version2 = SchemaVersion::new(
            2,
            schema_def,
            "fingerprint2".to_string(),
            "test_user".to_string(),
            "V2".to_string(),
        );

        metadata.add_version(version2);

        assert_eq!(metadata.latest_version, 2);
        assert_eq!(metadata.versions.len(), 2);
        assert!(metadata.get_latest_version().is_some());
        assert_eq!(metadata.get_latest_version().unwrap().version, 2);
    }

    #[test]
    fn test_get_version_returns_correct_version() {
        let schema_def = SchemaDefinition::Number;
        let version1 = SchemaVersion::new(
            1,
            schema_def.clone(),
            "fp1".to_string(),
            "user1".to_string(),
            "First".to_string(),
        );

        let mut metadata = SchemaMetadata::new(
            100,
            "numbers".to_string(),
            version1,
            "user1".to_string(),
        );

        let version2 = SchemaVersion::new(
            2,
            schema_def,
            "fp2".to_string(),
            "user2".to_string(),
            "Second".to_string(),
        );

        metadata.add_version(version2);

        // Test retrieving specific versions
        assert!(metadata.get_version(1).is_some());
        assert_eq!(metadata.get_version(1).unwrap().fingerprint, "fp1");
        assert!(metadata.get_version(2).is_some());
        assert_eq!(metadata.get_version(2).unwrap().fingerprint, "fp2");
        assert!(metadata.get_version(99).is_none());
    }
}
