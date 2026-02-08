use std::fmt;
use std::str::FromStr;

/// Schema compatibility modes for schema evolution
///
/// Defines how strictly new schema versions must be compatible with existing versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityMode {
    /// No compatibility checking - any schema change is allowed
    None,
    /// New schema can read data written with old schema (most common)
    /// - Allows: adding optional fields, removing fields from reader
    /// - Use case: Consumers upgrade before producers
    Backward,
    /// Old schema can read data written with new schema
    /// - Allows: adding required fields, removing optional fields
    /// - Use case: Producers upgrade before consumers
    Forward,
    /// Both backward and forward compatible (strictest)
    /// - Use case: Critical schemas that need both directions
    Full,
}

impl CompatibilityMode {
    /// Convert to string representation for API calls
    pub fn as_str(&self) -> &'static str {
        match self {
            CompatibilityMode::None => "none",
            CompatibilityMode::Backward => "backward",
            CompatibilityMode::Forward => "forward",
            CompatibilityMode::Full => "full",
        }
    }
}

impl Default for CompatibilityMode {
    fn default() -> Self {
        CompatibilityMode::Backward
    }
}

impl fmt::Display for CompatibilityMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CompatibilityMode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(CompatibilityMode::None),
            "backward" => Ok(CompatibilityMode::Backward),
            "forward" => Ok(CompatibilityMode::Forward),
            "full" => Ok(CompatibilityMode::Full),
            other => Err(format!("unknown compatibility mode: '{}'", other)),
        }
    }
}

/// Schema types supported by the registry
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    /// Raw bytes - no schema validation
    /// Use for binary data or custom serialization
    Bytes,

    /// UTF-8 string - validates string encoding
    /// Use for plain text messages
    String,

    /// Numeric types (int, long, float, double)
    /// Validates numeric data
    Number,

    /// Apache Avro schema format
    /// Structured binary format with schema evolution support
    Avro,

    /// JSON Schema format
    /// JSON-based schema validation
    JsonSchema,

    /// Protocol Buffers schema format
    /// Google's language-neutral serialization format
    Protobuf,
}

impl SchemaType {
    /// Convert to string representation for API calls
    pub fn as_str(&self) -> &'static str {
        match self {
            SchemaType::Bytes => "bytes",
            SchemaType::String => "string",
            SchemaType::Number => "number",
            SchemaType::Avro => "avro",
            SchemaType::JsonSchema => "json_schema",
            SchemaType::Protobuf => "protobuf",
        }
    }
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SchemaType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "bytes" => Ok(SchemaType::Bytes),
            "string" => Ok(SchemaType::String),
            "number" => Ok(SchemaType::Number),
            "avro" => Ok(SchemaType::Avro),
            "json_schema" | "jsonschema" => Ok(SchemaType::JsonSchema),
            "protobuf" | "proto" => Ok(SchemaType::Protobuf),
            other => Err(format!("unknown schema type: '{}'", other)),
        }
    }
}

use danube_core::proto::danube_schema::GetSchemaResponse as ProtoGetSchemaResponse;

/// Information about a schema retrieved from the registry
///
/// This is a user-friendly wrapper around the proto GetSchemaResponse.
/// Consumers can use this to fetch and validate schemas.
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    /// Schema ID (identifies the subject)
    pub schema_id: u64,
    /// Subject name
    pub subject: String,
    /// Schema version number
    pub version: u32,
    /// Schema type (avro, json, protobuf, etc.)
    pub schema_type: String,
    /// Schema definition as bytes (e.g., Avro schema JSON, Protobuf descriptor)
    pub schema_definition: Vec<u8>,
    /// Fingerprint for deduplication
    pub fingerprint: String,
}

impl SchemaInfo {
    /// Get schema definition as a UTF-8 string (for JSON-based schemas)
    ///
    /// Returns None if the schema definition is not valid UTF-8
    pub fn schema_definition_as_string(&self) -> Option<String> {
        String::from_utf8(self.schema_definition.clone()).ok()
    }
}

impl From<ProtoGetSchemaResponse> for SchemaInfo {
    fn from(proto: ProtoGetSchemaResponse) -> Self {
        SchemaInfo {
            schema_id: proto.schema_id,
            subject: proto.subject,
            version: proto.version,
            schema_type: proto.schema_type,
            schema_definition: proto.schema_definition,
            fingerprint: proto.fingerprint,
        }
    }
}
