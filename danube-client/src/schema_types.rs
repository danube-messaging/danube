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
