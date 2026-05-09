use serde::{Deserialize, Serialize};
use std::fmt;

/// Schema type enumeration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaType {
    Bytes,
    String,
    Number, // Supports int, long, float, double
    Avro,
    JsonSchema,
    Protobuf,
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaType::Bytes => write!(f, "bytes"),
            SchemaType::String => write!(f, "string"),
            SchemaType::Number => write!(f, "number"),
            SchemaType::Avro => write!(f, "avro"),
            SchemaType::JsonSchema => write!(f, "json_schema"),
            SchemaType::Protobuf => write!(f, "protobuf"),
        }
    }
}

impl SchemaType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bytes" => Some(SchemaType::Bytes),
            "string" => Some(SchemaType::String),
            "number" | "int64" | "int" | "float" | "double" => Some(SchemaType::Number),
            "avro" => Some(SchemaType::Avro),
            "json_schema" | "json" => Some(SchemaType::JsonSchema),
            "protobuf" | "proto" => Some(SchemaType::Protobuf),
            _ => None,
        }
    }
}

/// Compatibility modes for schema evolution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilityMode {
    /// No compatibility checking
    None,
    /// New schema can read data written with old schema (most common)
    Backward,
    /// Old schema can read data written with new schema
    Forward,
    /// Both backward and forward compatible (safest)
    Full,
}

impl Default for CompatibilityMode {
    fn default() -> Self {
        CompatibilityMode::Backward
    }
}

impl fmt::Display for CompatibilityMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompatibilityMode::None => write!(f, "none"),
            CompatibilityMode::Backward => write!(f, "backward"),
            CompatibilityMode::Forward => write!(f, "forward"),
            CompatibilityMode::Full => write!(f, "full"),
        }
    }
}

impl CompatibilityMode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" => Some(CompatibilityMode::None),
            "backward" => Some(CompatibilityMode::Backward),
            "forward" => Some(CompatibilityMode::Forward),
            "full" => Some(CompatibilityMode::Full),
            _ => None,
        }
    }
}

/// Validation policy for topics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationPolicy {
    /// No validation
    None,
    /// Validate and log warnings
    Warn,
    /// Validate and reject invalid messages
    Enforce,
}

impl Default for ValidationPolicy {
    fn default() -> Self {
        ValidationPolicy::None
    }
}

impl fmt::Display for ValidationPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationPolicy::None => write!(f, "none"),
            ValidationPolicy::Warn => write!(f, "warn"),
            ValidationPolicy::Enforce => write!(f, "enforce"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compatibility_mode_from_str_case_insensitive() {
        assert_eq!(
            CompatibilityMode::from_str("backward"),
            Some(CompatibilityMode::Backward)
        );
        assert_eq!(
            CompatibilityMode::from_str("FULL"),
            Some(CompatibilityMode::Full)
        );
        assert_eq!(
            CompatibilityMode::from_str("Forward"),
            Some(CompatibilityMode::Forward)
        );
        assert_eq!(CompatibilityMode::from_str("invalid"), None);
    }

    #[test]
    fn test_schema_type_from_str_with_aliases() {
        // Test canonical names
        assert_eq!(SchemaType::from_str("avro"), Some(SchemaType::Avro));
        assert_eq!(
            SchemaType::from_str("json_schema"),
            Some(SchemaType::JsonSchema)
        );
        assert_eq!(SchemaType::from_str("protobuf"), Some(SchemaType::Protobuf));

        // Test aliases (critical for API compatibility)
        assert_eq!(SchemaType::from_str("json"), Some(SchemaType::JsonSchema));
        assert_eq!(SchemaType::from_str("proto"), Some(SchemaType::Protobuf));
        assert_eq!(SchemaType::from_str("int64"), Some(SchemaType::Number));
        assert_eq!(SchemaType::from_str("double"), Some(SchemaType::Number));

        // Test invalid
        assert_eq!(SchemaType::from_str("invalid"), None);
    }
}
