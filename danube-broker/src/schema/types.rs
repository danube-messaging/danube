use serde::{Deserialize, Serialize};
use std::fmt;

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

/// Subject naming strategy (for future use)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
pub enum SubjectNameStrategy {
    /// Subject = Topic Name (default)
    TopicName,
    /// Subject = Record Name (future)
    RecordName,
    /// Subject = Topic + Record Name (future)
    TopicRecordName,
}

impl Default for SubjectNameStrategy {
    fn default() -> Self {
        SubjectNameStrategy::TopicName
    }
}

impl SubjectNameStrategy {
    /// Extract subject name from topic name using the strategy
    #[allow(dead_code)]
    pub fn get_subject(&self, topic_name: &str, _record_name: Option<&str>) -> String {
        match self {
            SubjectNameStrategy::TopicName => {
                // Remove namespace prefix: "/default/orders" -> "orders"
                topic_name
                    .split('/')
                    .last()
                    .unwrap_or(topic_name)
                    .to_string()
            }
            SubjectNameStrategy::RecordName => {
                // TODO: Future implementation
                unimplemented!("RecordName strategy not yet implemented")
            }
            SubjectNameStrategy::TopicRecordName => {
                // TODO: Future implementation
                unimplemented!("TopicRecordName strategy not yet implemented")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compatibility_mode_display() {
        assert_eq!(CompatibilityMode::Backward.to_string(), "backward");
        assert_eq!(CompatibilityMode::Full.to_string(), "full");
    }

    #[test]
    fn test_compatibility_mode_from_str() {
        assert_eq!(
            CompatibilityMode::from_str("backward"),
            Some(CompatibilityMode::Backward)
        );
        assert_eq!(
            CompatibilityMode::from_str("FULL"),
            Some(CompatibilityMode::Full)
        );
        assert_eq!(CompatibilityMode::from_str("invalid"), None);
    }

    #[test]
    fn test_schema_type_from_str() {
        assert_eq!(SchemaType::from_str("avro"), Some(SchemaType::Avro));
        assert_eq!(SchemaType::from_str("json"), Some(SchemaType::JsonSchema));
        assert_eq!(
            SchemaType::from_str("json_schema"),
            Some(SchemaType::JsonSchema)
        );
    }

    #[test]
    fn test_subject_name_strategy() {
        let strategy = SubjectNameStrategy::TopicName;
        assert_eq!(strategy.get_subject("/default/orders", None), "orders");
        assert_eq!(strategy.get_subject("orders", None), "orders");
        assert_eq!(
            strategy.get_subject("/namespace/topic-name", None),
            "topic-name"
        );
    }
}
