//! Schema Resolver — determines the Arrow schema for a topic.
//!
//! Implements the 2-tier strategy matching WarpStream / AutoMQ:
//!
//! 1. **Schema Registry** — If messages carry `schema_id`, fetch the schema
//!    definition from the Danube Schema Registry via gRPC, convert it to an
//!    Arrow schema, and cache the result.
//!
//! 2. **Envelope** — If no `schema_id` is present, fall back to the fixed
//!    envelope schema (metadata + opaque binary payload).
//!
//! ## Schema Type Support
//!
//! | Schema Type   | Conversion                              |
//! |---------------|-----------------------------------------|
//! | `json_schema` | Parse JSON Schema → Arrow fields        |
//! | `avro`        | Parse Avro schema → Arrow fields        |
//! | `bytes`       | Envelope mode (no structured payload)   |
//! | `string`      | Envelope mode                           |
//! | `number`      | Envelope mode                           |
//! | `protobuf`    | Envelope mode (future: proto → Arrow)   |

use crate::schema::SchemaMode;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use danube_client::{DanubeClient, SchemaInfo};
use danube_core::message::StreamMessage;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Resolves schema mode for a topic by checking messages for schema IDs
/// and fetching definitions from the Danube Schema Registry.
pub struct SchemaResolver {
    /// Danube client for Schema Registry gRPC calls.
    danube_client: DanubeClient,
    /// Cache: (schema_id, version) → resolved SchemaMode.
    /// Schemas are immutable for a given (id, version) pair.
    cache: HashMap<(u64, u32), SchemaMode>,
}

impl SchemaResolver {
    /// Create a new resolver connected to a Danube broker.
    pub fn new(danube_client: DanubeClient) -> Self {
        Self {
            danube_client,
            cache: HashMap::new(),
        }
    }

    /// Resolve the schema mode for a batch of messages.
    ///
    /// Strategy (matching WarpStream / AutoMQ):
    /// 1. If messages carry `schema_id` → fetch from registry → typed columns
    /// 2. Otherwise → envelope (opaque payload)
    pub async fn resolve(&mut self, messages: &[StreamMessage]) -> anyhow::Result<SchemaMode> {
        if messages.is_empty() {
            return Ok(SchemaMode::Envelope);
        }

        // Check first message for schema_id (WarpStream approach: use latest)
        let schema_id = match messages.iter().find_map(|m| m.schema_id) {
            Some(id) => id,
            None => {
                debug!("no schema_id in messages, using envelope mode");
                return Ok(SchemaMode::Envelope);
            }
        };

        // Use version from the message, default to latest (version 1)
        let version = messages
            .iter()
            .find_map(|m| m.schema_version)
            .unwrap_or(1);

        // Check cache
        if let Some(cached) = self.cache.get(&(schema_id, version)) {
            debug!(
                schema_id,
                version, "schema cache hit"
            );
            return Ok(cached.clone());
        }

        // Fetch from registry
        info!(
            schema_id,
            version, "fetching schema from registry"
        );

        let schema_client = self.danube_client.schema();
        let schema_info = schema_client
            .get_schema_version(schema_id, Some(version))
            .await
            .map_err(|e| anyhow::anyhow!("failed to fetch schema {}/{}: {}", schema_id, version, e))?;

        let mode = self.convert_schema_info(schema_id, version, &schema_info)?;

        // Cache the result
        self.cache.insert((schema_id, version), mode.clone());

        info!(
            schema_id,
            version,
            schema_type = %schema_info.schema_type,
            "schema resolved and cached"
        );

        Ok(mode)
    }

    /// Convert a SchemaInfo from the registry to a SchemaMode.
    fn convert_schema_info(
        &self,
        schema_id: u64,
        version: u32,
        info: &SchemaInfo,
    ) -> anyhow::Result<SchemaMode> {
        match info.schema_type.as_str() {
            "json_schema" | "json" => {
                let definition = info.schema_definition_as_string().ok_or_else(|| {
                    anyhow::anyhow!("json_schema definition is not valid UTF-8")
                })?;
                let (field_names, field_types) = json_schema_to_arrow_fields(&definition)?;
                let schema = build_registry_schema(&field_names, &field_types);
                Ok(SchemaMode::Registry {
                    schema_id,
                    schema_version: version,
                    schema,
                    field_names,
                    field_types,
                })
            }
            "avro" => {
                let definition = info.schema_definition_as_string().ok_or_else(|| {
                    anyhow::anyhow!("avro schema definition is not valid UTF-8")
                })?;
                let (field_names, field_types) = avro_schema_to_arrow_fields(&definition)?;
                let schema = build_registry_schema(&field_names, &field_types);
                Ok(SchemaMode::Registry {
                    schema_id,
                    schema_version: version,
                    schema,
                    field_names,
                    field_types,
                })
            }
            // bytes, string, number, protobuf → no structured payload
            other => {
                debug!(
                    schema_type = other,
                    "schema type not convertible to typed columns, using envelope"
                );
                Ok(SchemaMode::Envelope)
            }
        }
    }
}

/// Build a full Arrow schema with metadata columns + registry-defined payload columns.
fn build_registry_schema(field_names: &[String], field_types: &[DataType]) -> Arc<ArrowSchema> {
    let mut fields = vec![
        Field::new("offset", DataType::Int64, false),
        Field::new("publish_time", DataType::Int64, false),
        Field::new("producer_name", DataType::Utf8, false),
        Field::new("routing_key", DataType::Utf8, true),
    ];

    for (name, dt) in field_names.iter().zip(field_types.iter()) {
        // All registry fields are nullable (a message might have nulls)
        fields.push(Field::new(name, dt.clone(), true));
    }

    Arc::new(ArrowSchema::new(fields))
}

// ============================================================================
// JSON Schema → Arrow conversion
// ============================================================================

/// Parse a JSON Schema document and extract flat field definitions as Arrow types.
///
/// Supports flat objects only (no `$ref`, `allOf`, nested objects).
/// Nested objects and arrays are stringified to JSON (stored as Utf8).
///
/// ## Type mapping
///
/// | JSON Schema type | Arrow type |
/// |------------------|------------|
/// | `string`         | `Utf8`     |
/// | `integer`        | `Int64`    |
/// | `number`         | `Float64`  |
/// | `boolean`        | `Boolean`  |
/// | `object`         | `Utf8` (JSON stringified) |
/// | `array`          | `Utf8` (JSON stringified) |
fn json_schema_to_arrow_fields(
    definition: &str,
) -> anyhow::Result<(Vec<String>, Vec<DataType>)> {
    let schema: serde_json::Value = serde_json::from_str(definition)
        .map_err(|e| anyhow::anyhow!("invalid JSON Schema: {}", e))?;

    let properties = schema
        .get("properties")
        .and_then(|p| p.as_object())
        .ok_or_else(|| anyhow::anyhow!("JSON Schema missing 'properties' object"))?;

    let mut field_names = Vec::new();
    let mut field_types = Vec::new();

    for (name, prop) in properties {
        let json_type = prop
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("string");

        let arrow_type = match json_type {
            "string" => DataType::Utf8,
            "integer" => DataType::Int64,
            "number" => DataType::Float64,
            "boolean" => DataType::Boolean,
            // Nested objects and arrays → stringify as JSON
            "object" | "array" => DataType::Utf8,
            _ => DataType::Utf8,
        };

        field_names.push(name.clone());
        field_types.push(arrow_type);
    }

    Ok((field_names, field_types))
}

// ============================================================================
// Avro Schema → Arrow conversion
// ============================================================================

/// Parse an Avro schema and extract field definitions as Arrow types.
///
/// ## Type mapping
///
/// | Avro type   | Arrow type |
/// |-------------|------------|
/// | `string`    | `Utf8`     |
/// | `int`       | `Int64`    |
/// | `long`      | `Int64`    |
/// | `float`     | `Float64`  |
/// | `double`    | `Float64`  |
/// | `boolean`   | `Boolean`  |
/// | `bytes`     | `Binary`   |
/// | `null`      | `Utf8`     |
/// | union       | inner type (if `["null", T]`) |
/// | record/map  | `Utf8` (JSON stringified) |
/// | array       | `Utf8` (JSON stringified) |
fn avro_schema_to_arrow_fields(
    definition: &str,
) -> anyhow::Result<(Vec<String>, Vec<DataType>)> {
    let schema: serde_json::Value = serde_json::from_str(definition)
        .map_err(|e| anyhow::anyhow!("invalid Avro schema: {}", e))?;

    let fields = schema
        .get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| anyhow::anyhow!("Avro schema missing 'fields' array"))?;

    let mut field_names = Vec::new();
    let mut field_types = Vec::new();

    for field in fields {
        let name = field
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow::anyhow!("Avro field missing 'name'"))?;

        let avro_type = field
            .get("type")
            .ok_or_else(|| anyhow::anyhow!("Avro field '{}' missing 'type'", name))?;

        let arrow_type = avro_type_to_arrow(avro_type);

        field_names.push(name.to_string());
        field_types.push(arrow_type);
    }

    Ok((field_names, field_types))
}

/// Convert a single Avro type to an Arrow DataType.
fn avro_type_to_arrow(avro_type: &serde_json::Value) -> DataType {
    match avro_type {
        serde_json::Value::String(s) => match s.as_str() {
            "string" => DataType::Utf8,
            "int" | "long" => DataType::Int64,
            "float" | "double" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "bytes" => DataType::Binary,
            "null" => DataType::Utf8,
            // Named types (records, enums, fixed) → stringify
            _ => DataType::Utf8,
        },
        // Union type: ["null", "string"] → use the non-null type
        serde_json::Value::Array(union_types) => {
            let non_null: Vec<_> = union_types
                .iter()
                .filter(|t| t.as_str() != Some("null"))
                .collect();
            if non_null.len() == 1 {
                avro_type_to_arrow(non_null[0])
            } else {
                // Complex union → stringify
                DataType::Utf8
            }
        }
        // Complex type object (record, map, array, etc.) → stringify
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_schema_flat_object() {
        let schema = r#"{
            "type": "object",
            "properties": {
                "temperature": { "type": "number" },
                "humidity": { "type": "integer" },
                "unit": { "type": "string" },
                "active": { "type": "boolean" }
            }
        }"#;

        let (names, types) = json_schema_to_arrow_fields(schema).unwrap();
        assert_eq!(names.len(), 4);
        assert!(names.contains(&"temperature".to_string()));
        assert!(names.contains(&"humidity".to_string()));
        assert!(names.contains(&"unit".to_string()));
        assert!(names.contains(&"active".to_string()));

        let temp_idx = names.iter().position(|n| n == "temperature").unwrap();
        assert_eq!(types[temp_idx], DataType::Float64);

        let humid_idx = names.iter().position(|n| n == "humidity").unwrap();
        assert_eq!(types[humid_idx], DataType::Int64);
    }

    #[test]
    fn json_schema_nested_stringified() {
        let schema = r#"{
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "metadata": { "type": "object" },
                "tags": { "type": "array" }
            }
        }"#;

        let (names, types) = json_schema_to_arrow_fields(schema).unwrap();
        let meta_idx = names.iter().position(|n| n == "metadata").unwrap();
        assert_eq!(types[meta_idx], DataType::Utf8, "nested object → Utf8");

        let tags_idx = names.iter().position(|n| n == "tags").unwrap();
        assert_eq!(types[tags_idx], DataType::Utf8, "array → Utf8");
    }

    #[test]
    fn avro_schema_flat_record() {
        let schema = r#"{
            "type": "record",
            "name": "SensorReading",
            "fields": [
                { "name": "sensor_id", "type": "string" },
                { "name": "temperature", "type": "double" },
                { "name": "count", "type": "long" },
                { "name": "active", "type": "boolean" }
            ]
        }"#;

        let (names, types) = avro_schema_to_arrow_fields(schema).unwrap();
        assert_eq!(names, vec!["sensor_id", "temperature", "count", "active"]);
        assert_eq!(types[0], DataType::Utf8);
        assert_eq!(types[1], DataType::Float64);
        assert_eq!(types[2], DataType::Int64);
        assert_eq!(types[3], DataType::Boolean);
    }

    #[test]
    fn avro_schema_nullable_union() {
        let schema = r#"{
            "type": "record",
            "name": "Event",
            "fields": [
                { "name": "name", "type": "string" },
                { "name": "value", "type": ["null", "double"] }
            ]
        }"#;

        let (names, types) = avro_schema_to_arrow_fields(schema).unwrap();
        assert_eq!(names, vec!["name", "value"]);
        assert_eq!(types[0], DataType::Utf8);
        assert_eq!(types[1], DataType::Float64, "union [null, double] → Float64");
    }

    #[test]
    fn build_registry_schema_has_metadata_columns() {
        let names = vec!["temp".to_string(), "unit".to_string()];
        let types = vec![DataType::Float64, DataType::Utf8];
        let schema = build_registry_schema(&names, &types);

        assert_eq!(schema.fields().len(), 6); // 4 metadata + 2 payload
        assert_eq!(schema.field(0).name(), "offset");
        assert_eq!(schema.field(1).name(), "publish_time");
        assert_eq!(schema.field(2).name(), "producer_name");
        assert_eq!(schema.field(3).name(), "routing_key");
        assert_eq!(schema.field(4).name(), "temp");
        assert_eq!(schema.field(5).name(), "unit");
    }
}
