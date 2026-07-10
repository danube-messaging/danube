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

use crate::schema::{PayloadFormat, SchemaMode};
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
        let version = messages.iter().find_map(|m| m.schema_version).unwrap_or(1);

        // Check cache
        if let Some(cached) = self.cache.get(&(schema_id, version)) {
            debug!(schema_id, version, "schema cache hit");
            return Ok(cached.clone());
        }

        // Fetch from registry
        info!(schema_id, version, "fetching schema from registry");

        let schema_client = self.danube_client.schema();
        let schema_info = schema_client
            .get_schema_version(schema_id, Some(version))
            .await
            .map_err(|e| {
                anyhow::anyhow!("failed to fetch schema {}/{}: {}", schema_id, version, e)
            })?;

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
                let definition = info
                    .schema_definition_as_string()
                    .ok_or_else(|| anyhow::anyhow!("json_schema definition is not valid UTF-8"))?;
                let (field_names, field_types) = json_schema_to_arrow_fields(&definition)?;
                let schema = build_registry_schema(&field_names, &field_types);
                Ok(SchemaMode::Registry {
                    schema_id,
                    schema_version: version,
                    schema,
                    payload_format: PayloadFormat::Json,
                })
            }
            "avro" => {
                let definition = info
                    .schema_definition_as_string()
                    .ok_or_else(|| anyhow::anyhow!("avro schema definition is not valid UTF-8"))?;
                let (field_names, field_types) = avro_schema_to_arrow_fields(&definition)?;
                let schema = build_registry_schema(&field_names, &field_types);

                Ok(SchemaMode::Registry {
                    schema_id,
                    schema_version: version,
                    schema,
                    payload_format: PayloadFormat::Avro {
                        schema_json: definition.to_string(),
                    },
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

/// Parse a JSON Schema document and extract field definitions as Arrow types.
///
/// Recursively converts nested objects to `Struct` and arrays to `List`,
/// taking full advantage of `arrow_json::ReaderBuilder`'s native complex
/// type support.
///
/// ## Type mapping
///
/// | JSON Schema type | Arrow type |
/// |------------------|------------|
/// | `string`         | `Utf8`     |
/// | `integer`        | `Int64`    |
/// | `number`         | `Float64`  |
/// | `boolean`        | `Boolean`  |
/// | `object`         | `Struct` (recursive) |
/// | `array`          | `List` (of items type) |
fn json_schema_to_arrow_fields(definition: &str) -> anyhow::Result<(Vec<String>, Vec<DataType>)> {
    let schema: serde_json::Value = serde_json::from_str(definition)
        .map_err(|e| anyhow::anyhow!("invalid JSON Schema: {}", e))?;

    let properties = schema
        .get("properties")
        .and_then(|p| p.as_object())
        .ok_or_else(|| anyhow::anyhow!("JSON Schema missing 'properties' object"))?;

    let mut field_names = Vec::new();
    let mut field_types = Vec::new();

    for (name, prop) in properties {
        let arrow_type = json_schema_type_to_arrow(prop);
        field_names.push(name.clone());
        field_types.push(arrow_type);
    }

    Ok((field_names, field_types))
}

/// Convert a single JSON Schema type definition to an Arrow DataType.
///
/// Handles nested `object` (→ `Struct`) and `array` (→ `List`) recursively.
fn json_schema_type_to_arrow(prop: &serde_json::Value) -> DataType {
    let json_type = prop
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("string");

    match json_type {
        "string" => DataType::Utf8,
        "integer" => DataType::Int64,
        "number" => DataType::Float64,
        "boolean" => DataType::Boolean,
        "object" => {
            // Recurse into nested object properties → Struct
            if let Some(props) = prop.get("properties").and_then(|p| p.as_object()) {
                let fields: Vec<Field> = props
                    .iter()
                    .map(|(name, sub_prop)| {
                        Field::new(name, json_schema_type_to_arrow(sub_prop), true)
                    })
                    .collect();
                if fields.is_empty() {
                    DataType::Utf8 // empty object → stringify
                } else {
                    DataType::Struct(fields.into())
                }
            } else {
                DataType::Utf8 // no properties defined → stringify
            }
        }
        "array" => {
            // Convert items type → List
            if let Some(items) = prop.get("items") {
                let item_type = json_schema_type_to_arrow(items);
                DataType::List(Arc::new(Field::new("item", item_type, true)))
            } else {
                // No items schema → list of strings
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
            }
        }
        _ => DataType::Utf8,
    }
}

// ============================================================================
// Avro Schema → Arrow conversion
// ============================================================================

/// Parse an Avro schema and extract field definitions as Arrow types.
///
/// Recursively converts nested records to `Struct`, arrays to `List`,
/// and maps to `Map`, taking full advantage of `arrow_json::ReaderBuilder`.
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
/// | record      | `Struct` (recursive) |
/// | array       | `List` (of items type) |
/// | map         | `Map<Utf8, V>` |
fn avro_schema_to_arrow_fields(definition: &str) -> anyhow::Result<(Vec<String>, Vec<DataType>)> {
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
///
/// Handles nested records (→ `Struct`), arrays (→ `List`), and
/// maps (→ `Map<Utf8, V>`) recursively.
fn avro_type_to_arrow(avro_type: &serde_json::Value) -> DataType {
    match avro_type {
        serde_json::Value::String(s) => match s.as_str() {
            "string" => DataType::Utf8,
            "int" | "long" => DataType::Int64,
            "float" | "double" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "bytes" => DataType::Binary,
            "null" => DataType::Utf8,
            // Named types (enums, fixed) → stringify
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
        // Complex type object: {"type": "record", ...}, {"type": "array", ...}, etc.
        serde_json::Value::Object(obj) => {
            let type_name = obj.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match type_name {
                "record" => {
                    // Nested record → Struct
                    if let Some(fields) = obj.get("fields").and_then(|f| f.as_array()) {
                        let arrow_fields: Vec<Field> = fields
                            .iter()
                            .filter_map(|f| {
                                let name = f.get("name")?.as_str()?;
                                let ft = f.get("type")?;
                                Some(Field::new(name, avro_type_to_arrow(ft), true))
                            })
                            .collect();
                        if arrow_fields.is_empty() {
                            DataType::Utf8
                        } else {
                            DataType::Struct(arrow_fields.into())
                        }
                    } else {
                        DataType::Utf8
                    }
                }
                "array" => {
                    // Avro array → List
                    let item_type = obj
                        .get("items")
                        .map(avro_type_to_arrow)
                        .unwrap_or(DataType::Utf8);
                    DataType::List(Arc::new(Field::new("item", item_type, true)))
                }
                "map" => {
                    // Avro map → Map<Utf8, V>
                    let value_type = obj
                        .get("values")
                        .map(avro_type_to_arrow)
                        .unwrap_or(DataType::Utf8);
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Field::new("key", DataType::Utf8, false),
                                    Field::new("value", value_type, true),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    )
                }
                _ => DataType::Utf8,
            }
        }
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
#[path = "schema_resolver_tests.rs"]
mod tests;
