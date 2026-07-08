//! Schema mapping — converts Danube `StreamMessage` values into Arrow `RecordBatch`.
//!
//! Two modes, matching the WarpStream / AutoMQ pattern:
//!
//! 1. **Registry** — The topic has a registered schema (JSON Schema, Avro, etc.)
//!    in Danube's Schema Registry. Messages carry `schema_id` / `schema_version`
//!    which the [`SchemaResolver`](crate::schema_resolver) uses to fetch the
//!    schema definition and convert it to a typed Arrow schema.
//!
//! 2. **Envelope** (fallback) — No schema is registered. Every message becomes
//!    a row with fixed metadata columns + an opaque binary payload. Analytics
//!    engines can query metadata but need to parse the payload themselves.

use arrow_array::builder::{BinaryBuilder, Int64Builder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

/// The resolved schema mode for a topic.
#[derive(Debug, Clone)]
pub enum SchemaMode {
    /// Fixed envelope schema — message metadata + raw payload.
    Envelope,
    /// Schema from registry — payload fields promoted to typed Arrow columns.
    Registry {
        /// Schema ID from the Danube Schema Registry.
        schema_id: u64,
        /// Schema version from the Danube Schema Registry.
        schema_version: u32,
        /// Arrow schema with metadata columns + registry-defined payload columns.
        schema: Arc<Schema>,
        /// Ordered field names from the registry schema (payload columns only).
        field_names: Vec<String>,
        /// Arrow data types for each registry field.
        field_types: Vec<DataType>,
    },
}

/// Build the fixed envelope Arrow schema.
///
/// Every Danube message can be represented as this schema — it's the
/// universal fallback when no schema is registered in the registry.
pub fn envelope_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("offset", DataType::Int64, false),
        Field::new("publish_time", DataType::Int64, false),
        Field::new("producer_name", DataType::Utf8, false),
        Field::new("routing_key", DataType::Utf8, true),
        Field::new("schema_id", DataType::Int64, true),
        Field::new("schema_version", DataType::Int64, true),
        Field::new("payload", DataType::Binary, false),
        Field::new("attributes_json", DataType::Utf8, true),
    ]))
}

// ============================================================================
// RecordBatch builders
// ============================================================================

/// Convert messages to a RecordBatch using the envelope schema.
pub fn messages_to_envelope_batch(
    messages: &[crate::segment_reader::DecodedMessage],
) -> anyhow::Result<RecordBatch> {
    let schema = envelope_schema();

    let mut offsets = Int64Builder::with_capacity(messages.len());
    let mut publish_times = Int64Builder::with_capacity(messages.len());
    let mut producers = StringBuilder::with_capacity(messages.len(), messages.len() * 32);
    let mut routing_keys = StringBuilder::with_capacity(messages.len(), messages.len() * 16);
    let mut schema_ids = Int64Builder::with_capacity(messages.len());
    let mut schema_versions = Int64Builder::with_capacity(messages.len());
    let mut payloads = BinaryBuilder::with_capacity(messages.len(), messages.len() * 256);
    let mut attributes = StringBuilder::with_capacity(messages.len(), messages.len() * 64);

    for dm in messages {
        let msg = &dm.message;
        offsets.append_value(dm.offset as i64);
        publish_times.append_value(msg.publish_time as i64);
        producers.append_value(&msg.producer_name);

        match &msg.routing_key {
            Some(k) => routing_keys.append_value(k),
            None => routing_keys.append_null(),
        }

        match msg.schema_id {
            Some(id) => schema_ids.append_value(id as i64),
            None => schema_ids.append_null(),
        }

        match msg.schema_version {
            Some(v) => schema_versions.append_value(v as i64),
            None => schema_versions.append_null(),
        }

        payloads.append_value(&msg.payload);

        if msg.attributes.is_empty() {
            attributes.append_null();
        } else {
            let json = serde_json::to_string(&msg.attributes)?;
            attributes.append_value(&json);
        }
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(offsets.finish()),
            Arc::new(publish_times.finish()),
            Arc::new(producers.finish()),
            Arc::new(routing_keys.finish()),
            Arc::new(schema_ids.finish()),
            Arc::new(schema_versions.finish()),
            Arc::new(payloads.finish()),
            Arc::new(attributes.finish()),
        ],
    )?;

    Ok(batch)
}

/// Convert messages to a RecordBatch using a registry-defined schema.
///
/// Message metadata (offset, publish_time, producer_name, routing_key) are
/// always included as the first 4 columns. The remaining columns come from
/// the registry schema — payload is parsed as JSON and fields are extracted
/// into typed columns.
pub fn messages_to_registry_batch(
    messages: &[crate::segment_reader::DecodedMessage],
    field_names: &[String],
    field_types: &[DataType],
    schema: &Arc<Schema>,
) -> anyhow::Result<RecordBatch> {
    let n = messages.len();

    // Metadata columns
    let mut offsets = Int64Builder::with_capacity(n);
    let mut publish_times = Int64Builder::with_capacity(n);
    let mut producers = StringBuilder::with_capacity(n, n * 32);
    let mut routing_keys = StringBuilder::with_capacity(n, n * 16);

    // Registry-defined payload columns — one builder per field
    let mut col_builders: Vec<ColumnBuilder> = field_types
        .iter()
        .map(|dt| ColumnBuilder::new(dt, n))
        .collect();

    for dm in messages {
        let msg = &dm.message;
        offsets.append_value(dm.offset as i64);
        publish_times.append_value(msg.publish_time as i64);
        producers.append_value(&msg.producer_name);
        match &msg.routing_key {
            Some(k) => routing_keys.append_value(k),
            None => routing_keys.append_null(),
        }

        // Parse the JSON payload
        let json_obj: Option<serde_json::Map<String, serde_json::Value>> =
            serde_json::from_slice(&msg.payload)
                .ok()
                .and_then(|v: serde_json::Value| {
                    if let serde_json::Value::Object(m) = v {
                        Some(m)
                    } else {
                        None
                    }
                });

        for (i, name) in field_names.iter().enumerate() {
            let value = json_obj.as_ref().and_then(|m| m.get(name));
            col_builders[i].append(value);
        }
    }

    let mut arrays: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(offsets.finish()),
        Arc::new(publish_times.finish()),
        Arc::new(producers.finish()),
        Arc::new(routing_keys.finish()),
    ];

    for builder in &mut col_builders {
        arrays.push(builder.finish());
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    Ok(batch)
}

// ============================================================================
// Dynamic column builder (per registry-defined field)
// ============================================================================

enum ColumnBuilder {
    Boolean(arrow_array::builder::BooleanBuilder),
    Int64(Int64Builder),
    Float64(arrow_array::builder::Float64Builder),
    Utf8(StringBuilder),
}

impl ColumnBuilder {
    fn new(dt: &DataType, capacity: usize) -> Self {
        match dt {
            DataType::Boolean => {
                Self::Boolean(arrow_array::builder::BooleanBuilder::with_capacity(capacity))
            }
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => {
                Self::Float64(arrow_array::builder::Float64Builder::with_capacity(capacity))
            }
            _ => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 32)),
        }
    }

    fn append(&mut self, value: Option<&serde_json::Value>) {
        match self {
            Self::Boolean(b) => match value {
                Some(serde_json::Value::Bool(v)) => b.append_value(*v),
                Some(serde_json::Value::Null) | None => b.append_null(),
                _ => b.append_null(),
            },
            Self::Int64(b) => match value {
                Some(serde_json::Value::Number(n)) => {
                    if let Some(v) = n.as_i64() {
                        b.append_value(v);
                    } else if let Some(v) = n.as_f64() {
                        b.append_value(v as i64);
                    } else {
                        b.append_null();
                    }
                }
                Some(serde_json::Value::Null) | None => b.append_null(),
                _ => b.append_null(),
            },
            Self::Float64(b) => match value {
                Some(serde_json::Value::Number(n)) => {
                    if let Some(v) = n.as_f64() {
                        b.append_value(v);
                    } else {
                        b.append_null();
                    }
                }
                Some(serde_json::Value::Null) | None => b.append_null(),
                _ => b.append_null(),
            },
            Self::Utf8(b) => match value {
                Some(serde_json::Value::String(s)) => b.append_value(s),
                Some(serde_json::Value::Null) | None => b.append_null(),
                // Arrays and objects → JSON stringify
                Some(other) => b.append_value(other.to_string()),
            },
        }
    }

    fn finish(&mut self) -> Arc<dyn arrow_array::Array> {
        match self {
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Utf8(b) => Arc::new(b.finish()),
        }
    }
}
