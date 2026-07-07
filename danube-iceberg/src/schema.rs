//! Schema mapping — converts Danube `StreamMessage` values into Arrow `RecordBatch`.
//!
//! Supports three modes:
//! 1. **Envelope** (fallback): Every message becomes a row with fixed columns
//!    (offset, publish_time, producer_name, routing_key, payload, attributes).
//! 2. **Inferred JSON**: Attempts to parse payloads as JSON objects, infers a
//!    columnar schema from the union of all keys across a sample of messages.
//! 3. **Explicit**: Uses schema information from the schema registry (JsonSchema/Avro)
//!    to build a typed Arrow schema.

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use danube_core::message::StreamMessage;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::debug;

/// The resolved schema mode for a topic.
#[derive(Debug, Clone)]
pub enum SchemaMode {
    /// Fixed envelope schema — message metadata + raw payload.
    Envelope,
    /// Inferred JSON columnar schema — payload fields promoted to Arrow columns.
    InferredJson {
        /// Arrow schema with message metadata + inferred payload columns.
        schema: Arc<Schema>,
        /// Ordered list of inferred JSON field names (for column construction).
        field_names: Vec<String>,
        /// Arrow data types for each inferred field.
        field_types: Vec<DataType>,
    },
}

/// Build the fixed envelope Arrow schema.
///
/// Every Danube message can be represented as this schema — it's the
/// universal fallback when JSON inference fails or isn't applicable.
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

/// Try to infer a JSON columnar schema from a sample of messages.
///
/// Returns `SchemaMode::InferredJson` if all messages have valid JSON object
/// payloads, otherwise falls back to `SchemaMode::Envelope`.
pub fn infer_schema(messages: &[StreamMessage]) -> SchemaMode {
    if messages.is_empty() {
        return SchemaMode::Envelope;
    }

    // Sample up to 100 messages for schema inference
    let sample_size = messages.len().min(100);
    let sample = &messages[..sample_size];

    // Try parsing each payload as JSON object
    let mut all_keys: BTreeMap<String, DataType> = BTreeMap::new();
    let mut json_count = 0;

    for msg in sample {
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
            if let serde_json::Value::Object(map) = value {
                json_count += 1;
                for (key, val) in &map {
                    let arrow_type = json_value_to_arrow_type(val);
                    all_keys
                        .entry(key.clone())
                        .and_modify(|existing| {
                            // Widen type if there's a conflict
                            *existing = widen_types(existing, &arrow_type);
                        })
                        .or_insert(arrow_type);
                }
            }
        }
    }

    // Require at least 80% of sampled messages to be valid JSON objects
    if json_count < (sample_size * 80 / 100).max(1) {
        debug!(
            json_count,
            sample_size,
            "JSON inference failed — falling back to envelope schema"
        );
        return SchemaMode::Envelope;
    }

    if all_keys.is_empty() {
        return SchemaMode::Envelope;
    }

    // Build the Arrow schema: metadata columns + inferred payload columns
    let mut fields = vec![
        Field::new("offset", DataType::Int64, false),
        Field::new("publish_time", DataType::Int64, false),
        Field::new("producer_name", DataType::Utf8, false),
        Field::new("routing_key", DataType::Utf8, true),
    ];

    let mut field_names = Vec::new();
    let mut field_types = Vec::new();

    for (name, dtype) in &all_keys {
        // All inferred fields are nullable (a message may not have every key)
        fields.push(Field::new(name, dtype.clone(), true));
        field_names.push(name.clone());
        field_types.push(dtype.clone());
    }

    let schema = Arc::new(Schema::new(fields));

    debug!(
        inferred_columns = field_names.len(),
        columns = ?field_names,
        "JSON schema inference successful"
    );

    SchemaMode::InferredJson {
        schema,
        field_names,
        field_types,
    }
}

/// Map a JSON value to an Arrow DataType.
fn json_value_to_arrow_type(val: &serde_json::Value) -> DataType {
    match val {
        serde_json::Value::Bool(_) => DataType::Boolean,
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        serde_json::Value::String(_) => DataType::Utf8,
        // Arrays and nested objects → stringify as JSON text
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => DataType::Utf8,
        serde_json::Value::Null => DataType::Utf8, // Default null to string
    }
}

/// Widen two types to a compatible supertype.
fn widen_types(a: &DataType, b: &DataType) -> DataType {
    if a == b {
        return a.clone();
    }
    match (a, b) {
        // Int64 + Float64 → Float64
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
            DataType::Float64
        }
        // Any mismatch → fallback to Utf8 (stringify)
        _ => DataType::Utf8,
    }
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

/// Convert messages to a RecordBatch using an inferred JSON schema.
///
/// Message metadata (offset, publish_time, etc.) are always included.
/// JSON payload fields are extracted and placed into typed columns.
pub fn messages_to_inferred_batch(
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

    // Inferred payload columns — one builder per field
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
// Dynamic column builder (per inferred JSON field)
// ============================================================================

enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
}

impl ColumnBuilder {
    fn new(dt: &DataType, capacity: usize) -> Self {
        match dt {
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            _ => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 32)),
        }
    }

    fn append(&mut self, value: Option<&serde_json::Value>) {
        match self {
            Self::Boolean(b) => match value {
                Some(serde_json::Value::Bool(v)) => b.append_value(*v),
                Some(serde_json::Value::Null) | None => b.append_null(),
                _ => b.append_null(), // Type mismatch → null
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
