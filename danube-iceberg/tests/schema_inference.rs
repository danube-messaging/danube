//! # Schema Inference and RecordBatch Conversion Tests
//!
//! These tests validate the two core data transformation steps in `danube-iceberg`:
//!
//! 1. **Schema Inference** — Automatically determining the Arrow columnar schema
//!    from the JSON payloads inside Danube messages. This is what enables
//!    schema-less topics (where producers just send JSON) to become queryable
//!    Parquet tables without any manual schema definition.
//!
//! 2. **RecordBatch Conversion** — Transforming decoded `StreamMessage` values
//!    into Apache Arrow `RecordBatch` (the in-memory columnar format that
//!    Parquet is built on). This step must correctly handle:
//!    - Message metadata (offset, publish_time, producer_name, routing_key)
//!    - Optional fields (schema_id, routing_key → nullable Arrow columns)
//!    - User-defined attributes (HashMap → JSON string column)
//!    - Payload extraction (raw binary for envelope, typed columns for JSON)
//!
//! ## Why this matters
//!
//! If schema inference produces wrong types (e.g., Int64 instead of Float64),
//! downstream queries will return incorrect results. If the RecordBatch builder
//! mishandles nulls, Parquet readers will crash or return garbage. These tests
//! catch both classes of bugs.
//!
//! ## Design note
//!
//! Since `danube-iceberg` is a binary crate (not a library), the tests can't
//! import its internal modules directly. Instead, we re-implement the core
//! schema logic in an inline `danube_iceberg_schema` module at the bottom of
//! this file. This mirrors the production code 1:1 and validates the same
//! algorithms.

mod common;

use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use common::{
    make_json_message, make_rich_message, make_test_message,
};
use std::collections::HashMap;

/// A decoded message mirror for tests (mirrors segment_reader::DecodedMessage).
/// In production, this comes from the segment reader after decoding WAL frames.
struct DecodedMessage {
    offset: u64,
    message: danube_core::message::StreamMessage,
}

/// Helper: build decoded messages from StreamMessages, using each message's
/// topic_offset as the WAL offset.
fn to_decoded(messages: &[danube_core::message::StreamMessage]) -> Vec<DecodedMessage> {
    messages
        .iter()
        .map(|m| DecodedMessage {
            offset: m.msg_id.topic_offset,
            message: m.clone(),
        })
        .collect()
}

// ============================================================================
// Schema Inference Tests
//
// These verify that the inference engine correctly maps JSON value types to
// Arrow data types. This is what determines the Parquet column schema.
// ============================================================================

/// Verifies that JSON object payloads with float and string fields are
/// correctly inferred as `Float64` and `Utf8` Arrow columns.
///
/// This is the most common use case: IoT sensors, application events, and
/// log messages are typically JSON objects with a mix of numeric and string
/// fields. The inference engine must sample the payloads and build a schema
/// that maps each JSON key to the appropriate Arrow data type.
#[test]
fn infer_schema_json_objects() {
    let messages: Vec<_> = (0..10u64)
        .map(|i| {
            let json = serde_json::json!({
                "temperature": 22.5 + i as f64,
                "sensor": format!("sensor-{}", i)
            });
            make_json_message(i, &json)
        })
        .collect();

    let schema_mode = danube_iceberg_schema::infer_schema(&messages);

    match schema_mode {
        danube_iceberg_schema::SchemaMode::InferredJson {
            field_names,
            field_types,
            ..
        } => {
            assert!(field_names.contains(&"temperature".to_string()));
            assert!(field_names.contains(&"sensor".to_string()));

            let temp_idx = field_names.iter().position(|n| n == "temperature").unwrap();
            let sensor_idx = field_names.iter().position(|n| n == "sensor").unwrap();

            assert_eq!(field_types[temp_idx], DataType::Float64);
            assert_eq!(field_types[sensor_idx], DataType::Utf8);
        }
        other => panic!("expected InferredJson, got {:?}", other),
    }
}

/// Verifies that when the same JSON key appears as both integer and float
/// across different messages, the inference engine widens the type to Float64.
///
/// This is critical for real-world data where producers might send `{"value": 42}`
/// (parsed as Int64) in one message and `{"value": 3.14}` (Float64) in the next.
/// Without widening, the Int64 column would lose the fractional part of float
/// values, producing silently incorrect data.
#[test]
fn infer_schema_mixed_types_widening() {
    // Mix integer and float for the same key → should widen to Float64
    let mut messages = Vec::new();
    for i in 0..10u64 {
        let json = if i % 2 == 0 {
            serde_json::json!({"value": 42_i64})
        } else {
            serde_json::json!({"value": 3.14})
        };
        messages.push(make_json_message(i, &json));
    }

    let schema_mode = danube_iceberg_schema::infer_schema(&messages);

    match schema_mode {
        danube_iceberg_schema::SchemaMode::InferredJson {
            field_names,
            field_types,
            ..
        } => {
            let val_idx = field_names.iter().position(|n| n == "value").unwrap();
            assert_eq!(
                field_types[val_idx],
                DataType::Float64,
                "Int64 + Float64 should widen to Float64"
            );
        }
        other => panic!("expected InferredJson, got {:?}", other),
    }
}

/// Verifies that nested JSON objects (e.g., `{"meta": {"firmware": "v1.2"}}`) are
/// stored as JSON-stringified `Utf8` columns rather than attempting nested Arrow
/// structs.
///
/// Arrow does support nested StructArray, but for the initial implementation we
/// stringify complex values. This is simpler, avoids schema explosion from deeply
/// nested data, and still lets users query nested fields with JSON functions in
/// DuckDB/Trino (`json_extract(meta, '$.firmware')`).
#[test]
fn infer_schema_nested_objects_stringified() {
    let messages: Vec<_> = (0..10u64)
        .map(|i| {
            let json = serde_json::json!({
                "name": "device",
                "meta": {"firmware": "v1.2", "build": i}
            });
            make_json_message(i, &json)
        })
        .collect();

    let schema_mode = danube_iceberg_schema::infer_schema(&messages);

    match schema_mode {
        danube_iceberg_schema::SchemaMode::InferredJson {
            field_names,
            field_types,
            ..
        } => {
            let meta_idx = field_names.iter().position(|n| n == "meta").unwrap();
            assert_eq!(
                field_types[meta_idx],
                DataType::Utf8,
                "nested objects should be stringified as Utf8"
            );
        }
        other => panic!("expected InferredJson, got {:?}", other),
    }
}

/// Verifies that non-JSON payloads (raw binary) trigger a fallback to the
/// envelope schema.
///
/// Not all Danube topics use JSON — some send Protobuf, Avro, or raw binary.
/// When the inference engine can't parse payloads as JSON objects, it must
/// fall back to the universal "envelope" schema that stores payloads as
/// opaque binary blobs. This ensures all topics can be exported, even without
/// JSON.
#[test]
fn infer_schema_non_json_fallback() {
    let messages: Vec<_> = (0..10u64)
        .map(|i| make_test_message(i, &[0xFF, 0xFE, 0xFD])) // binary payloads
        .collect();

    let schema_mode = danube_iceberg_schema::infer_schema(&messages);

    match schema_mode {
        danube_iceberg_schema::SchemaMode::Envelope => {} // expected
        other => panic!("expected Envelope for binary payloads, got {:?}", other),
    }
}

/// Verifies the 80% threshold for JSON inference: if fewer than 80% of
/// sampled messages are valid JSON objects, the engine falls back to envelope.
///
/// This threshold prevents the inference engine from being fooled by topics
/// where most messages are binary but a few happen to be valid JSON (e.g.,
/// error messages mixed with binary sensor data). A conservative threshold
/// ensures we don't create a JSON-columnar schema that would produce mostly
/// null columns.
#[test]
fn infer_schema_mixed_json_binary_threshold() {
    // 70% JSON (below 80% threshold) → should fall back to Envelope
    let mut messages = Vec::new();
    for i in 0..10u64 {
        if i < 7 {
            let json = serde_json::json!({"key": "value"});
            messages.push(make_json_message(i, &json));
        } else {
            messages.push(make_test_message(i, &[0xFF, 0xFE]));
        }
    }

    let schema_mode = danube_iceberg_schema::infer_schema(&messages);

    match schema_mode {
        danube_iceberg_schema::SchemaMode::Envelope => {} // expected — 70% < 80% threshold
        other => panic!(
            "expected Envelope for 70% JSON (below 80% threshold), got {:?}",
            other
        ),
    }
}

// ============================================================================
// RecordBatch Conversion Tests
//
// These verify that decoded messages are correctly transformed into Arrow
// RecordBatch — the in-memory columnar format that gets written to Parquet.
// Each test covers a specific aspect: data fidelity, null handling, optional
// fields, user attributes, and typed JSON columns.
// ============================================================================

/// Verifies the envelope schema roundtrip: 5 messages with binary payloads
/// are converted to a RecordBatch with the expected 8 columns and all values
/// correctly populated.
///
/// The envelope schema is the universal fallback — it works for any message
/// type and preserves all data (offset, timestamps, producer, payload, etc.).
/// This test ensures no data is lost or garbled during the conversion.
#[test]
fn envelope_batch_roundtrip() {
    let messages: Vec<_> = (0..5u64)
        .map(|i| make_test_message(i, format!("msg-{}", i).as_bytes()))
        .collect();
    let decoded = to_decoded(&messages);

    let batch = danube_iceberg_schema::messages_to_envelope_batch(&decoded).expect("build batch");

    assert_eq!(batch.num_rows(), 5);
    assert_eq!(batch.num_columns(), 8); // offset, publish_time, producer_name, routing_key, schema_id, schema_version, payload, attributes_json

    // Check offsets column
    let offsets = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
    for i in 0..5 {
        assert_eq!(offsets.value(i), i as i64);
    }

    // Check producer_name column
    let producers = batch.column(2).as_string::<i32>();
    for i in 0..5 {
        assert_eq!(producers.value(i), "test-producer-1");
    }

    // Check payload column
    let payloads = batch.column(6).as_binary::<i32>();
    for i in 0..5 {
        assert_eq!(payloads.value(i), format!("msg-{}", i).as_bytes());
    }
}

/// Verifies that messages with `None` optional fields (routing_key, schema_id)
/// produce null values in the corresponding Arrow columns.
///
/// This is important because Arrow distinguishes between "empty string" and
/// "null" — downstream queries like `WHERE routing_key IS NOT NULL` depend on
/// correct null handling. A bug here would cause incorrect query results.
#[test]
fn envelope_batch_optional_fields() {
    // Messages with None routing_key and None schema_id
    let messages: Vec<_> = (0..3u64)
        .map(|i| make_test_message(i, b"payload"))
        .collect();
    let decoded = to_decoded(&messages);

    let batch = danube_iceberg_schema::messages_to_envelope_batch(&decoded).expect("build batch");

    // routing_key column (index 3) should be all null
    let routing_keys = batch.column(3);
    for i in 0..3 {
        assert!(routing_keys.is_null(i), "routing_key should be null");
    }

    // schema_id column (index 4) should be all null
    let schema_ids = batch.column(4);
    for i in 0..3 {
        assert!(schema_ids.is_null(i), "schema_id should be null");
    }
}

/// Verifies that messages with populated optional fields (routing_key, schema_id)
/// produce the correct non-null values in the Arrow columns.
///
/// This is the complement to `envelope_batch_optional_fields` — together they
/// ensure that nullable columns correctly handle both the Some and None cases.
#[test]
fn envelope_batch_with_populated_optional_fields() {
    let messages: Vec<_> = (0..3u64)
        .map(|i| make_rich_message(i, b"payload", Some("key-1"), Some(42)))
        .collect();
    let decoded = to_decoded(&messages);

    let batch = danube_iceberg_schema::messages_to_envelope_batch(&decoded).expect("build batch");

    // routing_key column should be populated
    let routing_keys = batch.column(3).as_string::<i32>();
    for i in 0..3 {
        assert_eq!(routing_keys.value(i), "key-1");
    }

    // schema_id column should be populated
    let schema_ids = batch.column(4).as_primitive::<arrow_array::types::Int64Type>();
    for i in 0..3 {
        assert_eq!(schema_ids.value(i), 42);
    }
}

/// Verifies that user-defined message attributes (HashMap<String, String>)
/// are serialized as a JSON string in the `attributes_json` column.
///
/// Attributes are an important Danube feature for message-level metadata
/// (e.g., `{"env": "production", "region": "us-east-1"}`). We serialize them
/// as JSON text rather than separate columns because the key set varies per
/// message. This test verifies the JSON is valid and round-trips correctly.
#[test]
fn envelope_batch_with_attributes() {
    let mut msg = make_test_message(0, b"payload");
    msg.attributes
        .insert("env".to_string(), "production".to_string());
    msg.attributes
        .insert("region".to_string(), "us-east-1".to_string());
    let decoded = to_decoded(&[msg]);

    let batch = danube_iceberg_schema::messages_to_envelope_batch(&decoded).expect("build batch");

    // attributes_json column (index 7) should contain valid JSON
    let attrs = batch.column(7).as_string::<i32>();
    let json_str = attrs.value(0);
    let parsed: HashMap<String, String> =
        serde_json::from_str(json_str).expect("parse attributes json");
    assert_eq!(parsed.get("env").unwrap(), "production");
    assert_eq!(parsed.get("region").unwrap(), "us-east-1");
}

/// Verifies the full JSON-inferred schema pipeline: messages with typed JSON
/// fields (Float64, Boolean, Utf8) are inferred and then converted to a
/// RecordBatch with correctly typed columns.
///
/// This is the most important schema test because it validates the entire
/// flow: inference → schema construction → RecordBatch building → data
/// extraction. It checks:
/// - Column count: 4 metadata + 3 inferred = 7
/// - Float64 column values match the JSON numbers
/// - Boolean column values match the JSON booleans  
/// - Utf8 column values match the JSON strings
///
/// A failure here would mean Parquet files contain wrong data types, which
/// would cause query errors or silent data corruption in analytics engines.
#[test]
fn inferred_batch_roundtrip() {
    let messages: Vec<_> = (0..5u64)
        .map(|i| {
            let json = serde_json::json!({
                "temperature": 20.0 + i as f64,
                "active": i % 2 == 0,
                "sensor": format!("s-{}", i)
            });
            make_json_message(i, &json)
        })
        .collect();

    let schema_mode = danube_iceberg_schema::infer_schema(&messages);

    match schema_mode {
        danube_iceberg_schema::SchemaMode::InferredJson {
            schema,
            field_names,
            field_types,
        } => {
            let decoded = to_decoded(&messages);
            let batch = danube_iceberg_schema::messages_to_inferred_batch(
                &decoded,
                &field_names,
                &field_types,
                &schema,
            )
            .expect("build batch");

            assert_eq!(batch.num_rows(), 5);

            // Metadata columns (4) + inferred columns (3) = 7
            assert_eq!(batch.num_columns(), 7);

            // Verify offsets
            let offsets = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
            for i in 0..5 {
                assert_eq!(offsets.value(i), i as i64);
            }

            // Find inferred column positions
            let temp_col_idx = 4 + field_names.iter().position(|n| n == "temperature").unwrap();
            let active_col_idx = 4 + field_names.iter().position(|n| n == "active").unwrap();
            let sensor_col_idx = 4 + field_names.iter().position(|n| n == "sensor").unwrap();

            // Verify temperature values (Float64)
            let temps = batch
                .column(temp_col_idx)
                .as_primitive::<arrow_array::types::Float64Type>();
            assert!((temps.value(0) - 20.0).abs() < 0.001);
            assert!((temps.value(4) - 24.0).abs() < 0.001);

            // Verify active values (Boolean)
            let actives = batch.column(active_col_idx).as_boolean();
            assert!(actives.value(0)); // 0 % 2 == 0 → true
            assert!(!actives.value(1)); // 1 % 2 != 0 → false

            // Verify sensor values (Utf8)
            let sensors = batch.column(sensor_col_idx).as_string::<i32>();
            assert_eq!(sensors.value(0), "s-0");
            assert_eq!(sensors.value(4), "s-4");
        }
        other => panic!("expected InferredJson, got {:?}", other),
    }
}

// ============================================================================
// Inline schema helpers module
//
// Since danube-iceberg is a binary crate (not a library), integration tests
// can't import its internal modules. We re-implement the core schema logic
// here as a test-only module. This mirrors the production code in
// danube-iceberg/src/schema.rs 1:1, so any test failure here indicates a
// real bug in the production code.
// ============================================================================
mod danube_iceberg_schema {
    use arrow_array::builder::{
        BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    };
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    pub enum SchemaMode {
        Envelope,
        InferredJson {
            schema: Arc<Schema>,
            field_names: Vec<String>,
            field_types: Vec<DataType>,
        },
    }

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

    pub fn infer_schema(messages: &[danube_core::message::StreamMessage]) -> SchemaMode {
        if messages.is_empty() {
            return SchemaMode::Envelope;
        }
        let sample_size = messages.len().min(100);
        let sample = &messages[..sample_size];
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
                                *existing = widen_types(existing, &arrow_type);
                            })
                            .or_insert(arrow_type);
                    }
                }
            }
        }
        if json_count < (sample_size * 80 / 100).max(1) {
            return SchemaMode::Envelope;
        }
        if all_keys.is_empty() {
            return SchemaMode::Envelope;
        }
        let mut fields = vec![
            Field::new("offset", DataType::Int64, false),
            Field::new("publish_time", DataType::Int64, false),
            Field::new("producer_name", DataType::Utf8, false),
            Field::new("routing_key", DataType::Utf8, true),
        ];
        let mut field_names = Vec::new();
        let mut field_types = Vec::new();
        for (name, dtype) in &all_keys {
            fields.push(Field::new(name, dtype.clone(), true));
            field_names.push(name.clone());
            field_types.push(dtype.clone());
        }
        let schema = Arc::new(Schema::new(fields));
        SchemaMode::InferredJson {
            schema,
            field_names,
            field_types,
        }
    }

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
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => DataType::Utf8,
            serde_json::Value::Null => DataType::Utf8,
        }
    }

    fn widen_types(a: &DataType, b: &DataType) -> DataType {
        if a == b {
            return a.clone();
        }
        match (a, b) {
            (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
                DataType::Float64
            }
            _ => DataType::Utf8,
        }
    }

    pub fn messages_to_envelope_batch(
        messages: &[super::DecodedMessage],
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
                    _ => b.append_null(),
                },
                Self::Utf8(b) => match value {
                    Some(serde_json::Value::String(s)) => b.append_value(s),
                    Some(serde_json::Value::Null) | None => b.append_null(),
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

    pub fn messages_to_inferred_batch(
        messages: &[super::DecodedMessage],
        field_names: &[String],
        field_types: &[DataType],
        schema: &Arc<Schema>,
    ) -> anyhow::Result<RecordBatch> {
        let n = messages.len();
        let mut offsets = Int64Builder::with_capacity(n);
        let mut publish_times = Int64Builder::with_capacity(n);
        let mut producers = StringBuilder::with_capacity(n, n * 32);
        let mut routing_keys = StringBuilder::with_capacity(n, n * 16);
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
}
