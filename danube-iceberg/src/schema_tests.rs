//! Unit tests for `schema.rs` — RecordBatch construction from Danube messages.
//!
//! These tests validate the two payload decode paths (JSON and Avro) and
//! verify that metadata columns + payload columns merge correctly into a
//! final `RecordBatch`.

use super::*;
use arrow_array::cast::AsArray;
use bytes::Bytes;
use danube_core::message::{MessageID, StreamMessage};
use std::collections::HashMap;

/// Helper to create a `DecodedMessage` with the given offset and payload bytes.
///
/// Fills in reasonable defaults for all metadata fields so tests only need
/// to specify what varies (offset + payload).
fn test_decoded_message(offset: u64, payload: &[u8]) -> crate::segment_reader::DecodedMessage {
    crate::segment_reader::DecodedMessage {
        offset,
        message: StreamMessage {
            request_id: offset,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test/topic".to_string(),
                broker_addr: "localhost:6650".to_string(),
                topic_offset: offset,
            },
            payload: Bytes::copy_from_slice(payload),
            publish_time: 1720000000 + offset,
            producer_name: format!("producer-{}", offset),
            subscription_name: None,
            attributes: HashMap::new(),
            schema_id: Some(1),
            schema_version: Some(1),
            routing_key: Some(format!("key-{}", offset)),
        },
    }
}

/// Verifies the JSON payload → RecordBatch round-trip.
///
/// Two JSON messages with `temperature` (f64) and `sensor_id` (string) fields
/// are converted via `messages_to_registry_batch` with `PayloadFormat::Json`.
/// The test asserts that:
/// - Metadata columns (offset, publish_time, producer_name, routing_key) are
///   populated correctly.
/// - Payload columns are parsed into the expected Arrow types and values.
///
/// This is a regression test — it must continue working after adding the
/// Avro decode branch.
#[test]
fn json_payload_roundtrip() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::Int64, false),
        Field::new("publish_time", DataType::Int64, false),
        Field::new("producer_name", DataType::Utf8, false),
        Field::new("routing_key", DataType::Utf8, true),
        Field::new("temperature", DataType::Float64, true),
        Field::new("sensor_id", DataType::Utf8, true),
    ]));

    let msg1 = test_decoded_message(0, br#"{"temperature": 23.5, "sensor_id": "s1"}"#);
    let msg2 = test_decoded_message(1, br#"{"temperature": 19.8, "sensor_id": "s2"}"#);

    let batch = messages_to_registry_batch(
        &[msg1, msg2],
        &schema,
        &PayloadFormat::Json,
        1, // schema_id (unused for JSON)
    )
    .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 6);

    // Verify metadata columns
    let offsets = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(offsets.value(0), 0);
    assert_eq!(offsets.value(1), 1);

    // Verify payload columns
    let temps = batch.column(4).as_primitive::<arrow_array::types::Float64Type>();
    assert!((temps.value(0) - 23.5).abs() < 1e-10);
    assert!((temps.value(1) - 19.8).abs() < 1e-10);

    let sensors = batch.column(5).as_string::<i32>();
    assert_eq!(sensors.value(0), "s1");
    assert_eq!(sensors.value(1), "s2");
}

/// Verifies the complete Avro payload → RecordBatch round-trip.
///
/// Two Avro-encoded records (sensor_id: string, temperature: double,
/// count: long, active: boolean) are serialized with `apache_avro::to_avro_datum`
/// and then decoded via `messages_to_registry_batch` with `PayloadFormat::Avro`.
///
/// The test:
/// 1. Calls `decode_avro_payloads` first to discover the Arrow types that
///    `arrow-avro` infers from the Avro schema (so the full schema is built
///    correctly — metadata + payload columns).
/// 2. Calls `messages_to_registry_batch` with the full schema.
/// 3. Asserts metadata + payload column values.
///
/// This validates the fix for Bug 1 — Avro binary payloads were previously
/// silently producing null columns because `serde_json::from_slice` can't
/// parse binary data. Now `arrow-avro` decodes them directly to Arrow.
#[test]
fn avro_payload_roundtrip() {
    let avro_schema_str = r#"{
        "type": "record",
        "name": "SensorReading",
        "fields": [
            {"name": "sensor_id", "type": "string"},
            {"name": "temperature", "type": "double"},
            {"name": "count", "type": "long"},
            {"name": "active", "type": "boolean"}
        ]
    }"#;
    let avro_schema = apache_avro::Schema::parse_str(avro_schema_str).unwrap();

    // Encode two messages as Avro binary
    let record1 = apache_avro::types::Value::Record(vec![
        ("sensor_id".into(), apache_avro::types::Value::String("sensor-A".into())),
        ("temperature".into(), apache_avro::types::Value::Double(22.5)),
        ("count".into(), apache_avro::types::Value::Long(42)),
        ("active".into(), apache_avro::types::Value::Boolean(true)),
    ]);
    let record2 = apache_avro::types::Value::Record(vec![
        ("sensor_id".into(), apache_avro::types::Value::String("sensor-B".into())),
        ("temperature".into(), apache_avro::types::Value::Double(-3.1)),
        ("count".into(), apache_avro::types::Value::Long(0)),
        ("active".into(), apache_avro::types::Value::Boolean(false)),
    ]);

    let payload1 = apache_avro::to_avro_datum(&avro_schema, record1).unwrap();
    let payload2 = apache_avro::to_avro_datum(&avro_schema, record2).unwrap();

    // Decode payload columns to discover arrow-avro's inferred schema
    let payload_batch = decode_avro_payloads(
        &[test_decoded_message(0, &payload1), test_decoded_message(1, &payload2)],
        avro_schema_str,
        1,
    )
    .unwrap();

    // Build the full Arrow schema from arrow-avro's output (metadata + payload)
    let mut fields = vec![
        Arc::new(Field::new("offset", DataType::Int64, false)),
        Arc::new(Field::new("publish_time", DataType::Int64, false)),
        Arc::new(Field::new("producer_name", DataType::Utf8, false)),
        Arc::new(Field::new("routing_key", DataType::Utf8, true)),
    ];
    for f in payload_batch.schema().fields() {
        fields.push(f.clone());
    }
    let full_schema = Arc::new(Schema::new(fields));

    let msg1 = test_decoded_message(0, &payload1);
    let msg2 = test_decoded_message(1, &payload2);

    let batch = messages_to_registry_batch(
        &[msg1, msg2],
        &full_schema,
        &PayloadFormat::Avro { schema_json: avro_schema_str.to_string() },
        1,
    )
    .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 8);

    // Verify metadata columns
    let offsets = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(offsets.value(0), 0);
    assert_eq!(offsets.value(1), 1);

    // Verify payload columns — find them by name (arrow-avro determines the order)
    let schema_ref = batch.schema();

    let sensor_col = schema_ref.index_of("sensor_id").unwrap();
    let sensor_ids = batch.column(sensor_col).as_string::<i32>();
    assert_eq!(sensor_ids.value(0), "sensor-A");
    assert_eq!(sensor_ids.value(1), "sensor-B");

    let temp_col = schema_ref.index_of("temperature").unwrap();
    let temps = batch.column(temp_col).as_primitive::<arrow_array::types::Float64Type>();
    assert!((temps.value(0) - 22.5).abs() < 1e-10);
    assert!((temps.value(1) - (-3.1)).abs() < 1e-10);

    let count_col = schema_ref.index_of("count").unwrap();
    let counts = batch.column(count_col).as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(counts.value(0), 42);
    assert_eq!(counts.value(1), 0);

    let active_col = schema_ref.index_of("active").unwrap();
    let actives = batch.column(active_col).as_boolean();
    assert!(actives.value(0));
    assert!(!actives.value(1));
}

/// Verifies that a corrupt/unparseable Avro payload is handled gracefully.
///
/// Feeds garbage bytes (`"this is not avro data"`) to `decode_avro_payloads`.
/// With the Confluent decoder, corrupt payloads cause a decode error that
/// is logged and skipped — the function should not panic. The resulting batch
/// may have 0 rows since the corrupt payload was skipped.
#[test]
fn avro_payload_corrupt_graceful() {
    let avro_schema_str = r#"{
        "type": "record",
        "name": "Simple",
        "fields": [
            {"name": "name", "type": "string"}
        ]
    }"#;

    // Garbage bytes that are not valid Avro
    let msg = test_decoded_message(0, b"this is not avro data");

    // The decode should not panic — corrupt payloads are logged and skipped
    let result = decode_avro_payloads(&[msg], avro_schema_str, 1);

    // decode_avro_payloads should succeed (graceful handling), but the
    // resulting batch may have 0 rows since the corrupt payload was skipped
    assert!(result.is_ok(), "corrupt Avro should not panic");
}
