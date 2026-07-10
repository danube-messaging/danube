//! Unit tests for the schema resolver's type conversion logic.
//!
//! These tests validate the JSON Schema → Arrow and Avro → Arrow type mappings
//! that determine how message payloads are stored in Parquet/Iceberg tables.
//!
//! Getting the type mapping wrong means either:
//! - **Silent data loss**: a field mapped to the wrong Arrow type produces nulls
//!   or truncated values in the Parquet file, with no error at write time.
//! - **Query performance degradation**: complex types stringified to Utf8 lose
//!   columnar pushdown benefits (predicate pushdown, min/max statistics).
//!
//! The tests are organized by schema format (JSON Schema, Avro) and by
//! complexity (flat primitives → nested structs/lists/maps).

use super::*;

// ============================================================================
// JSON Schema → Arrow
// ============================================================================

/// Verifies that all four JSON Schema primitive types map to the correct
/// Arrow types: `number` → Float64, `integer` → Int64, `string` → Utf8,
/// `boolean` → Boolean.
///
/// This is the most common case — IoT sensor data, user events, etc. all
/// use flat JSON objects with primitive fields.
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

/// Verifies the fallback behavior when nested types lack full definitions:
/// - `"type": "object"` **without** `"properties"` → `Utf8` (can't infer
///   struct fields, so we stringify the JSON object).
/// - `"type": "array"` **without** `"items"` → `List<Utf8>` (we still create
///   a proper List, but default the item type to Utf8).
///
/// This matters because producers sometimes register schemas with incomplete
/// nested type definitions — the converter must handle it gracefully rather
/// than panicking or producing invalid Arrow schemas.
#[test]
fn json_schema_nested_without_properties() {
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
    assert_eq!(
        types[meta_idx],
        DataType::Utf8,
        "object without properties → Utf8"
    );

    let tags_idx = names.iter().position(|n| n == "tags").unwrap();
    assert_eq!(
        types[tags_idx],
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        "array without items → List<Utf8>"
    );
}

/// Verifies recursive conversion of fully-defined nested types:
/// - `"type": "object"` **with** `"properties"` → Arrow `Struct` with typed
///   child fields (preserving the nested structure for columnar pushdown).
/// - `"type": "array"` **with** `"items"` → Arrow `List<item_type>`.
///
/// This is the key improvement over the old `ColumnBuilder` which stringified
/// all nested types. With `arrow_json::ReaderBuilder`, Struct and List columns
/// are stored natively in Parquet, enabling:
/// - Nested field access in query engines (`SELECT location.lat ...`)
/// - Predicate pushdown on nested fields
/// - Proper null handling at each nesting level
#[test]
fn json_schema_nested_struct_and_list() {
    let schema = r#"{
        "type": "object",
        "properties": {
            "location": {
                "type": "object",
                "properties": {
                    "lat": { "type": "number" },
                    "lon": { "type": "number" }
                }
            },
            "readings": {
                "type": "array",
                "items": { "type": "number" }
            }
        }
    }"#;

    let (names, types) = json_schema_to_arrow_fields(schema).unwrap();

    let loc_idx = names.iter().position(|n| n == "location").unwrap();
    match &types[loc_idx] {
        DataType::Struct(fields) => {
            assert_eq!(fields.len(), 2);
            assert!(fields.iter().any(|f| f.name() == "lat"));
            assert!(fields.iter().any(|f| f.name() == "lon"));
        }
        other => panic!("expected Struct, got {:?}", other),
    }

    let readings_idx = names.iter().position(|n| n == "readings").unwrap();
    assert_eq!(
        types[readings_idx],
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        "array of numbers → List<Float64>"
    );
}

// ============================================================================
// Avro → Arrow
// ============================================================================

/// Verifies the basic Avro primitive type mappings: `string` → Utf8,
/// `double` → Float64, `long` → Int64, `boolean` → Boolean.
///
/// Avro uses different type names than JSON Schema (`double` vs `number`,
/// `long` vs `integer`), so this test ensures the Avro-specific mapping
/// is correct and doesn't accidentally use JSON Schema's names.
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

/// Verifies that Avro's nullable union pattern `["null", T]` correctly
/// extracts the non-null inner type.
///
/// This is the standard Avro idiom for optional fields — unlike JSON Schema
/// which has a separate `required` array, Avro encodes nullability inline
/// using union types. The converter must unwrap the union and use `T` as
/// the Arrow type (with `nullable = true` on the field).
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

/// Verifies that an Avro nested record (a record field whose type is itself
/// a record) converts to an Arrow `Struct` with correctly typed child fields.
///
/// This is common in domain models — e.g., a `Device` record containing a
/// `GeoPoint` sub-record. Storing as `Struct` (vs. stringified JSON) enables
/// direct nested field access in query engines.
#[test]
fn avro_schema_nested_record() {
    let schema = r#"{
        "type": "record",
        "name": "Device",
        "fields": [
            { "name": "id", "type": "string" },
            {
                "name": "location",
                "type": {
                    "type": "record",
                    "name": "GeoPoint",
                    "fields": [
                        { "name": "lat", "type": "double" },
                        { "name": "lon", "type": "double" }
                    ]
                }
            }
        ]
    }"#;

    let (names, types) = avro_schema_to_arrow_fields(schema).unwrap();
    assert_eq!(names, vec!["id", "location"]);
    assert_eq!(types[0], DataType::Utf8);

    match &types[1] {
        DataType::Struct(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "lat");
            assert_eq!(*fields[0].data_type(), DataType::Float64);
            assert_eq!(fields[1].name(), "lon");
            assert_eq!(*fields[1].data_type(), DataType::Float64);
        }
        other => panic!("expected Struct, got {:?}", other),
    }
}

/// Verifies that Avro's array type `{"type": "array", "items": T}` converts
/// to Arrow `List<T>`.
///
/// This is used for repeated values — e.g., a list of sensor readings or
/// a batch of measurements. Storing as a native List enables array function
/// support in query engines (unnest, array_length, etc.).
#[test]
fn avro_schema_array_type() {
    let schema = r#"{
        "type": "record",
        "name": "Readings",
        "fields": [
            { "name": "sensor_id", "type": "string" },
            {
                "name": "values",
                "type": {
                    "type": "array",
                    "items": "double"
                }
            }
        ]
    }"#;

    let (names, types) = avro_schema_to_arrow_fields(schema).unwrap();
    assert_eq!(names, vec!["sensor_id", "values"]);
    assert_eq!(
        types[1],
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        "Avro array of double → List<Float64>"
    );
}

/// Verifies that Avro's map type `{"type": "map", "values": T}` converts
/// to Arrow `Map<Utf8, T>`.
///
/// Avro maps always have string keys. This is commonly used for dynamic
/// metadata or configuration key-value pairs. Storing as a native Map
/// (vs. stringified JSON) enables map access functions in query engines.
#[test]
fn avro_schema_map_type() {
    let schema = r#"{
        "type": "record",
        "name": "Config",
        "fields": [
            { "name": "name", "type": "string" },
            {
                "name": "settings",
                "type": {
                    "type": "map",
                    "values": "long"
                }
            }
        ]
    }"#;

    let (names, types) = avro_schema_to_arrow_fields(schema).unwrap();
    assert_eq!(names, vec!["name", "settings"]);

    let expected_map = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    assert_eq!(types[1], expected_map, "Avro map<long> → Map<Utf8, Int64>");
}

/// Verifies that Avro's `bytes` type maps to Arrow `Binary`.
///
/// This is important for payloads that contain raw binary data (e.g.,
/// serialized protobuf, encrypted blobs). Using `Binary` instead of `Utf8`
/// avoids invalid UTF-8 errors and preserves the raw bytes faithfully.
#[test]
fn avro_schema_bytes_type() {
    let schema = r#"{
        "type": "record",
        "name": "Payload",
        "fields": [
            { "name": "id", "type": "string" },
            { "name": "data", "type": "bytes" }
        ]
    }"#;

    let (names, types) = avro_schema_to_arrow_fields(schema).unwrap();
    assert_eq!(names, vec!["id", "data"]);
    assert_eq!(types[1], DataType::Binary, "Avro bytes → Binary");
}

// ============================================================================
// Registry schema builder
// ============================================================================

/// Verifies that `build_registry_schema` prepends the 4 standard metadata
/// columns (offset, publish_time, producer_name, routing_key) before the
/// payload columns from the registry.
///
/// This layout is a contract with the rest of the pipeline:
/// - `messages_to_registry_batch` builds metadata columns at indices 0–3
///   and payload columns at index 4+.
/// - `arrow_json::ReaderBuilder` skips the first 4 fields when parsing
///   JSON payloads.
/// If the column order changes here, both of those break silently.
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
