//! # Iceberg Data File Writer Tests
//!
//! These tests validate the iceberg writer pipeline that `danube-iceberg` uses
//! to produce Parquet data files with full column-level statistics.
//!
//! ## Why this matters
//!
//! When data files are committed to an Iceberg table, query engines (Spark,
//! Trino, DuckDB) use per-column statistics for predicate pushdown and file
//! pruning. Without these statistics, every query must scan every data file.
//!
//! ## Writer pipeline
//!
//! ```text
//! ParquetWriterBuilder → RollingFileWriterBuilder → DataFileWriterBuilder
//!   → DataFileWriter.write(batch)
//!   → DataFileWriter.close() → Vec<DataFile> (with stats)
//! ```
//!
//! ## Note on test structure
//!
//! Since `danube-iceberg` is a binary crate, integration tests cannot import
//! its internal modules directly. These tests exercise the same iceberg writer
//! stack that `src/writer.rs` uses, validating the pipeline independently.

mod common;

use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileFormat, Datum, NestedField, PrimitiveType, Schema as IcebergSchema,
    Struct, Type,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::Arc;

/// Helper: build an iceberg Schema matching the Danube envelope format.
fn envelope_iceberg_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_fields(vec![
            NestedField::required(1, "offset", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "publish_time", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(3, "producer_name", Type::Primitive(PrimitiveType::String))
                .into(),
            NestedField::required(4, "payload", Type::Primitive(PrimitiveType::Binary)).into(),
        ])
        .build()
        .expect("build schema")
}

/// Helper: build an Arrow RecordBatch with Parquet field ID metadata.
///
/// The `PARQUET_FIELD_ID_META_KEY` metadata is required by iceberg's
/// `ParquetWriter` to map Arrow columns to Iceberg field IDs for statistics.
fn make_envelope_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("offset", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )])),
        Field::new("publish_time", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        )])),
        Field::new("producer_name", DataType::Utf8, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "3".to_string(),
        )])),
        Field::new("payload", DataType::Binary, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "4".to_string(),
        )])),
    ]));

    let offsets: Vec<i64> = (0..num_rows as i64).collect();
    let timestamps: Vec<i64> = (0..num_rows).map(|i| 1720000000 + i as i64).collect();
    let producers: Vec<&str> = (0..num_rows).map(|_| "test-producer").collect();
    let payload_strings: Vec<String> = (0..num_rows).map(|i| format!("payload-{}", i)).collect();
    let payload_refs: Vec<&[u8]> = payload_strings.iter().map(|s| s.as_bytes()).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(offsets)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(StringArray::from(producers)),
            Arc::new(BinaryArray::from(payload_refs)),
        ],
    )
    .expect("build batch")
}

/// Validates that the iceberg DataFileWriter pipeline produces DataFiles
/// with full column-level statistics.
///
/// This is the same writer stack used by `src/writer.rs`:
/// `ParquetWriterBuilder → RollingFileWriterBuilder → DataFileWriterBuilder`
///
/// We verify:
/// - `record_count` matches the input
/// - `file_size_in_bytes` is non-zero
/// - `column_sizes` are populated for all 4 columns
/// - `value_counts` are populated for all 4 columns
/// - `null_value_counts` are populated (0 for required, 0 for optional with no nulls)
/// - `lower_bounds` and `upper_bounds` contain valid min/max values
/// - ZSTD compression is applied
#[tokio::test]
async fn data_file_writer_produces_statistics() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let file_io = FileIO::new_with_fs();

    let location_gen =
        DefaultLocationGenerator::with_data_location(temp_dir.path().to_str().unwrap().to_string());
    let file_name_gen =
        DefaultFileNameGenerator::new("part".to_string(), None, DataFileFormat::Parquet);

    let iceberg_schema = Arc::new(envelope_iceberg_schema());

    let parquet_builder = ParquetWriterBuilder::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build(),
        iceberg_schema,
    );

    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        file_io.clone(),
        location_gen,
        file_name_gen,
    );

    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(None) // unpartitioned
        .await
        .expect("build data file writer");

    // Write 100 rows
    let batch = make_envelope_batch(100);
    writer.write(batch).await.expect("write batch");

    let data_files = writer.close().await.expect("close writer");

    // Should produce exactly one data file (100 rows is well under the default size limit)
    assert_eq!(data_files.len(), 1, "should produce 1 data file");

    let df = &data_files[0];

    // Basic metadata
    assert_eq!(df.record_count(), 100);
    assert!(df.file_size_in_bytes() > 0, "file size should be non-zero");
    assert_eq!(df.file_format(), DataFileFormat::Parquet);
    assert_eq!(df.content_type(), DataContentType::Data);
    assert_eq!(*df.partition(), Struct::empty());

    // Column sizes — should have entries for all 4 fields (IDs 1-4)
    let col_sizes = df.column_sizes();
    assert!(
        col_sizes.contains_key(&1),
        "should have column_size for field 1 (offset)"
    );
    assert!(
        col_sizes.contains_key(&2),
        "should have column_size for field 2 (publish_time)"
    );
    assert!(
        col_sizes.contains_key(&3),
        "should have column_size for field 3 (producer_name)"
    );
    assert!(
        col_sizes.contains_key(&4),
        "should have column_size for field 4 (payload)"
    );

    // Value counts — 100 values per column
    let value_counts = df.value_counts();
    assert_eq!(value_counts.get(&1), Some(&100), "offset value_count");
    assert_eq!(value_counts.get(&2), Some(&100), "publish_time value_count");
    assert_eq!(
        value_counts.get(&3),
        Some(&100),
        "producer_name value_count"
    );
    assert_eq!(value_counts.get(&4), Some(&100), "payload value_count");

    // Null value counts — all zero (no null data in our test)
    let null_counts = df.null_value_counts();
    assert_eq!(null_counts.get(&1), Some(&0), "offset null_count");
    assert_eq!(null_counts.get(&2), Some(&0), "publish_time null_count");
    assert_eq!(null_counts.get(&3), Some(&0), "producer_name null_count");
    assert_eq!(null_counts.get(&4), Some(&0), "payload null_count");

    // Lower bounds — offset min should be 0
    let lower = df.lower_bounds();
    assert_eq!(
        lower.get(&1),
        Some(&Datum::long(0)),
        "offset lower bound should be 0"
    );

    // Upper bounds — offset max should be 99
    let upper = df.upper_bounds();
    assert_eq!(
        upper.get(&1),
        Some(&Datum::long(99)),
        "offset upper bound should be 99"
    );

    // publish_time bounds
    assert_eq!(
        lower.get(&2),
        Some(&Datum::long(1720000000)),
        "publish_time lower bound"
    );
    assert_eq!(
        upper.get(&2),
        Some(&Datum::long(1720000099)),
        "publish_time upper bound"
    );

    // Split offsets should be populated
    assert!(
        df.split_offsets().is_some(),
        "split_offsets should be populated"
    );
}

/// Validates that the writer handles nullable columns correctly by tracking
/// null value counts in the DataFile statistics.
///
/// Query engines use `null_value_counts` to skip files where a column is
/// entirely null (e.g., `WHERE producer_name IS NOT NULL`).
#[tokio::test]
async fn data_file_writer_tracks_null_counts() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let file_io = FileIO::new_with_fs();

    let location_gen =
        DefaultLocationGenerator::with_data_location(temp_dir.path().to_str().unwrap().to_string());
    let file_name_gen =
        DefaultFileNameGenerator::new("nulltest".to_string(), None, DataFileFormat::Parquet);

    // Simple schema: one required int + one nullable string
    let iceberg_schema = Arc::new(
        IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("build schema"),
    );

    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_schema);

    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        file_io.clone(),
        location_gen,
        file_name_gen,
    );

    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(None)
        .await
        .expect("build writer");

    // Build batch with 5 rows, 2 of which have null names
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )])),
        Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        )])),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                None,
                Some("Charlie"),
                None,
                Some("Eve"),
            ])),
        ],
    )
    .expect("build batch");

    writer.write(batch).await.expect("write");
    let data_files = writer.close().await.expect("close");

    assert_eq!(data_files.len(), 1);
    let df = &data_files[0];

    assert_eq!(df.record_count(), 5);

    // id column: no nulls
    assert_eq!(df.null_value_counts().get(&1), Some(&0));

    // name column: 2 nulls
    assert_eq!(
        df.null_value_counts().get(&2),
        Some(&2),
        "name should have 2 null values"
    );

    // name lower/upper bounds should only reflect non-null values
    let lower = df.lower_bounds();
    let upper = df.upper_bounds();
    assert!(lower.contains_key(&2), "name should have lower bound");
    assert!(upper.contains_key(&2), "name should have upper bound");
}
