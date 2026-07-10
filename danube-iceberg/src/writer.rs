//! Iceberg data file writer — writes Arrow `RecordBatch` data through iceberg's
//! writer pipeline to produce Parquet files with full column-level statistics.
//!
//! Uses iceberg-rust's composable writer stack:
//! ```text
//! ParquetWriterBuilder → RollingFileWriterBuilder → DataFileWriterBuilder → DataFileWriter
//! ```
//!
//! The resulting `DataFile` metadata includes per-column statistics (min/max bounds,
//! null counts, value counts, column sizes) that enable query engines (Spark, Trino,
//! DuckDB) to perform predicate pushdown and file pruning.

use iceberg::spec::DataFile;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tracing::{debug, info};

/// Write a `RecordBatch` to one or more Parquet data files using iceberg's writer pipeline.
///
/// The writer uses the table's `FileIO` for storage and `metadata()` for path generation,
/// ensuring files are written to the standard Iceberg data directory with proper statistics.
///
/// Returns `Vec<DataFile>` — each with full per-column statistics ready for `fast_append`.
///
/// ## Rolling behavior
///
/// If the batch is large enough, `RollingFileWriter` will split it across multiple
/// Parquet files, each up to `target_file_size_bytes`.
pub async fn write_data_files(
    table: &iceberg::table::Table,
    batch: arrow_array::RecordBatch,
    target_file_size_bytes: usize,
) -> anyhow::Result<Vec<DataFile>> {
    let metadata = table.metadata();
    let iceberg_schema = metadata.current_schema().clone();
    let file_io = table.file_io().clone();

    let num_rows = batch.num_rows();

    // Parquet writer properties
    let writer_props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(128 * 1024) // 128K rows per row group
        .build();

    // Level 1: File format writer (Parquet)
    let parquet_builder = ParquetWriterBuilder::new(writer_props, iceberg_schema);

    // Level 2: Location + file name generators (standard Iceberg layout)
    let location_generator = DefaultLocationGenerator::new(metadata.clone())
        .map_err(|e| anyhow::anyhow!("failed to create location generator: {}", e))?;

    let file_name_generator = DefaultFileNameGenerator::new(
        "part".to_string(),
        Some(uuid::Uuid::new_v4().to_string()),
        iceberg::spec::DataFileFormat::Parquet,
    );

    // Level 3: Rolling file writer (handles size-based splitting)
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        target_file_size_bytes,
        file_io,
        location_generator,
        file_name_generator,
    );

    // Level 4: Data file writer (produces DataFile with full statistics)
    let data_file_builder = DataFileWriterBuilder::new(rolling_builder);

    let mut writer = data_file_builder
        .build(None) // No partition key (unpartitioned tables)
        .await
        .map_err(|e| anyhow::anyhow!("failed to build data file writer: {}", e))?;

    debug!(rows = num_rows, "writing data through iceberg writer");

    // Write the batch
    writer
        .write(batch)
        .await
        .map_err(|e| anyhow::anyhow!("iceberg writer failed: {}", e))?;

    // Close and get data files with full statistics
    let data_files = writer
        .close()
        .await
        .map_err(|e| anyhow::anyhow!("iceberg writer close failed: {}", e))?;

    for df in &data_files {
        info!(
            path = %df.file_path(),
            rows = df.record_count(),
            size_bytes = df.file_size_in_bytes(),
            "wrote iceberg data file"
        );
    }

    Ok(data_files)
}
