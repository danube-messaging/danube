//! Parquet writer — writes Arrow `RecordBatch` as Parquet files to object storage.
//!
//! Uses `parquet::arrow::async_writer::ParquetObjectWriter` which handles
//! multipart upload internally for large files.

use crate::storage::StorageHandle;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use object_store::path::Path;
use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tracing::{debug, info};
use uuid::Uuid;

/// Configuration for the Parquet writer.
pub struct WriterConfig {
    /// Output prefix within the storage root (e.g., "iceberg").
    pub output_prefix: String,
    /// Topic namespace (e.g., "default").
    pub namespace: String,
    /// Topic name (e.g., "sensor-data").
    pub topic: String,
}

impl WriterConfig {
    /// Generate the object path for a new Parquet file.
    ///
    /// Layout: `{output_prefix}/{namespace}/{topic}/data/part-{uuid}.parquet`
    fn parquet_path(&self, storage: &StorageHandle) -> Path {
        let file_name = format!("part-{}.parquet", Uuid::new_v4());
        let relative = format!(
            "{}/{}/{}/data/{}",
            self.output_prefix, self.namespace, self.topic, file_name
        );
        storage.path(&relative)
    }
}

/// Write a RecordBatch to a Parquet file on object storage.
///
/// Returns the object path of the written file and the number of rows.
pub async fn write_parquet(
    storage: &StorageHandle,
    config: &WriterConfig,
    schema: SchemaRef,
    batch: &RecordBatch,
) -> anyhow::Result<ParquetWriteResult> {
    let path = config.parquet_path(storage);

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(128 * 1024) // 128K rows per row group
        .build();

    debug!(
        path = %path,
        rows = batch.num_rows(),
        "writing parquet file"
    );

    // ParquetObjectWriter handles multipart upload internally
    let object_writer = ParquetObjectWriter::new(storage.store.clone(), path.clone());
    let mut writer = AsyncArrowWriter::try_new(object_writer, schema, Some(props))?;

    writer.write(batch).await?;
    let _file_meta = writer.close().await?;

    let num_rows = batch.num_rows();

    info!(
        path = %path,
        rows = num_rows,
        "parquet file written"
    );

    Ok(ParquetWriteResult {
        path: path.to_string(),
        num_rows,
    })
}

/// Result of a successful Parquet write.
pub struct ParquetWriteResult {
    /// Object store path of the written file.
    pub path: String,
    /// Number of rows in the file.
    pub num_rows: usize,
}
