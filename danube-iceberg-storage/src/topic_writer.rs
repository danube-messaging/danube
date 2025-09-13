use crate::catalog::{create_danube_schema, DataFile, IcebergCatalog, Snapshot, TableMetadata};
use crate::errors::{IcebergStorageError, Result};
use crate::wal::WalReader;
use arrow::array::{
    ArrayRef, BinaryArray, Int64Array, StringArray, TimestampMillisecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use danube_core::message::StreamMessage;
use object_store::{path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::config::WriterConfig;

/// TopicWriter handles asynchronous flushing from WAL to Iceberg tables
pub struct TopicWriter {
    /// Topic name
    topic_name: String,
    /// WAL reader for reading entries
    wal_reader: Arc<tokio::sync::Mutex<WalReader>>,
    /// Iceberg catalog
    catalog: Arc<dyn IcebergCatalog>,
    /// Object store for writing Parquet files
    object_store: Arc<dyn ObjectStore>,
    /// Current batch of messages
    current_batch: Vec<StreamMessage>,
    /// Batch size threshold
    batch_size: usize,
    /// Batch timeout
    batch_timeout: Duration,
    /// Last flush time
    last_flush: SystemTime,
    /// Shutdown signal
    shutdown_rx: mpsc::Receiver<()>,
    /// Table metadata cache
    table_metadata: Arc<RwLock<Option<TableMetadata>>>,
    /// Warehouse base location (e.g., s3://bucket/prefix)
    warehouse: String,
    /// Max memory budget for in-flight batch (bytes)
    max_memory_bytes: u64,
    /// Current batch estimated size (bytes)
    current_batch_bytes: u64,
}

impl TopicWriter {
    /// Create a new TopicWriter
    pub async fn new(
        topic_name: String,
        wal_reader: Arc<tokio::sync::Mutex<WalReader>>,
        catalog: Arc<dyn IcebergCatalog>,
        object_store: Arc<dyn ObjectStore>,
        shutdown_rx: mpsc::Receiver<()>,
        writer_cfg: &WriterConfig,
        warehouse: &str,
    ) -> Result<Self> {
        Ok(Self {
            topic_name,
            wal_reader,
            catalog,
            object_store,
            current_batch: Vec::new(),
            batch_size: writer_cfg.batch_size,
            batch_timeout: Duration::from_millis(writer_cfg.flush_interval_ms),
            last_flush: SystemTime::now(),
            shutdown_rx,
            table_metadata: Arc::new(RwLock::new(None)),
            warehouse: warehouse.to_string(),
            max_memory_bytes: writer_cfg.max_memory_bytes,
            current_batch_bytes: 0,
        })
    }

    /// Start the background processing loop
    pub async fn start(mut self) -> Result<()> {
        info!(topic = %self.topic_name, "Starting TopicWriter");

        // Ensure table exists
        self.ensure_table_exists().await?;

        let mut flush_interval = interval(self.batch_timeout);

        loop {
            // Process WAL entries first
            if let Err(e) = self.process_wal_entries().await {
                error!(topic = %self.topic_name, error = %e, "Failed to process WAL entries");
            }

            tokio::select! {
                // Check for shutdown signal
                shutdown_result = self.shutdown_rx.recv() => {
                    if shutdown_result.is_some() {
                        info!(topic = %self.topic_name, "TopicWriter received shutdown signal");
                        break;
                    }
                }

                // Periodic flush
                _ = flush_interval.tick() => {
                    if !self.current_batch.is_empty() {
                        if let Err(e) = self.flush_batch().await {
                            error!(topic = %self.topic_name, error = %e, "Failed to flush batch");
                        }
                    }
                }
            }
        }

        // Final flush before shutdown
        if !self.current_batch.is_empty() {
            if let Err(e) = self.flush_batch().await {
                error!(topic = %self.topic_name, error = %e, "Failed to flush final batch");
            }
        }

        info!(topic = %self.topic_name, "TopicWriter stopped");
        Ok(())
    }

    /// Process entries from the WAL
    async fn process_wal_entries(&mut self) -> Result<()> {
        let mut wal_reader = self.wal_reader.lock().await;

        // Read next entry from WAL
        if let Some(entry) = wal_reader.read_next().await? {
            match entry {
                crate::wal::WalEntry::Message(message) => {
                    let est = Self::estimate_message_size(&message) as u64;

                    // If adding this message would exceed memory budget, flush current batch first
                    if self.current_batch_bytes + est > self.max_memory_bytes {
                        drop(wal_reader); // Release lock before async flush
                        self.flush_batch().await?;
                        wal_reader = self.wal_reader.lock().await;
                    }

                    // Add to batch
                    self.current_batch_bytes = self.current_batch_bytes.saturating_add(est);
                    self.current_batch.push(message);

                    // Check if we should flush based on count
                    if self.current_batch.len() >= self.batch_size {
                        drop(wal_reader); // Release lock before async operation
                        self.flush_batch().await?;
                    }
                }
                crate::wal::WalEntry::Checkpoint(_offset) => {
                    // Handle checkpoint if needed
                    debug!(topic = %self.topic_name, "Processed WAL checkpoint");
                }
            }
        }

        Ok(())
    }

    /// Roughly estimate memory size of a message in bytes
    fn estimate_message_size(m: &StreamMessage) -> usize {
        let mut sz = 0usize;
        // payload dominates
        sz += m.payload.len();
        // strings and optionals
        sz += m.msg_id.topic_name.len();
        sz += m.msg_id.broker_addr.len();
        sz += m.producer_name.len();
        if let Some(sub) = &m.subscription_name {
            sz += sub.len();
        }
        // attributes map (keys+values)
        for (k, v) in &m.attributes {
            sz += k.len();
            sz += v.len();
        }
        // fixed overhead for struct fields and Arrow array bookkeeping
        sz += 128;
        sz
    }

    /// Ensure the Iceberg table exists for this topic
    async fn ensure_table_exists(&self) -> Result<()> {
        let namespace = "danube";

        // Check if table already exists
        if self
            .catalog
            .table_exists(namespace, &self.topic_name)
            .await?
        {
            // Load existing metadata
            let metadata = self.catalog.load_table(namespace, &self.topic_name).await?;
            let mut table_metadata = self.table_metadata.write().await;
            *table_metadata = Some(metadata);
            return Ok(());
        }

        // Create namespace if it doesn't exist
        if let Err(_) = self.catalog.create_namespace(namespace).await {
            // Namespace might already exist, continue
        }

        // Create table
        let schema = create_danube_schema();
        let location = format!(
            "{}/{}",
            self.warehouse.trim_end_matches('/'),
            self.topic_name
        );

        let metadata = self
            .catalog
            .create_table(namespace, &self.topic_name, &schema, &location)
            .await?;

        let mut table_metadata = self.table_metadata.write().await;
        *table_metadata = Some(metadata);

        info!(topic = %self.topic_name, "Created Iceberg table");
        Ok(())
    }

    /// Flush current batch to Iceberg
    async fn flush_batch(&mut self) -> Result<()> {
        if self.current_batch.is_empty() {
            return Ok(());
        }

        debug!(
            topic = %self.topic_name,
            batch_size = self.current_batch.len(),
            "Flushing batch to Iceberg"
        );

        // Convert messages to Arrow RecordBatch
        let record_batch = self.messages_to_record_batch(&self.current_batch)?;

        // Write to Parquet file
        let file_path = self.write_parquet_file(&record_batch).await?;

        // Create data file metadata
        let _data_file = DataFile {
            content: "data".to_string(),
            file_path: file_path.clone(),
            file_format: "parquet".to_string(),
            partition: HashMap::new(),
            record_count: self.current_batch.len() as i64,
            file_size_in_bytes: 0, // TODO: Get actual file size
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
        };

        // Create new snapshot
        let snapshot = Snapshot {
            snapshot_id: rand::thread_rng().gen::<i64>().abs(),
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            manifest_list: format!("{}/manifests/manifest-{}.avro", file_path, Uuid::new_v4()),
            summary: HashMap::new(),
            schema_id: 0,
        };

        // Update table metadata with new snapshot
        let mut table_metadata_guard = self.table_metadata.write().await;
        if let Some(ref mut metadata) = *table_metadata_guard {
            metadata.snapshots.push(snapshot.clone());
            metadata.current_snapshot_id = Some(snapshot.snapshot_id);
            metadata.last_updated_ms = snapshot.timestamp_ms;

            // Update table in catalog
            let updated_metadata = self
                .catalog
                .update_table("danube", &self.topic_name, metadata, metadata)
                .await?;

            *metadata = updated_metadata;
        }

        // Clear the batch
        self.current_batch.clear();
        self.current_batch_bytes = 0;
        self.last_flush = SystemTime::now();

        info!(
            topic = %self.topic_name,
            snapshot_id = snapshot.snapshot_id,
            "Flushed batch to Iceberg"
        );

        Ok(())
    }

    /// Convert messages to Arrow RecordBatch
    fn messages_to_record_batch(&self, messages: &[StreamMessage]) -> Result<RecordBatch> {
        let _len = messages.len();

        // Create arrays for each field
        let request_ids: Vec<u64> = messages.iter().map(|m| m.request_id).collect();
        let producer_ids: Vec<i64> = messages
            .iter()
            .map(|m| m.msg_id.producer_id as i64)
            .collect();
        let topic_names: Vec<String> = messages
            .iter()
            .map(|m| m.msg_id.topic_name.clone())
            .collect();
        let broker_addrs: Vec<String> = messages
            .iter()
            .map(|m| m.msg_id.broker_addr.clone())
            .collect();
        let segment_ids: Vec<i64> = messages
            .iter()
            .map(|m| m.msg_id.segment_id as i64)
            .collect();
        let segment_offsets: Vec<i64> = messages
            .iter()
            .map(|m| m.msg_id.segment_offset as i64)
            .collect();
        let payloads: Vec<Vec<u8>> = messages.iter().map(|m| m.payload.clone()).collect();
        let publish_times: Vec<i64> = messages.iter().map(|m| m.publish_time as i64).collect();
        let producer_names: Vec<String> =
            messages.iter().map(|m| m.producer_name.clone()).collect();
        let subscription_names: Vec<Option<String>> = messages
            .iter()
            .map(|m| m.subscription_name.clone())
            .collect();

        // Create Arrow arrays
        let request_id_array = Arc::new(UInt64Array::from(request_ids)) as ArrayRef;
        let producer_id_array = Arc::new(Int64Array::from(producer_ids)) as ArrayRef;
        let topic_name_array = Arc::new(StringArray::from(topic_names)) as ArrayRef;
        let broker_addr_array = Arc::new(StringArray::from(broker_addrs)) as ArrayRef;
        let segment_id_array = Arc::new(Int64Array::from(segment_ids)) as ArrayRef;
        let segment_offset_array = Arc::new(Int64Array::from(segment_offsets)) as ArrayRef;
        let payload_array = Arc::new(BinaryArray::from(
            payloads
                .iter()
                .map(|p| p.as_slice())
                .collect::<Vec<&[u8]>>(),
        )) as ArrayRef;
        let publish_time_array =
            Arc::new(TimestampMillisecondArray::from(publish_times)) as ArrayRef;
        let producer_name_array = Arc::new(StringArray::from(producer_names)) as ArrayRef;
        let subscription_name_array = Arc::new(StringArray::from(subscription_names)) as ArrayRef;

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("request_id", DataType::UInt64, false),
            Field::new("producer_id", DataType::Int64, false),
            Field::new("topic_name", DataType::Utf8, false),
            Field::new("broker_addr", DataType::Utf8, false),
            Field::new("segment_id", DataType::Int64, false),
            Field::new("segment_offset", DataType::Int64, false),
            Field::new("payload", DataType::Binary, false),
            Field::new(
                "publish_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("producer_name", DataType::Utf8, false),
            Field::new("subscription_name", DataType::Utf8, true),
        ]));

        // Create record batch
        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                request_id_array,
                producer_id_array,
                topic_name_array,
                broker_addr_array,
                segment_id_array,
                segment_offset_array,
                payload_array,
                publish_time_array,
                producer_name_array,
                subscription_name_array,
            ],
        )
        .map_err(|e| IcebergStorageError::Arrow(e.to_string()))?;

        Ok(record_batch)
    }

    /// Write RecordBatch to Parquet file
    async fn write_parquet_file(&self, record_batch: &RecordBatch) -> Result<String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let file_name = format!("data-{}-{}.parquet", self.topic_name, timestamp);
        // Store under per-topic prefix within the bucket/prefix
        let file_path = Path::from(format!("{}/data/{}", self.topic_name, file_name));

        // Create in-memory buffer
        let mut buffer = Vec::new();

        // Write to Parquet
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), Some(props))
            .map_err(|e| IcebergStorageError::Arrow(e.to_string()))?;

        writer
            .write(record_batch)
            .map_err(|e| IcebergStorageError::Arrow(e.to_string()))?;

        writer
            .close()
            .map_err(|e| IcebergStorageError::Arrow(e.to_string()))?;

        // Upload to object store
        let bytes = Bytes::from(buffer);
        self.object_store.put(&file_path, bytes.into()).await?;

        Ok(file_path.to_string())
    }
}
