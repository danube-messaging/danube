use crate::catalog::{IcebergCatalog, TableMetadata};
use crate::config::ReaderConfig;
use crate::errors::{IcebergStorageError, Result};
use arrow::array::{BinaryArray, Int64Array, StringArray, TimestampMillisecondArray, UInt64Array};
use danube_core::message::StreamMessage;
use futures::StreamExt;
use object_store::{path::Path, ObjectStore};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

/// TopicReader handles streaming reads from Iceberg snapshots
pub struct TopicReader {
    /// Topic name
    topic_name: String,
    /// Iceberg catalog
    catalog: Arc<dyn IcebergCatalog>,
    /// Object store for reading Parquet files
    object_store: Arc<dyn ObjectStore>,
    /// Message sender for streaming to consumers
    message_tx: broadcast::Sender<StreamMessage>,
    /// Last processed snapshot ID
    last_snapshot_id: Option<i64>,
    /// Shutdown signal
    shutdown_rx: mpsc::Receiver<()>,
    /// Table metadata cache
    table_metadata: Arc<RwLock<Option<TableMetadata>>>,
    /// Reader configuration
    reader_cfg: ReaderConfig,
    /// Concurrency limiter for file reads
    concurrent_reads: Arc<Semaphore>,
    /// Prefetch queue for fetched messages
    prefetch_tx: mpsc::Sender<StreamMessage>,
    prefetch_rx: mpsc::Receiver<StreamMessage>,
    /// Set of already processed file paths (to avoid re-reading)
    seen_files: Arc<RwLock<HashSet<String>>>,
}

impl TopicReader {
    /// Create a new TopicReader
    pub async fn new(
        topic_name: String,
        catalog: Arc<dyn IcebergCatalog>,
        object_store: Arc<dyn ObjectStore>,
        reader_cfg: &ReaderConfig,
        message_tx: broadcast::Sender<StreamMessage>,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<Self> {
        // Create prefetch queue according to configuration
        let (prefetch_tx, prefetch_rx) = mpsc::channel(reader_cfg.prefetch_size);
        let reader = Self {
            topic_name,
            catalog,
            object_store,
            message_tx,
            last_snapshot_id: None,
            shutdown_rx,
            table_metadata: Arc::new(RwLock::new(None)),
            reader_cfg: reader_cfg.clone(),
            concurrent_reads: Arc::new(Semaphore::new(reader_cfg.max_concurrent_reads.max(1))),
            prefetch_tx,
            prefetch_rx,
            seen_files: Arc::new(RwLock::new(HashSet::new())),
        };

        Ok(reader)
    }

    /// Start the background reading loop
    pub async fn start(mut self) -> Result<()> {
        info!(topic = %self.topic_name, "Starting TopicReader");

        let mut poll_interval = interval(Duration::from_millis(self.reader_cfg.poll_interval_ms));

        loop {
            tokio::select! {
                // Check for shutdown signal
                _ = self.shutdown_rx.recv() => {
                    info!(topic = %self.topic_name, "TopicReader received shutdown signal");
                    break;
                }

                // Poll for new snapshots
                _ = poll_interval.tick() => {
                    if let Err(e) = self.poll_for_new_snapshots().await {
                        error!(topic = %self.topic_name, error = %e, "Failed to poll for new snapshots");
                    }
                }

                // Drain prefetch queue and broadcast to subscribers
                maybe_msg = self.prefetch_rx.recv() => {
                    match maybe_msg {
                        Some(msg) => {
                            if let Err(e) = self.message_tx.send(msg) {
                                warn!(topic = %self.topic_name, error = %e, "Broadcast send failed");
                            }
                        }
                        None => {
                            // channel closed; should not happen normally
                        }
                    }
                }
            }
        }

        info!(topic = %self.topic_name, "TopicReader stopped");
        Ok(())
    }

    /// Poll for new Iceberg snapshots and stream messages
    async fn poll_for_new_snapshots(&mut self) -> Result<()> {
        let namespace = "danube";

        // Check if table exists
        if !self
            .catalog
            .table_exists(namespace, &self.topic_name)
            .await?
        {
            debug!(topic = %self.topic_name, "Table does not exist yet");
            return Ok(());
        }

        // Load current table metadata
        let metadata = self.catalog.load_table(namespace, &self.topic_name).await?;

        // Check if there are new snapshots
        if let Some(current_snapshot_id) = metadata.current_snapshot_id {
            if self.last_snapshot_id.is_none() || self.last_snapshot_id != Some(current_snapshot_id)
            {
                info!(
                    topic = %self.topic_name,
                    snapshot_id = current_snapshot_id,
                    "Processing new snapshot"
                );

                // Process the new snapshot
                self.process_snapshot(&metadata, current_snapshot_id)
                    .await?;
                self.last_snapshot_id = Some(current_snapshot_id);
            }
        }

        // Update cached metadata
        let mut table_metadata = self.table_metadata.write().await;
        *table_metadata = Some(metadata);

        Ok(())
    }

    /// Process a specific snapshot and stream its messages
    async fn process_snapshot(&self, metadata: &TableMetadata, snapshot_id: i64) -> Result<()> {
        // Find the snapshot
        let snapshot = metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == snapshot_id)
            .ok_or_else(|| {
                IcebergStorageError::Catalog(format!("Snapshot {} not found", snapshot_id))
            })?;

        debug!(
            topic = %self.topic_name,
            snapshot_id = snapshot_id,
            manifest_list = %snapshot.manifest_list,
            "Processing snapshot"
        );

        // Use precise scan planning via catalog to enumerate only newly added files
        let from_snapshot_id = snapshot.parent_snapshot_id;
        let to_snapshot_id = Some(snapshot.snapshot_id);
        let planned = self
            .catalog
            .plan_scan("danube", &self.topic_name, from_snapshot_id, to_snapshot_id)
            .await?;

        for pf in planned
            .into_iter()
            .filter(|pf| pf.status.eq_ignore_ascii_case("ADDED"))
        {
            let path_str = pf.file_path;

            // Skip already processed files
            {
                let seen = self.seen_files.read().await;
                if seen.contains(&path_str) {
                    continue;
                }
            }

            // Convert to object_store Path. For now assume the path is a key relative to the store's root.
            let file_path = Path::from(path_str.clone());

            // Acquire concurrency permit and spawn read task
            let permit = self.concurrent_reads.clone().acquire_owned().await.unwrap();
            let obj = self.object_store.clone();
            let mut tx = self.prefetch_tx.clone();
            let topic = self.topic_name.clone();
            let seen_files = self.seen_files.clone();

            tokio::spawn(async move {
                match Self::read_parquet_file_inner(&obj, &file_path, &topic).await {
                    Ok(messages) => {
                        for msg in messages {
                            if tx.send(msg).await.is_err() {
                                break;
                            }
                        }
                        let mut guard = seen_files.write().await;
                        guard.insert(file_path.to_string());
                    }
                    Err(e) => {
                        warn!(topic = %topic, file = %file_path, error = %e, "Failed to read Parquet file");
                    }
                }
                drop(permit);
            });
        }

        Ok(())
    }

    /// Read Parquet file and convert to messages (actual implementation)
    async fn read_parquet_file_inner(
        object_store: &Arc<dyn ObjectStore>,
        file_path: &Path,
        topic_name: &str,
    ) -> Result<Vec<StreamMessage>> {
        // Download file into memory as Bytes
        let file_bytes = object_store.get(file_path).await?.bytes().await?;

        // Build a RecordBatch reader using Bytes directly (implements ChunkReader)
        let builder = ParquetRecordBatchReaderBuilder::try_new(file_bytes.clone())
            .map_err(|e| IcebergStorageError::Arrow(e.to_string()))?;
        let mut reader = builder
            .build()
            .map_err(|e| IcebergStorageError::Arrow(e.to_string()))?;

        let mut out = Vec::new();
        while let Some(batch) = reader.next() {
            let batch = batch.map_err(|e| IcebergStorageError::Arrow(e.to_string()))?;
            let mut msgs = Self::record_batch_to_messages(&batch, topic_name)?;
            out.append(&mut msgs);
        }

        Ok(out)
    }

    /// Convert Arrow RecordBatch to StreamMessage vector for the Danube schema
    fn record_batch_to_messages(
        batch: &arrow::record_batch::RecordBatch,
        default_topic: &str,
    ) -> Result<Vec<StreamMessage>> {
        use arrow::array::Array;

        let request_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| IcebergStorageError::Arrow("Invalid request_id column type".into()))?;
        let producer_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| IcebergStorageError::Arrow("Invalid producer_id column type".into()))?;
        let topic_names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IcebergStorageError::Arrow("Invalid topic_name column type".into()))?;
        let broker_addrs = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IcebergStorageError::Arrow("Invalid broker_addr column type".into()))?;
        let segment_ids = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| IcebergStorageError::Arrow("Invalid segment_id column type".into()))?;
        let segment_offsets = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                IcebergStorageError::Arrow("Invalid segment_offset column type".into())
            })?;
        let payloads = batch
            .column(6)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| IcebergStorageError::Arrow("Invalid payload column type".into()))?;
        let publish_times = batch
            .column(7)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| IcebergStorageError::Arrow("Invalid publish_time column type".into()))?;
        let producer_names = batch
            .column(8)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                IcebergStorageError::Arrow("Invalid producer_name column type".into())
            })?;
        let subscription_names = batch
            .column(9)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                IcebergStorageError::Arrow("Invalid subscription_name column type".into())
            })?;

        let mut messages = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let topic_name = if topic_names.is_valid(i) {
                topic_names.value(i).to_string()
            } else {
                default_topic.to_string()
            };
            let sub_name = if subscription_names.is_valid(i) {
                Some(subscription_names.value(i).to_string())
            } else {
                None
            };
            let payload = if payloads.is_valid(i) {
                payloads.value(i).to_vec()
            } else {
                Vec::new()
            };

            let msg = StreamMessage {
                request_id: request_ids.value(i),
                msg_id: danube_core::message::MessageID {
                    producer_id: producer_ids.value(i) as u64,
                    topic_name,
                    broker_addr: broker_addrs.value(i).to_string(),
                    segment_id: segment_ids.value(i) as u64,
                    segment_offset: segment_offsets.value(i) as u64,
                },
                payload,
                publish_time: publish_times.value(i) as u64,
                producer_name: producer_names.value(i).to_string(),
                subscription_name: sub_name,
                attributes: std::collections::HashMap::new(),
            };
            messages.push(msg);
        }
        Ok(messages)
    }
}
