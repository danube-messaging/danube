use crate::catalog::{IcebergCatalog, TableMetadata};
use crate::config::ReaderConfig;
use crate::errors::{IcebergStorageError, Result};
use danube_core::message::StreamMessage;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
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

        // For now, we simulate a single "file read" with dummy messages but enforce concurrency and prefetch
        let permit = self.concurrent_reads.clone().acquire_owned().await.unwrap();
        let mut tx = self.prefetch_tx.clone();
        let topic = self.topic_name.clone();
        tokio::spawn(async move {
            // Simulated fetch: create dummy messages and push into prefetch
            use danube_core::message::MessageID;
            use std::collections::HashMap;
            for i in 0..5 {
                let message = StreamMessage {
                    request_id: (snapshot_id as u64) * 1000 + i,
                    msg_id: MessageID {
                        producer_id: 1,
                        topic_name: topic.clone(),
                        broker_addr: "localhost:6650".to_string(),
                        segment_id: snapshot_id as u64,
                        segment_offset: i,
                    },
                    payload: format!("Message {} from snapshot {}", i, snapshot_id).into_bytes(),
                    publish_time: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    producer_name: "iceberg_reader".to_string(),
                    subscription_name: None,
                    attributes: HashMap::new(),
                };
                if tx.send(message).await.is_err() {
                    // receiver dropped; stop
                    break;
                }
            }
            drop(permit);
        });

        Ok(())
    }

    /// Read Parquet file and convert to messages (placeholder implementation)
    async fn read_parquet_file(&self, file_path: &str) -> Result<Vec<StreamMessage>> {
        debug!(
            topic = %self.topic_name,
            file_path = file_path,
            "Reading Parquet file"
        );

        // TODO: Implement actual Parquet reading
        // This would involve:
        // 1. Download file from object store
        // 2. Parse Parquet using Arrow
        // 3. Convert Arrow records back to StreamMessage

        Ok(vec![])
    }
}
