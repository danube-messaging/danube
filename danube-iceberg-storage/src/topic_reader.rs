use crate::config::ReaderConfig;
use crate::errors::{IcebergStorageError, Result};
use arrow_array::{
    BinaryArray, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array,
};
use danube_core::message::StreamMessage;
use futures::StreamExt;
use iceberg::{Catalog as IcebergCatalog, TableIdent};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

/// TopicReader handles streaming reads from Iceberg snapshots
pub struct TopicReader {
    /// Topic name
    topic_name: String,
    /// Iceberg catalog (official trait)
    catalog: Arc<dyn IcebergCatalog>,
    /// Message sender for streaming to consumers
    message_tx: broadcast::Sender<StreamMessage>,
    /// Last processed snapshot ID
    last_snapshot_id: Option<i64>,
    /// Shutdown signal
    shutdown_rx: mpsc::Receiver<()>,
    /// Reader configuration
    reader_cfg: ReaderConfig,
    prefetch_rx: mpsc::Receiver<StreamMessage>,
}

impl TopicReader {
    /// Create a new TopicReader
    pub async fn new(
        topic_name: String,
        catalog: Arc<dyn IcebergCatalog>,
        reader_cfg: &ReaderConfig,
        message_tx: broadcast::Sender<StreamMessage>,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<Self> {
        // Create prefetch queue according to configuration
        let (_tx_unused, prefetch_rx) = mpsc::channel(reader_cfg.prefetch_size);
        let reader = Self {
            topic_name,
            catalog,
            message_tx,
            last_snapshot_id: None,
            shutdown_rx,
            reader_cfg: reader_cfg.clone(),
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
        let ident = TableIdent::from_strs([namespace, &self.topic_name])
            .map_err(|e| IcebergStorageError::Catalog(e.to_string()))?;
        if !self
            .catalog
            .table_exists(&ident)
            .await
            .map_err(|e| IcebergStorageError::Catalog(e.to_string()))?
        {
            debug!(topic = %self.topic_name, "Table does not exist yet");
            return Ok(());
        }

        // Load current table handle and metadata
        let table = self
            .catalog
            .load_table(&ident)
            .await
            .map_err(|e| IcebergStorageError::Catalog(e.to_string()))?;
        let meta = table.metadata();

        // Check if there are new snapshots
        if let Some(current_snapshot_id) = meta.current_snapshot_id() {
            if self.last_snapshot_id.is_none()
                || self.last_snapshot_id != Some(current_snapshot_id as i64)
            {
                info!(
                    topic = %self.topic_name,
                    snapshot_id = current_snapshot_id as i64,
                    "Processing new snapshot"
                );

                // Stage 4: scan-based reading using Iceberg Arrow reader for the current snapshot
                if let Err(e) = self.process_snapshot(current_snapshot_id as i64).await {
                    error!(topic = %self.topic_name, error = %e, "Failed to process snapshot");
                } else {
                    self.last_snapshot_id = Some(current_snapshot_id as i64);
                }
            }
        }

        Ok(())
    }

    /// Process a specific snapshot and stream its messages (Stage 4: reader-based scan)
    async fn process_snapshot(&self, snapshot_id: i64) -> Result<()> {
        // Load table
        let ident = TableIdent::from_strs(["danube", &self.topic_name])
            .map_err(|e| IcebergStorageError::Catalog(e.to_string()))?;
        let table = self
            .catalog
            .load_table(&ident)
            .await
            .map_err(|e| IcebergStorageError::Catalog(e.to_string()))?;

        // Build Arrow reader and plan file scan tasks for the specific snapshot
        let reader = table.reader_builder().build();
        let scan = table
            .scan()
            .select_all()
            .snapshot_id(snapshot_id)
            .build()
            .map_err(|e| IcebergStorageError::Catalog(e.to_string()))?;
        let tasks = scan
            .plan_files()
            .await
            .map_err(|e| IcebergStorageError::Catalog(e.to_string()))?;

        // Read Arrow record batches as a stream
        let mut batch_stream = reader
            .read(tasks)
            .await
            .map_err(|e| IcebergStorageError::Catalog(e.to_string()))?;

        // Iterate stream and publish messages
        while let Some(batch_res) = batch_stream.next().await {
            let batch = batch_res.map_err(|e| IcebergStorageError::Arrow(e.to_string()))?;
            let mut msgs = Self::record_batch_to_messages(&batch, &self.topic_name)?;
            for msg in msgs.drain(..) {
                let _ = self.message_tx.send(msg);
            }
        }

        Ok(())
    }

    /// Convert Arrow RecordBatch to StreamMessage vector for the Danube schema
    pub(crate) fn record_batch_to_messages(
        batch: &RecordBatch,
        default_topic: &str,
    ) -> Result<Vec<StreamMessage>> {
        use arrow_array::Array;

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
