//! Per-topic worker — orchestrates the segment-to-Parquet conversion pipeline.
//!
//! Each `TopicWorker` runs as an independent tokio task:
//! 1. Poll `ListSegmentDescriptors` gRPC for new sealed segments
//! 2. Read each .dnb1 segment from object storage
//! 3. Decode WAL frames → StreamMessage
//! 4. Infer or apply schema → Arrow RecordBatch
//! 5. Flush to Parquet when size/time threshold is reached
//! 6. Update checkpoint in object storage

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::config::{CompactionConfig, TopicConfig};
use crate::schema::{self, SchemaMode};
use crate::segment_reader::{self, DecodedMessage};
use crate::storage::StorageHandle;
use crate::table_manager::TableManager;
use crate::writer::{self, WriterConfig};
use danube_core::proto::{
    storage_service_client::StorageServiceClient, ListSegmentDescriptorsRequest,
};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

/// A per-topic background worker that converts segments to Parquet.
pub struct TopicWorker {
    topic_config: TopicConfig,
    compaction: CompactionConfig,
    storage: Arc<StorageHandle>,
    checkpoint_store: CheckpointStore,
    broker_address: String,
    poll_interval: std::time::Duration,
    /// Iceberg table manager (None = Parquet-only mode).
    table_manager: Option<TableManager>,
}

impl TopicWorker {
    pub fn new(
        topic_config: TopicConfig,
        compaction: CompactionConfig,
        storage: Arc<StorageHandle>,
        output_prefix: String,
        broker_address: String,
        poll_interval_seconds: u64,
        catalog: Option<Arc<dyn iceberg::Catalog>>,
    ) -> Self {
        let table_manager = catalog.map(TableManager::new);
        Self {
            topic_config,
            compaction,
            storage,
            checkpoint_store: CheckpointStore::new(&output_prefix),
            broker_address,
            poll_interval: std::time::Duration::from_secs(poll_interval_seconds),
            table_manager,
        }
    }

    /// Run the worker loop until shutdown signal.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let fq_topic = self.topic_config.fully_qualified_topic();
        info!(topic = %fq_topic, "starting topic worker");

        // Connect to broker gRPC
        let channel = match Channel::from_shared(format!("http://{}", self.broker_address))
            .expect("valid broker address")
            .connect()
            .await
        {
            Ok(ch) => ch,
            Err(e) => {
                error!(topic = %fq_topic, error = %e, "failed to connect to broker, worker exiting");
                return;
            }
        };
        let mut client = StorageServiceClient::new(channel);

        // Load checkpoint
        let mut checkpoint = self
            .checkpoint_store
            .load(
                &self.storage,
                &self.topic_config.namespace,
                &self.topic_config.topic,
                &fq_topic,
            )
            .await;

        // Schema mode: inferred on first batch of messages
        let mut schema_mode: Option<SchemaMode> = None;

        // Message buffer for compaction (accumulate across segments until flush)
        let mut buffer: Vec<DecodedMessage> = Vec::new();
        let mut buffer_bytes: usize = 0;
        let mut last_flush = Instant::now();

        let target_size = self.compaction.target_parquet_size_mb * 1024 * 1024;
        let max_flush_interval = std::time::Duration::from_secs(
            self.compaction.max_flush_interval_seconds,
        );

        loop {
            // Check shutdown
            if *shutdown.borrow() {
                info!(topic = %fq_topic, "shutdown signal received");
                // Flush remaining buffer before exit
                if !buffer.is_empty() {
                    if let Some(ref mode) = schema_mode {
                        if let Err(e) = self
                            .flush_buffer(&mut buffer, mode, &mut checkpoint)
                            .await
                        {
                            error!(topic = %fq_topic, error = %e, "flush on shutdown failed");
                        }
                    }
                }
                break;
            }

            // Poll for new segments
            match self
                .poll_segments(&mut client, &fq_topic, checkpoint.last_offset)
                .await
            {
                Ok(new_messages) => {
                    if !new_messages.is_empty() {
                        let count = new_messages.len();
                        let bytes: usize =
                            new_messages.iter().map(|m| m.message.payload.len()).sum();

                        debug!(
                            topic = %fq_topic,
                            messages = count,
                            bytes,
                            "received new messages from segments"
                        );

                        // Infer schema on first batch
                        if schema_mode.is_none() {
                            let msgs: Vec<_> =
                                new_messages.iter().map(|dm| dm.message.clone()).collect();
                            schema_mode = Some(schema::infer_schema(&msgs));
                            info!(
                                topic = %fq_topic,
                                mode = ?schema_mode.as_ref().map(|m| match m {
                                    SchemaMode::Envelope => "envelope",
                                    SchemaMode::InferredJson { .. } => "inferred_json",
                                }),
                                "schema mode determined"
                            );
                        }

                        buffer_bytes += bytes;
                        buffer.extend(new_messages);
                    }
                }
                Err(e) => {
                    warn!(topic = %fq_topic, error = %e, "segment poll failed, will retry");
                }
            }

            // Check flush thresholds
            let should_flush = !buffer.is_empty()
                && (buffer_bytes >= target_size || last_flush.elapsed() >= max_flush_interval);

            if should_flush {
                if let Some(ref mode) = schema_mode {
                    match self.flush_buffer(&mut buffer, mode, &mut checkpoint).await {
                        Ok(()) => {
                            buffer_bytes = 0;
                            last_flush = Instant::now();
                        }
                        Err(e) => {
                            error!(
                                topic = %fq_topic,
                                error = %e,
                                "flush failed, will retry on next cycle"
                            );
                        }
                    }
                }
            }

            // Wait for next poll interval or shutdown
            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {},
                _ = shutdown.changed() => {
                    continue; // Loop will check shutdown flag
                }
            }
        }

        info!(topic = %fq_topic, "topic worker stopped");
    }

    /// Poll the broker for new sealed segments and read their contents.
    async fn poll_segments(
        &self,
        client: &mut StorageServiceClient<Channel>,
        fq_topic: &str,
        after_offset: u64,
    ) -> anyhow::Result<Vec<DecodedMessage>> {
        let request = ListSegmentDescriptorsRequest {
            topic: fq_topic.to_string(),
            after_offset,
        };

        let response = client
            .list_segment_descriptors(request)
            .await?
            .into_inner();

        if response.segments.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            topic = %fq_topic,
            segments = response.segments.len(),
            "discovered new segments"
        );

        let topic_path = format!(
            "{}/{}",
            self.topic_config.namespace, self.topic_config.topic
        );

        let mut all_messages = Vec::new();

        for seg in &response.segments {
            match segment_reader::read_segment(&self.storage, &topic_path, &seg.segment_id).await {
                Ok(messages) => {
                    debug!(
                        segment_id = %seg.segment_id,
                        messages = messages.len(),
                        "read segment"
                    );
                    all_messages.extend(messages);
                }
                Err(e) => {
                    warn!(
                        segment_id = %seg.segment_id,
                        error = %e,
                        "failed to read segment, skipping"
                    );
                }
            }
        }

        Ok(all_messages)
    }

    /// Flush the accumulated message buffer to a Parquet file and update checkpoint.
    async fn flush_buffer(
        &self,
        buffer: &mut Vec<DecodedMessage>,
        mode: &SchemaMode,
        checkpoint: &mut Checkpoint,
    ) -> anyhow::Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        let fq_topic = self.topic_config.fully_qualified_topic();

        // Build RecordBatch
        let (batch, schema_ref) = match mode {
            SchemaMode::Envelope => {
                let batch = schema::messages_to_envelope_batch(buffer)?;
                let schema = batch.schema();
                (batch, schema)
            }
            SchemaMode::InferredJson {
                schema,
                field_names,
                field_types,
            } => {
                let batch = schema::messages_to_inferred_batch(
                    buffer,
                    field_names,
                    field_types,
                    schema,
                )?;
                (batch, schema.clone())
            }
        };

        // Write Parquet
        let writer_config = WriterConfig {
            output_prefix: self.checkpoint_store.output_prefix().to_string(),
            namespace: self.topic_config.namespace.clone(),
            topic: self.topic_config.topic.clone(),
        };

        let result =
            writer::write_parquet(&self.storage, &writer_config, schema_ref, &batch).await?;

        // Update checkpoint with the highest offset in this batch
        let max_offset = buffer.iter().map(|m| m.offset).max().unwrap_or(0);
        checkpoint.advance(max_offset, result.num_rows);

        // Save checkpoint
        self.checkpoint_store
            .save(
                &self.storage,
                &self.topic_config.namespace,
                &self.topic_config.topic,
                checkpoint,
            )
            .await?;

        info!(
            topic = %fq_topic,
            parquet_path = %result.path,
            rows = result.num_rows,
            last_offset = checkpoint.last_offset,
            "flushed to parquet"
        );

        // Commit to Iceberg catalog (if configured)
        if let Some(ref tm) = self.table_manager {
            let table = tm
                .get_or_create_table(
                    &self.topic_config.namespace,
                    &self.topic_config.table_name,
                    &batch.schema(),
                )
                .await?;

            tm.commit_data_file(
                &table,
                &result.path,
                result.num_rows as u64,
                result.file_size_bytes,
            )
            .await?;
        }

        buffer.clear();
        Ok(())
    }
}
