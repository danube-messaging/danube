//! Per-topic worker — orchestrates the segment-to-Parquet conversion pipeline.
//!
//! Each `TopicWorker` runs as an independent tokio task:
//! 1. Poll `ListSegmentDescriptors` gRPC for new sealed segments
//! 2. Read each `.dnb1` segment from object storage
//! 3. Resolve schema via Schema Registry (or fallback to envelope)
//! 4. Build Arrow `RecordBatch` → flush to Parquet
//! 5. Commit to Iceberg catalog (atomically, before checkpoint)
//! 6. Update checkpoint in object storage
//!
//! ## Schema version tracking
//!
//! The worker tracks the `schema_version` from message headers. When a newer
//! version is detected in incoming messages, the current buffer is flushed
//! at the version boundary and the schema is re-resolved from the registry.
//! This prevents mixing data from different schema versions in the same
//! Parquet file.
//!
//! ## Commit ordering (at-least-once semantics)
//!
//! The flush sequence is: **Write Parquet → Commit Iceberg → Save Checkpoint**.
//! This ordering guarantees at-least-once delivery:
//!
//! - Crash after Parquet write but before Iceberg commit: on restart the worker
//!   will re-process the same offsets, producing a duplicate Parquet file that
//!   was never committed to the catalog. The orphan file is harmless.
//! - Crash after Iceberg commit but before checkpoint: on restart the worker
//!   will re-process and re-commit the same offsets. This produces duplicate
//!   data in the Iceberg table (at-least-once), which can be deduped.
//! - The checkpoint is always the *last* step, so it can never advance past
//!   data that was committed to the catalog.

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::config::{CompactionConfig, TopicConfig};
use crate::schema::{self, SchemaMode};
use crate::schema_resolver::SchemaResolver;
use crate::segment_reader::{self, DecodedMessage};
use crate::storage::StorageHandle;
use crate::table_manager::TableManager;
use crate::writer;
use danube_client::DanubeClient;
use danube_core::proto::{
    storage_service_client::StorageServiceClient, ListSegmentDescriptorsRequest,
    SegmentDescriptorProto,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

/// Maximum number of retries for schema resolution before falling back.
const SCHEMA_RESOLVE_MAX_RETRIES: u32 = 5;

/// Initial backoff duration for schema resolution retries.
const SCHEMA_RESOLVE_INITIAL_BACKOFF: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// Buffer state
// ---------------------------------------------------------------------------

/// Accumulated messages waiting to be flushed to Parquet.
struct BufferState {
    messages: Vec<DecodedMessage>,
    bytes: usize,
    last_flush: Instant,
}

impl BufferState {
    fn new() -> Self {
        Self {
            messages: Vec::new(),
            bytes: 0,
            last_flush: Instant::now(),
        }
    }

    fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Add decoded messages to the buffer.
    fn extend(&mut self, msgs: Vec<DecodedMessage>, bytes: usize) {
        self.bytes += bytes;
        self.messages.extend(msgs);
    }

    /// Reset counters after a successful flush (messages are cleared by flush_buffer).
    fn reset_counters(&mut self) {
        self.bytes = 0;
        self.last_flush = Instant::now();
    }

    /// Whether the buffer has exceeded the flush threshold.
    fn should_flush(&self, target_size: usize, max_interval: Duration) -> bool {
        !self.is_empty()
            && (self.bytes >= target_size || self.last_flush.elapsed() >= max_interval)
    }
}

// ---------------------------------------------------------------------------
// TopicWorker
// ---------------------------------------------------------------------------

/// A per-topic background worker that converts segments to Parquet.
pub struct TopicWorker {
    topic_config: TopicConfig,
    compaction: CompactionConfig,
    storage: Arc<StorageHandle>,
    checkpoint_store: CheckpointStore,
    /// Danube client — provides connection pool (with TLS/mTLS) and schema registry.
    danube_client: DanubeClient,
    poll_interval: Duration,
    /// Iceberg table manager (None = Parquet-only mode).
    table_manager: Option<TableManager>,
    /// Schema resolver for fetching schemas from the registry.
    schema_resolver: SchemaResolver,
}

impl TopicWorker {
    pub fn new(
        topic_config: TopicConfig,
        compaction: CompactionConfig,
        storage: Arc<StorageHandle>,
        output_prefix: String,
        danube_client: DanubeClient,
        poll_interval_seconds: u64,
        catalog: Option<Arc<dyn iceberg::Catalog>>,
    ) -> Self {
        let table_manager = catalog.map(TableManager::new);
        let schema_resolver = SchemaResolver::new(danube_client.clone());
        Self {
            topic_config,
            compaction,
            storage,
            checkpoint_store: CheckpointStore::new(&output_prefix),
            danube_client,
            poll_interval: Duration::from_secs(poll_interval_seconds),
            table_manager,
            schema_resolver,
        }
    }

    // -----------------------------------------------------------------------
    // Main loop
    // -----------------------------------------------------------------------

    /// Run the worker loop until shutdown signal.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        let fq_topic = self.topic_config.fully_qualified_topic();
        info!(topic = %fq_topic, "starting topic worker");

        // Get a gRPC channel from the shared connection pool.
        // All workers reuse the same underlying HTTP/2 connection via
        // DanubeClient's ConnectionManager (which also handles TLS/mTLS).
        let channel = match self.get_broker_channel().await {
            Ok(ch) => ch,
            Err(e) => {
                error!(topic = %fq_topic, error = %e, "failed to get broker channel, worker exiting");
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

        let mut schema_mode: Option<SchemaMode> = None;
        let mut schema_resolved_successfully = false;
        let mut buf = BufferState::new();

        let target_size = self.compaction.target_parquet_size_mb * 1024 * 1024;
        let max_flush_interval =
            Duration::from_secs(self.compaction.max_flush_interval_seconds);

        loop {
            // ── Shutdown check ──────────────────────────────────────────
            if *shutdown.borrow() {
                info!(topic = %fq_topic, "shutdown signal received");
                if let Some(ref mode) = schema_mode {
                    self.try_flush(&mut buf, mode, &mut checkpoint, &fq_topic)
                        .await;
                }
                break;
            }

            // ── Poll for new sealed segments ────────────────────────────
            let segments = match self
                .poll_segment_descriptors(&mut client, &fq_topic, checkpoint.last_offset)
                .await
            {
                Ok(descs) => descs,
                Err(e) => {
                    warn!(topic = %fq_topic, error = %e, "segment poll failed, will retry");
                    tokio::select! {
                        _ = tokio::time::sleep(self.poll_interval) => {},
                        _ = shutdown.changed() => { continue; }
                    }
                    continue;
                }
            };

            // ── Process each segment ────────────────────────────────────
            let topic_path = format!(
                "{}/{}",
                self.topic_config.namespace, self.topic_config.topic
            );

            for seg in &segments {
                if *shutdown.borrow() {
                    break;
                }

                self.process_segment(
                    seg,
                    &topic_path,
                    &fq_topic,
                    &mut buf,
                    &mut schema_mode,
                    &mut schema_resolved_successfully,
                    &mut checkpoint,
                    target_size,
                    max_flush_interval,
                )
                .await;
            }

            // ── Time-based flush ────────────────────────────────────────
            if buf.should_flush(target_size, max_flush_interval) {
                if let Some(ref mode) = schema_mode {
                    self.try_flush(&mut buf, mode, &mut checkpoint, &fq_topic)
                        .await;
                }
            }

            // ── Wait for next poll interval or shutdown ─────────────────
            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {},
                _ = shutdown.changed() => { continue; }
            }
        }

        info!(topic = %fq_topic, "topic worker stopped");
    }

    // -----------------------------------------------------------------------
    // Segment processing
    // -----------------------------------------------------------------------

    /// Process a single segment: read, resolve schema, buffer, and flush if needed.
    async fn process_segment(
        &mut self,
        seg: &SegmentDescriptorProto,
        topic_path: &str,
        fq_topic: &str,
        buf: &mut BufferState,
        schema_mode: &mut Option<SchemaMode>,
        schema_resolved_successfully: &mut bool,
        checkpoint: &mut Checkpoint,
        target_size: usize,
        max_flush_interval: Duration,
    ) {
        let messages = match segment_reader::read_segment(
            &self.storage,
            topic_path,
            &seg.segment_id,
        )
        .await
        {
            Ok(msgs) if msgs.is_empty() => return,
            Ok(msgs) => msgs,
            Err(e) => {
                warn!(
                    segment_id = %seg.segment_id,
                    error = %e,
                    "failed to read segment, skipping"
                );
                return;
            }
        };

        let payload_bytes: usize = messages.iter().map(|m| m.message.payload.len()).sum();

        debug!(
            topic = %fq_topic,
            segment_id = %seg.segment_id,
            messages = messages.len(),
            bytes = payload_bytes,
            "read segment"
        );

        // Schema resolution: first-time, retry on failure, or version bump.
        if self.needs_schema_resolve(schema_mode, schema_resolved_successfully, &messages) {
            // Flush buffered data before switching schemas.
            if !buf.is_empty() {
                if let Some(ref mode) = schema_mode {
                    match self
                        .flush_buffer(&mut buf.messages, mode, checkpoint)
                        .await
                    {
                        Ok(()) => {
                            buf.reset_counters();
                            info!(topic = %fq_topic, "flushed buffer at schema version boundary");
                        }
                        Err(e) => {
                            error!(
                                topic = %fq_topic,
                                error = %e,
                                "flush at schema boundary failed"
                            );
                            return;
                        }
                    }
                }
            }

            self.resolve_and_update_schema(
                &messages,
                fq_topic,
                schema_mode,
                schema_resolved_successfully,
            )
            .await;
        }

        buf.extend(messages, payload_bytes);

        // Flush if buffer exceeds thresholds.
        if buf.should_flush(target_size, max_flush_interval) {
            if let Some(ref mode) = schema_mode {
                self.try_flush(buf, mode, checkpoint, fq_topic).await;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Schema resolution helpers
    // -----------------------------------------------------------------------

    /// Determine whether the schema needs (re-)resolution.
    ///
    /// Returns `true` when:
    /// - No schema has been resolved yet
    /// - The previous resolution attempt failed (transient)
    /// - Messages carry a `schema_version` higher than the current one
    fn needs_schema_resolve(
        &self,
        schema_mode: &Option<SchemaMode>,
        schema_resolved_successfully: &bool,
        messages: &[DecodedMessage],
    ) -> bool {
        if schema_mode.is_none() || !schema_resolved_successfully {
            return true;
        }

        if let Some(SchemaMode::Registry {
            schema_version: current,
            ..
        }) = schema_mode
        {
            messages
                .iter()
                .any(|dm| dm.message.schema_version.map_or(false, |v| v > *current))
        } else {
            false
        }
    }

    /// Resolve the schema from the registry and update the caller's state.
    async fn resolve_and_update_schema(
        &mut self,
        messages: &[DecodedMessage],
        fq_topic: &str,
        schema_mode: &mut Option<SchemaMode>,
        schema_resolved_successfully: &mut bool,
    ) {
        let stream_msgs: Vec<_> = messages.iter().map(|dm| dm.message.clone()).collect();

        match self.resolve_schema_with_retry(&stream_msgs).await {
            Ok(mode) => {
                let mode_label = match &mode {
                    SchemaMode::Envelope => "envelope",
                    SchemaMode::Registry {
                        schema_id,
                        schema_version,
                        ..
                    } => {
                        debug!(
                            topic = %fq_topic,
                            schema_id,
                            schema_version,
                            "resolved registry schema"
                        );
                        "registry"
                    }
                };
                info!(topic = %fq_topic, mode = mode_label, "schema mode determined");
                *schema_mode = Some(mode);
                *schema_resolved_successfully = true;
            }
            Err(e) => {
                warn!(
                    topic = %fq_topic,
                    error = %e,
                    "schema resolution failed after retries, using envelope for this batch"
                );
                *schema_mode = Some(SchemaMode::Envelope);
            }
        }
    }

    /// Resolve schema with exponential backoff retries.
    ///
    /// On transient failures (network blips, broker restarts), this avoids
    /// permanently degrading to envelope mode. Returns Err only after all
    /// retries are exhausted.
    async fn resolve_schema_with_retry(
        &mut self,
        messages: &[danube_core::message::StreamMessage],
    ) -> anyhow::Result<SchemaMode> {
        let fq_topic = self.topic_config.fully_qualified_topic();
        let mut backoff = SCHEMA_RESOLVE_INITIAL_BACKOFF;

        for attempt in 1..=SCHEMA_RESOLVE_MAX_RETRIES {
            match self.schema_resolver.resolve(messages).await {
                Ok(mode) => return Ok(mode),
                Err(e) => {
                    if attempt == SCHEMA_RESOLVE_MAX_RETRIES {
                        return Err(e);
                    }
                    warn!(
                        topic = %fq_topic,
                        attempt,
                        max_retries = SCHEMA_RESOLVE_MAX_RETRIES,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %e,
                        "schema resolution failed, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, Duration::from_secs(10));
                }
            }
        }

        unreachable!("loop always returns")
    }

    // -----------------------------------------------------------------------
    // Segment polling
    // -----------------------------------------------------------------------

    /// Get a gRPC channel from the shared DanubeClient connection pool.
    ///
    /// All workers share one connection (HTTP/2 multiplexed) with TLS/mTLS
    /// handled transparently by the DanubeClient's ConnectionManager.
    async fn get_broker_channel(&self) -> anyhow::Result<Channel> {
        self.danube_client
            .get_service_channel()
            .await
            .map_err(|e| anyhow::anyhow!("failed to get broker channel: {}", e))
    }

    /// Poll the broker for new sealed segment descriptors (metadata only).
    ///
    /// Returns the list of segment descriptors without downloading segment data.
    /// The caller processes segments one-at-a-time to bound memory usage.
    async fn poll_segment_descriptors(
        &self,
        client: &mut StorageServiceClient<Channel>,
        fq_topic: &str,
        after_offset: u64,
    ) -> anyhow::Result<Vec<SegmentDescriptorProto>> {
        let request = ListSegmentDescriptorsRequest {
            topic: fq_topic.to_string(),
            after_offset,
        };

        let response = client.list_segment_descriptors(request).await?.into_inner();

        if !response.segments.is_empty() {
            debug!(
                topic = %fq_topic,
                segments = response.segments.len(),
                "discovered new segments"
            );
        }

        Ok(response.segments)
    }

    // -----------------------------------------------------------------------
    // Flush helpers
    // -----------------------------------------------------------------------

    /// Attempt a flush, logging errors without propagating.
    ///
    /// Used for shutdown flushes and size/time-triggered flushes where the
    /// caller wants to continue rather than fail hard.
    async fn try_flush(
        &self,
        buf: &mut BufferState,
        mode: &SchemaMode,
        checkpoint: &mut Checkpoint,
        fq_topic: &str,
    ) {
        if buf.is_empty() {
            return;
        }
        match self
            .flush_buffer(&mut buf.messages, mode, checkpoint)
            .await
        {
            Ok(()) => buf.reset_counters(),
            Err(e) => {
                error!(
                    topic = %fq_topic,
                    error = %e,
                    "flush failed, will retry on next cycle"
                );
            }
        }
    }

    /// Flush the accumulated message buffer to Iceberg data files and update checkpoint.
    ///
    /// ## Commit ordering (at-least-once semantics)
    ///
    /// The sequence is: **Write Data Files → Commit Iceberg → Save Checkpoint**.
    ///
    /// This guarantees the checkpoint never advances past data that was committed
    /// to the Iceberg catalog. A crash at any point produces at-worst duplicate
    /// data (at-least-once), never data loss.
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

        // Build RecordBatch from buffered messages
        let batch = match mode {
            SchemaMode::Envelope => schema::messages_to_envelope_batch(buffer)?,
            SchemaMode::Registry {
                schema,
                field_names,
                field_types,
                ..
            } => schema::messages_to_registry_batch(buffer, field_names, field_types, schema)?,
        };

        let num_rows = batch.num_rows();
        let target_size = self.compaction.target_parquet_size_mb * 1024 * 1024;

        // Get or create the Iceberg table
        let tm = self.table_manager.as_ref().ok_or_else(|| {
            anyhow::anyhow!("table_manager is required — danube-iceberg must be configured with an Iceberg catalog")
        })?;

        let table = tm
            .get_or_create_table(
                &self.topic_config.namespace,
                &self.topic_config.table_name,
                &batch.schema(),
            )
            .await?;

        // Check for schema evolution (detection-only in iceberg-rust 0.9.1).
        if let Err(e) = tm.evolve_schema_if_needed(&table, &batch.schema()).await {
            warn!(
                topic = %fq_topic,
                error = %e,
                "incompatible schema change detected, data file will still be committed \
                 with the current Iceberg table schema"
            );
        }

        // Write data files through iceberg's writer pipeline.
        // This produces Parquet files with full column-level statistics.
        let data_files = writer::write_data_files(&table, batch, target_size).await?;

        // Commit data files to the Iceberg catalog.
        // If this fails, checkpoint is NOT advanced → re-process on restart.
        tm.commit_data_files(&table, data_files).await?;

        // Save checkpoint LAST — only after data is safely committed.
        let max_offset = buffer.iter().map(|m| m.offset).max().unwrap_or(0);
        checkpoint.advance(max_offset, num_rows);

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
            rows = num_rows,
            last_offset = checkpoint.last_offset,
            "flushed to iceberg"
        );

        buffer.clear();
        Ok(())
    }
}
