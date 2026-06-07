//! Per-topic replication task.
//!
//! Tails the local WAL from the last checkpoint offset, collects messages into
//! batches (by count or timeout), and sends them to the cloud cluster.
//! On successful ack, checkpoints the last replicated offset.
//!
//! Resilience:
//! - Retry with exponential backoff + jitter on transient failures
//! - Re-lookup topic broker on `Unavailable` (handles topic migration)
//! - Reset WAL reader to checkpoint on persistent failure
//! - Never gives up — outer loop retries indefinitely

use std::sync::Arc;
use std::time::Duration;

use danube_client::RetryManager;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorage, StartPosition};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

use crate::replicator::checkpoint::CheckpointStore;
use crate::replicator::cluster_client::EdgeCloudClient;

/// How long to wait before retrying after all batch retries are exhausted.
const RECOVERY_BACKOFF: Duration = Duration::from_secs(30);

/// Per-topic replication loop.
///
/// Runs as a spawned task. Tails the local WAL, batches messages,
/// and sends them to the cloud cluster via `EdgeCloudClient`.
pub struct TopicReplicator;

impl TopicReplicator {
    /// Run the per-topic replication loop.
    ///
    /// This function runs indefinitely until cancelled. It:
    /// 1. Loads the last checkpoint offset
    /// 2. Creates a WAL reader from that offset
    /// 3. Collects messages into batches (by size or timeout)
    /// 4. Sends each batch to the cloud with retry
    /// 5. Checkpoints the last acked offset
    /// 6. On persistent failure, resets reader to checkpoint and retries
    pub async fn run(
        topic_name: String,
        storage: Arc<dyn PersistentStorage>,
        cloud_client: Arc<EdgeCloudClient>,
        checkpoint: Arc<CheckpointStore>,
        batch_size: usize,
        batch_timeout: Duration,
    ) {
        let retry = RetryManager::new(0, 0, 0); // defaults: 5 retries, 200ms→5s

        info!(
            topic = %topic_name,
            batch_size,
            batch_timeout_ms = batch_timeout.as_millis() as u64,
            "starting topic replicator"
        );

        loop {
            // Load last checkpoint (resume from where we left off)
            let start_offset = checkpoint.load(&topic_name).await.unwrap_or(0);
            let start_pos = if start_offset > 0 {
                StartPosition::Offset(start_offset + 1)
            } else {
                // First run (no checkpoint): start from the very beginning of the WAL
                // so we don't miss messages written before the replicator attached.
                StartPosition::Offset(0)
            };

            info!(
                topic = %topic_name,
                start_offset,
                "resuming replication from checkpoint"
            );

            // Create WAL reader — always from checkpoint, ensuring no gaps
            let mut reader = match storage.create_reader(&topic_name, start_pos).await {
                Ok(r) => r,
                Err(e) => {
                    error!(
                        topic = %topic_name,
                        error = %e,
                        "failed to create WAL reader, retrying in {:?}",
                        RECOVERY_BACKOFF
                    );
                    tokio::time::sleep(RECOVERY_BACKOFF).await;
                    continue; // retry from top of loop (re-creates reader)
                }
            };

            let mut batch: Vec<StreamMessage> = Vec::with_capacity(batch_size);
            let mut last_offset: u64 = start_offset;
            #[allow(unused_assignments)]
            let mut reader_failed = false;

            // Inner loop: read and replicate until failure
            loop {
                let msg = tokio::time::timeout(batch_timeout, reader.next()).await;

                match msg {
                    // Got a message within timeout
                    Ok(Some(Ok(stream_msg))) => {
                        last_offset = stream_msg.msg_id.topic_offset;
                        batch.push(stream_msg);

                        if batch.len() >= batch_size {
                            let success = Self::flush_batch_with_retry(
                                &topic_name,
                                &cloud_client,
                                &checkpoint,
                                &mut batch,
                                last_offset,
                                &retry,
                            )
                            .await;

                            if !success {
                                reader_failed = true;
                                break; // exit inner loop → reset reader
                            }
                        }
                    }
                    // WAL reader error
                    Ok(Some(Err(e))) => {
                        warn!(
                            topic = %topic_name,
                            error = %e,
                            "WAL reader error during replication, continuing"
                        );
                    }
                    // WAL stream ended (shouldn't happen for tail reader)
                    Ok(None) => {
                        info!(
                            topic = %topic_name,
                            "WAL stream ended, flushing remaining batch"
                        );
                        if !batch.is_empty() {
                            Self::flush_batch_with_retry(
                                &topic_name,
                                &cloud_client,
                                &checkpoint,
                                &mut batch,
                                last_offset,
                                &retry,
                            )
                            .await;
                        }
                        reader_failed = true;
                        break; // re-create reader
                    }
                    // Timeout — flush partial batch if non-empty
                    Err(_) => {
                        if !batch.is_empty() {
                            let success = Self::flush_batch_with_retry(
                                &topic_name,
                                &cloud_client,
                                &checkpoint,
                                &mut batch,
                                last_offset,
                                &retry,
                            )
                            .await;

                            if !success {
                                reader_failed = true;
                                break; // exit inner loop → reset reader
                            }
                        }
                    }
                }
            }

            // If we exited the inner loop due to failure, wait before retrying.
            // The outer loop will re-create the reader from the last checkpoint.
            if reader_failed {
                warn!(
                    topic = %topic_name,
                    "replication interrupted, resetting reader from checkpoint in {:?}",
                    RECOVERY_BACKOFF
                );
                tokio::time::sleep(RECOVERY_BACKOFF).await;
            }
        }
    }

    /// Send accumulated batch to cloud with retry and checkpoint on success.
    ///
    /// Returns `true` if the batch was successfully sent and checkpointed,
    /// `false` if all retries were exhausted.
    async fn flush_batch_with_retry(
        topic_name: &str,
        cloud_client: &EdgeCloudClient,
        checkpoint: &CheckpointStore,
        batch: &mut Vec<StreamMessage>,
        last_offset: u64,
        retry: &RetryManager,
    ) -> bool {
        let batch_len = batch.len();
        debug!(
            topic = %topic_name,
            batch_size = batch_len,
            last_offset,
            "sending batch to cloud"
        );

        // Clone messages for potential retries (batch.drain() consumes them)
        let messages: Vec<StreamMessage> = batch.drain(..).collect();

        for attempt in 0..retry.max_retries() {
            match cloud_client
                .send_batch(topic_name, messages.clone(), last_offset)
                .await
            {
                Ok(acked_offset) => {
                    debug!(
                        topic = %topic_name,
                        acked_offset,
                        "batch acked by cloud"
                    );
                    // Checkpoint the acked offset
                    if let Err(e) = checkpoint.save(topic_name, acked_offset).await {
                        warn!(
                            topic = %topic_name,
                            error = %e,
                            "failed to save replication checkpoint"
                        );
                    }
                    return true;
                }
                Err(e) => {
                    let backoff = retry.calculate_backoff(attempt);
                    warn!(
                        topic = %topic_name,
                        error = %e,
                        attempt = attempt + 1,
                        max_retries = retry.max_retries(),
                        backoff_ms = backoff.as_millis() as u64,
                        "batch send failed, retrying"
                    );
                    // Invalidate cached broker — it may have moved
                    cloud_client.invalidate_topic_client(topic_name).await;
                    tokio::time::sleep(backoff).await;
                }
            }
        }

        error!(
            topic = %topic_name,
            batch_size = batch_len,
            "all retries exhausted for batch, will reset reader from checkpoint"
        );
        false
    }
}
