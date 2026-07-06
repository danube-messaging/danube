//! Checkpoint persistence — tracks the last processed offset per topic.
//!
//! Stored as a JSON file in object storage so it survives container restarts
//! without requiring persistent volumes.
//!
//! Path: `{output_prefix}/checkpoints/{namespace}/{topic}.json`

use crate::storage::StorageHandle;
use bytes::Bytes;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Checkpoint data for a single topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Fully qualified topic name (e.g., "/default/sensor-data").
    pub topic: String,
    /// Last successfully processed WAL offset.
    pub last_offset: u64,
    /// Number of Parquet files written so far.
    pub files_written: u64,
    /// Total rows written across all Parquet files.
    pub total_rows: u64,
    /// ISO 8601 timestamp of the last checkpoint update.
    pub updated_at: String,
}

impl Checkpoint {
    /// Create a new checkpoint at offset 0 (no data processed yet).
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            last_offset: 0,
            files_written: 0,
            total_rows: 0,
            updated_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Update the checkpoint after a successful Parquet write.
    pub fn advance(&mut self, last_offset: u64, rows_written: usize) {
        self.last_offset = last_offset;
        self.files_written += 1;
        self.total_rows += rows_written as u64;
        self.updated_at = chrono::Utc::now().to_rfc3339();
    }
}

/// Checkpoint store backed by object storage.
pub struct CheckpointStore {
    output_prefix: String,
}

impl CheckpointStore {
    pub fn new(output_prefix: &str) -> Self {
        Self {
            output_prefix: output_prefix.to_string(),
        }
    }

    pub fn output_prefix(&self) -> &str {
        &self.output_prefix
    }

    /// Derive the checkpoint path for a topic.
    fn checkpoint_path(
        &self,
        storage: &StorageHandle,
        namespace: &str,
        topic: &str,
    ) -> object_store::path::Path {
        let relative = format!(
            "{}/checkpoints/{}/{}.json",
            self.output_prefix, namespace, topic
        );
        storage.path(&relative)
    }

    /// Load the checkpoint for a topic. Returns a fresh checkpoint if none exists.
    pub async fn load(
        &self,
        storage: &StorageHandle,
        namespace: &str,
        topic: &str,
        fq_topic: &str,
    ) -> Checkpoint {
        let path = self.checkpoint_path(storage, namespace, topic);

        match storage.store.get(&path).await {
            Ok(result) => {
                let data = match result.bytes().await {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(path = %path, error = %e, "failed to read checkpoint bytes, starting fresh");
                        return Checkpoint::new(fq_topic);
                    }
                };
                match serde_json::from_slice::<Checkpoint>(&data) {
                    Ok(cp) => {
                        info!(
                            topic = %fq_topic,
                            last_offset = cp.last_offset,
                            files_written = cp.files_written,
                            "loaded checkpoint"
                        );
                        cp
                    }
                    Err(e) => {
                        warn!(path = %path, error = %e, "corrupt checkpoint, starting fresh");
                        Checkpoint::new(fq_topic)
                    }
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                info!(topic = %fq_topic, "no checkpoint found, starting from offset 0");
                Checkpoint::new(fq_topic)
            }
            Err(e) => {
                warn!(path = %path, error = %e, "failed to load checkpoint, starting fresh");
                Checkpoint::new(fq_topic)
            }
        }
    }

    /// Save the checkpoint for a topic.
    pub async fn save(
        &self,
        storage: &StorageHandle,
        namespace: &str,
        topic: &str,
        checkpoint: &Checkpoint,
    ) -> anyhow::Result<()> {
        let path = self.checkpoint_path(storage, namespace, topic);
        let data = serde_json::to_vec_pretty(checkpoint)?;
        let payload = PutPayload::from(Bytes::from(data));

        storage.store.put(&path, payload).await?;

        debug!(
            path = %path,
            last_offset = checkpoint.last_offset,
            "checkpoint saved"
        );

        Ok(())
    }
}
