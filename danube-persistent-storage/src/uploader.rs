use danube_core::storage::PersistentStorageError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::info;

use crate::cloud_store::CloudStore;
use crate::etcd_metadata::{EtcdMetadata, ObjectDescriptor};
use crate::wal::{UploaderCheckpoint, Wal};
use bincode;

#[derive(Debug, Clone)]
pub struct UploaderConfig {
    pub interval_seconds: u64,
    pub max_batch_bytes: usize,
    pub topic_path: String,  // e.g., "ns/topic"
    pub root_prefix: String, // e.g., "/danube"
}

impl Default for UploaderConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 10,
            max_batch_bytes: 8 * 1024 * 1024, // 8 MiB
            topic_path: "default/topic".to_string(),
            root_prefix: "/danube".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct Uploader {
    cfg: UploaderConfig,
    wal: Wal,
    cloud: CloudStore,
    etcd: EtcdMetadata,
    last_uploaded_offset: AtomicU64,
}

impl Uploader {
    pub fn new(
        cfg: UploaderConfig,
        wal: Wal,
        cloud: CloudStore,
        etcd: EtcdMetadata,
    ) -> Result<Self, PersistentStorageError> {
        Ok(Self {
            cfg,
            wal,
            cloud,
            etcd,
            last_uploaded_offset: AtomicU64::new(0),
        })
    }

    /// Build a self-describing bytes payload for a batch of items and return
    /// (bytes, start_offset, end_offset).
    fn serialize_items(
        &self,
        items: &[(u64, danube_core::message::StreamMessage)],
        last_committed: u64,
    ) -> Result<(Vec<u8>, u64, u64), PersistentStorageError> {
        // Format v1 (DNB1):
        //   magic: 4 bytes = "DNB1"
        //   version: u8 = 1
        //   record_count: u32
        //   repeated records: [u64 offset][u32 len][bytes bincode(StreamMessage)]
        let mut bytes = Vec::new();
        let start_offset = items
            .first()
            .map(|(o, _)| *o)
            .unwrap_or(last_committed);
        let end_offset = items
            .last()
            .map(|(o, _)| *o)
            .unwrap_or(last_committed);
        // header
        bytes.extend_from_slice(b"DNB1");
        bytes.push(1u8); // version
        let count = items.len() as u32;
        bytes.extend_from_slice(&count.to_le_bytes());
        // records
        for (off, msg) in items.iter() {
            bytes.extend_from_slice(&off.to_le_bytes());
            let rec = bincode::serialize(msg).map_err(|e| {
                PersistentStorageError::Other(format!(
                    "uploader: serialize StreamMessage failed: {}",
                    e
                ))
            })?;
            let len = rec.len() as u32;
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(&rec);
        }
        Ok((bytes, start_offset, end_offset))
    }

    /// Commit a serialized batch to cloud and metadata, and persist checkpoint.
    async fn commit_upload(
        &self,
        object_id: &str,
        bytes: &[u8],
        start_offset: u64,
        end_offset: u64,
    ) -> Result<(), PersistentStorageError> {
        // Upload to cloud
        let object_path = format!(
            "storage/topics/{}/objects/{}",
            self.cfg.topic_path, object_id
        );
        let meta = self.cloud.put_object_meta(&object_path, bytes).await?;

        // Write descriptor to ETCD (no CAS/lease; single-writer assumption)
        let desc = ObjectDescriptor {
            object_id: object_id.to_string(),
            start_offset,
            end_offset,
            size: bytes.len() as u64,
            etag: meta.etag().map(|s| s.to_string()),
            created_at: chrono::Utc::now().timestamp() as u64,
            completed: true,
        };
        let start_padded = format!("{:020}", start_offset);
        self.etcd
            .put_object_descriptor(&self.cfg.topic_path, &start_padded, &desc)
            .await?;
        let _ = self
            .etcd
            .put_current_pointer(&self.cfg.topic_path, &start_padded)
            .await;

        // Persist uploader checkpoint after successful commit
        let _ = self
            .wal
            .write_uploader_checkpoint(&UploaderCheckpoint {
                last_committed_offset: end_offset,
                last_object_id: Some(object_id.to_string()),
                updated_at: chrono::Utc::now().timestamp() as u64,
            })
            .await;

        // Advance watermark to the last committed offset for observability
        self.last_uploaded_offset
            .store(end_offset, Ordering::Release);
        Ok(())
    }

    /// Run a single upload cycle. Returns Ok(true) if a batch was uploaded.
    async fn run_once(&self) -> Result<bool, PersistentStorageError> {
        // Determine the start offset for this batch.
        // First run (no prior commit): start at 0 (inclusive) so offset 0 is included.
        // Subsequent runs: start strictly after the last committed offset to avoid duplicates.
        let last_committed = self.last_uploaded_offset.load(Ordering::Acquire);
        let start_from = if last_committed == 0 {
            0
        } else {
            last_committed.saturating_add(1)
        };
        let (items, _watermark) = self.wal.read_cached_since(start_from).await?;
        if items.is_empty() {
            return Ok(false);
        }

        let (bytes, start_offset, end_offset) =
            self.serialize_items(&items, last_committed)?;

        // Object name convention: data-<start>-<end>.dnb1
        let object_id = format!("data-{}-{}.dnb1", start_offset, end_offset);

        self.commit_upload(&object_id, &bytes, start_offset, end_offset)
            .await?;
        Ok(true)
    }

    /// Start a background periodic task that uploads batches.
    /// This is a simplified, best-effort uploader: it reads from the in-memory WAL cache only.
    /// No leader lease logic; assumes single broker owns the topic.
    pub fn start(self: Arc<Self>) -> JoinHandle<Result<(), PersistentStorageError>> {
        tokio::spawn(async move {
            info!(
                target = "uploader",
                topic = %self.cfg.topic_path,
                interval = self.cfg.interval_seconds,
                max_batch_bytes = self.cfg.max_batch_bytes,
                "uploader started"
            );
            // On start, try to resume from uploader checkpoint if present.
            if let Ok(Some(ckpt)) = self.wal.read_uploader_checkpoint().await {
                self.last_uploaded_offset
                    .store(ckpt.last_committed_offset, Ordering::Release);
                tracing::info!(
                    target = "uploader",
                    last_committed_offset = ckpt.last_committed_offset,
                    last_object_id = ckpt.last_object_id.as_deref().unwrap_or(""),
                    "resumed uploader from checkpoint"
                );
            }

            // Run one immediate cycle for determinism in tests and faster startup
            let _ = self.run_once().await?;

            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(self.cfg.interval_seconds));
            loop {
                ticker.tick().await;

                // Idle tick if no items; otherwise process a batch
                if !self.run_once().await? {
                    continue;
                }
            }
        })
    }

    /// Test-only: expose configuration for unit tests within this crate.
    #[cfg(test)]
    pub(crate) fn test_cfg(&self) -> &UploaderConfig {
        &self.cfg
    }

    /// Test-only: expose last uploaded offset watermark for assertions.
    #[cfg(test)]
    pub(crate) fn test_last_uploaded_offset(&self) -> u64 {
        self.last_uploaded_offset.load(Ordering::Acquire)
    }
}
