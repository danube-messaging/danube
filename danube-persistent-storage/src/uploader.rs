use danube_core::storage::PersistentStorageError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::cloud_store::CloudStore;
use crate::etcd_metadata::{EtcdMetadata, ObjectDescriptor};
use crate::wal::{UploaderCheckpoint, Wal};

#[derive(Debug, Clone)]
pub struct UploaderConfig {
    pub interval_seconds: u64,
    pub max_batch_bytes: usize,
    pub topic_path: String,  // e.g., "tenant/ns/topic"
    pub root_prefix: String, // e.g., "/danube"
}

impl Default for UploaderConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 10,
            max_batch_bytes: 8 * 1024 * 1024, // 8 MiB
            topic_path: "default/default/topic".to_string(),
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

    /// Start a background periodic task that uploads batches.
    /// This is a simplified, best-effort uploader: it reads from the in-memory WAL cache only.
    /// No leader lease logic; assumes single broker owns the topic.
    pub fn start(self: Arc<Self>) -> JoinHandle<Result<(), PersistentStorageError>> {
        tokio::spawn(async move {
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

            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(self.cfg.interval_seconds));
            loop {
                ticker.tick().await;

                let after = self.last_uploaded_offset.load(Ordering::Acquire);
                let (items, watermark) = self.wal.read_cached_since(after).await?;
                if items.is_empty() {
                    continue;
                }

                // Build a simple binary blob of payloads for now
                let mut bytes = Vec::new();
                let start_offset = items.first().map(|(o, _)| *o).unwrap_or(after + 1);
                let end_offset = items.last().map(|(o, _)| *o).unwrap_or(after);
                for (_off, msg) in &items {
                    bytes.extend_from_slice(&msg.payload);
                }

                // Object name convention: data-<start>-<end>.bin
                let object_id = format!("data-{}-{}.bin", start_offset, end_offset);
                // Store under a stable namespace relative to the CloudStore root
                // Final key will be <cloud_root>/<root_prefix>/storage/topics/<topic_path>/objects/<object_id>
                let object_path = format!(
                    "storage/topics/{}/objects/{}",
                    self.cfg.topic_path, object_id
                );

                // Upload to cloud (stub)
                let meta = self.cloud.put_object_meta(&object_path, &bytes).await?;

                // Write descriptor to ETCD (no CAS/lease; single-writer assumption)
                let desc = ObjectDescriptor {
                    object_id: object_id.clone(),
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

                // Optionally advance current pointer for convenience
                let _ = self
                    .etcd
                    .put_current_pointer(&self.cfg.topic_path, &start_padded)
                    .await;

                // Persist uploader checkpoint after successful commit
                let _ = self
                    .wal
                    .write_uploader_checkpoint(&UploaderCheckpoint {
                        last_committed_offset: end_offset,
                        last_object_id: Some(object_id.clone()),
                        updated_at: chrono::Utc::now().timestamp() as u64,
                    })
                    .await;

                // Advance watermark
                self.last_uploaded_offset
                    .store(watermark, Ordering::Release);
            }
        })
    }
}
