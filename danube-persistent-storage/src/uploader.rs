use danube_core::storage::PersistentStorageError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::cloud_store::CloudStore;
use crate::etcd_metadata::{EtcdMetadata, ObjectDescriptor};
use crate::wal::Wal;

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
                let object_path = format!(
                    "{}/storage/topics/{}/objects/{}",
                    self.cfg.root_prefix, self.cfg.topic_path, object_id
                );

                // Upload to cloud (stub)
                self.cloud.put_object(&object_path, &bytes).await?;

                // Write descriptor to ETCD (no CAS/lease; single-writer assumption)
                let desc = ObjectDescriptor {
                    object_id: object_id.clone(),
                    start_offset,
                    end_offset,
                    size: bytes.len() as u64,
                    etag: None,
                    created_at: chrono::Utc::now().timestamp() as u64,
                    completed: true,
                };
                let start_padded = format!("{:020}", start_offset);
                self.etcd
                    .put_object_descriptor(&self.cfg.topic_path, &start_padded, &desc)
                    .await?;

                // Advance watermark
                self.last_uploaded_offset
                    .store(watermark, Ordering::Release);
            }
        })
    }
}
