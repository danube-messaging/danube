use danube_core::storage::PersistentStorageError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::info;

use crate::cloud_store::CloudStore;
use crate::etcd_metadata::{EtcdMetadata, ObjectDescriptor};
use crate::wal::UploaderCheckpoint;
use crate::checkpoint::CheckpointStore;
use bincode;
use danube_core::message::StreamMessage;
use tokio::io::AsyncReadExt;

#[derive(Debug, Clone)]
pub struct UploaderBaseConfig {
    pub interval_seconds: u64,
    pub max_batch_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct UploaderConfig {
    pub interval_seconds: u64,
    pub max_batch_bytes: usize,
    pub topic_path: String,  // e.g., "ns/topic"
    pub root_prefix: String, // e.g., "/danube"
}

impl UploaderConfig {
    pub fn from_base(base: &UploaderBaseConfig, topic_path: String, root_prefix: String) -> Self {
        Self {
            interval_seconds: base.interval_seconds,
            max_batch_bytes: base.max_batch_bytes,
            topic_path,
            root_prefix,
        }
    }
}

impl Default for UploaderBaseConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 10,
            max_batch_bytes: 8 * 1024 * 1024, // 8 MiB
        }
    }
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
    cloud: CloudStore,
    etcd: EtcdMetadata,
    last_uploaded_offset: AtomicU64,
    ckpt_store: Option<Arc<CheckpointStore>>,
}

impl Uploader {
    pub fn new(
        cfg: UploaderConfig,
        cloud: CloudStore,
        etcd: EtcdMetadata,
        ckpt_store: Option<Arc<CheckpointStore>>,
    ) -> Result<Self, PersistentStorageError> {
        Ok(Self {
            cfg,
            cloud,
            etcd,
            last_uploaded_offset: AtomicU64::new(0),
            ckpt_store,
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
        let up = UploaderCheckpoint {
            last_committed_offset: end_offset,
            last_object_id: Some(object_id.to_string()),
            updated_at: chrono::Utc::now().timestamp() as u64,
        };
        if let Some(store) = &self.ckpt_store {
            let _ = store.update_uploader(&up).await;
        }

        // Advance watermark to the last committed offset for observability
        self.last_uploaded_offset
            .store(end_offset, Ordering::Release);
        Ok(())
    }

    /// Read persisted frames directly from WAL files using the provided WAL checkpoint.
    /// Files are read in sequence order given by `ckpt.rotated_files` followed by the active file.
    async fn read_persisted_since_ckpt(
        &self,
        ckpt: &crate::checkpoint::WalCheckpoint,
        after_offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<(u64, StreamMessage)>, PersistentStorageError> {
        let mut items = Vec::new();
        let mut total = 0usize;

        // Build list of files in ascending seq order
        let mut files: Vec<(u64, std::path::PathBuf)> = ckpt.rotated_files.clone();
        // Append active file as the last one with current seq
        files.push((ckpt.file_seq, std::path::PathBuf::from(&ckpt.file_path)));
        files.sort_by(|a, b| a.0.cmp(&b.0));

        for (_seq, path) in files {
            if path.as_os_str().is_empty() {
                continue;
            }
            let mut file = match tokio::fs::File::open(&path).await {
                Ok(f) => f,
                Err(_) => continue, // ignore missing files
            };
            loop {
                let mut off_bytes = [0u8; 8];
                if let Err(_) = file.read_exact(&mut off_bytes).await {
                    break;
                }
                let off = u64::from_le_bytes(off_bytes);

                let mut len_bytes = [0u8; 4];
                if let Err(e) = file.read_exact(&mut len_bytes).await {
                    return Err(PersistentStorageError::Io(format!(
                        "wal read len failed: {}",
                        e
                    )));
                }
                let len = u32::from_le_bytes(len_bytes) as usize;

                let mut crc_bytes = [0u8; 4];
                if let Err(e) = file.read_exact(&mut crc_bytes).await {
                    return Err(PersistentStorageError::Io(format!(
                        "wal read crc failed: {}",
                        e
                    )));
                }
                let _stored_crc = u32::from_le_bytes(crc_bytes);

                let mut buf = vec![0u8; len];
                if let Err(e) = file.read_exact(&mut buf).await {
                    return Err(PersistentStorageError::Io(format!(
                        "wal read payload failed: {}",
                        e
                    )));
                }

                if off < after_offset {
                    continue;
                }

                let msg: StreamMessage = bincode::deserialize(&buf).map_err(|e| {
                    PersistentStorageError::Other(format!("bincode deserialize failed: {}", e))
                })?;

                if !items.is_empty() && total + (8 + 4 + 4 + len) > max_bytes {
                    return Ok(items);
                }
                total += 8 + 4 + 4 + len;
                items.push((off, msg));
            }
            // If we've collected anything and reached max_bytes, stop early
            if total >= max_bytes {
                break;
            }
        }
        Ok(items)
    }

    /// Run a single upload cycle. Returns Ok(true) if a batch was uploaded.
    async fn run_once(&self) -> Result<bool, PersistentStorageError> {
        // Determine last committed and the current WAL checkpoint for rotation-aware reads.
        let last_committed = self.last_uploaded_offset.load(Ordering::Acquire);
        // Prefer CheckpointStore; if none, we cannot read persisted frames (no durable WAL) => no upload
        let wal_ckpt = match &self.ckpt_store {
            Some(store) => match store.get_wal().await { Some(c) => c, None => return Ok(false) },
            None => return Ok(false),
        };
        // Read only persisted frames from WAL files since last_committed
        let items = self
            .read_persisted_since_ckpt(&wal_ckpt, last_committed, self.cfg.max_batch_bytes)
            .await?;
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
            let initial_ckpt = if let Some(store) = &self.ckpt_store {
                store.get_uploader().await
            } else {
                None
            };
            if let Some(ckpt) = initial_ckpt {
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
