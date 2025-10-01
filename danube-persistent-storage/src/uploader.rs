use danube_core::storage::PersistentStorageError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::info;

use crate::checkpoint::CheckpointStore;
use crate::cloud_store::CloudStore;
use crate::etcd_metadata::{EtcdMetadata, ObjectDescriptor};
use crate::wal::UploaderCheckpoint;
use tokio::io::AsyncReadExt;

/// Base (broker-level) uploader configuration applied to all per-topic uploaders.
#[derive(Debug, Clone)]
pub struct UploaderBaseConfig {
    pub interval_seconds: u64,
}

/// Per-topic uploader configuration.
///
/// Fields:
/// - `interval_seconds`: background cycle interval in seconds
/// - `topic_path`: logical topic path (e.g., "ns/topic")
/// - `root_prefix`: metadata root prefix (e.g., "/danube") used for ETCD paths
#[derive(Debug, Clone)]
pub struct UploaderConfig {
    pub interval_seconds: u64,
    pub topic_path: String,  // e.g., "ns/topic"
    pub root_prefix: String, // e.g., "/danube"
}

impl UploaderConfig {
    pub fn from_base(base: &UploaderBaseConfig, topic_path: String, root_prefix: String) -> Self {
        Self {
            interval_seconds: base.interval_seconds,
            topic_path,
            root_prefix,
        }
    }
}

impl Default for UploaderBaseConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 300,
        }
    }
}

impl Default for UploaderConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 300,
            topic_path: "default/topic".to_string(),
            root_prefix: "/danube".to_string(),
        }
    }
}

/// Uploader streams raw WAL frames to cloud storage and writes object descriptors
/// to ETCD. It resumes precisely using `UploaderCheckpoint` `(last_read_file_seq,
/// last_read_byte_position)` and never flushes the WAL.
#[derive(Debug)]
pub struct Uploader {
    cfg: UploaderConfig,
    cloud: CloudStore,
    etcd: EtcdMetadata,
    last_uploaded_offset: AtomicU64,
    ckpt_store: Option<Arc<CheckpointStore>>,
}

impl Uploader {
    /// Create a new per-topic uploader.
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

    /// Read raw frames from WAL files starting at `(start_seq, start_pos)` up to a snapshot
    /// watermark determined by `wal_ckpt`. Avoids partial frames by stopping if a header/payload
    /// cannot be fully read. Does not flush WAL.
    ///
    /// Returns a tuple `(bytes, start_offset, end_offset, next_file_seq, next_byte_pos)` where:
    /// - `bytes` is a concatenation of raw frames `[u64 off][u32 len][u32 crc][bytes]`
    /// - `start_offset`/`end_offset` are the first/last message offsets in the batch
    /// - `next_file_seq`/`next_byte_pos` indicate the precise resume position
    async fn read_frames_by_position(
        &self,
        wal_ckpt: &crate::checkpoint::WalCheckpoint,
        start_seq: u64,
        start_pos: u64,
    ) -> Result<(Vec<u8>, u64, u64, u64, u64), PersistentStorageError> {
        use std::path::PathBuf;
        // Read from rotated files and include the active file as of snapshot time.
        // For the active file, stop at the first partial header/payload to avoid reading in-flight frames.
        let mut files = wal_ckpt.rotated_files.clone();
        files.push((wal_ckpt.file_seq, PathBuf::from(&wal_ckpt.file_path)));
        files.sort_by(|a, b| a.0.cmp(&b.0));

        let mut buf: Vec<u8> = Vec::new();
        let mut started = false;
        let mut first_offset: u64 = 0;
        let mut last_offset: u64 = 0;
        let mut next_seq = start_seq;
        let mut next_pos = start_pos;

        for (seq, path) in files.into_iter().filter(|(s, _)| *s >= start_seq) {
            if path.as_os_str().is_empty() {
                continue;
            }
            let mut file = match tokio::fs::File::open(&path).await {
                Ok(f) => f,
                Err(_) => continue,
            };
            // Seek to starting pos for the first file; subsequent files start at 0
            if seq == start_seq && start_pos > 0 {
                use tokio::io::AsyncSeekExt;
                use tokio::io::SeekFrom;
                if let Err(_) = file.seek(SeekFrom::Start(start_pos)).await {
                    break;
                }
            }

            loop {
                // Read header parts
                let mut off_bytes = [0u8; 8];
                match file.read_exact(&mut off_bytes).await {
                    Ok(_) => {}
                    Err(_) => break, // EOF or partial header => stop on this file
                }
                let off = u64::from_le_bytes(off_bytes);

                let mut len_bytes = [0u8; 4];
                if let Err(_) = file.read_exact(&mut len_bytes).await {
                    break; // partial header => stop
                }
                let len = u32::from_le_bytes(len_bytes) as usize;

                let mut crc_bytes = [0u8; 4];
                if let Err(_) = file.read_exact(&mut crc_bytes).await {
                    break; // partial header => stop
                }
                let crc = u32::from_le_bytes(crc_bytes);

                // Ensure full payload is available; if not, stop without error
                let mut payload = vec![0u8; len];
                if let Err(_) = file.read_exact(&mut payload).await {
                    break;
                }

                // record start offset
                if !started {
                    started = true;
                    first_offset = off;
                }
                last_offset = off;

                // Append header + payload to buf (raw frame)
                buf.extend_from_slice(&off.to_le_bytes());
                buf.extend_from_slice(&(len as u32).to_le_bytes());
                buf.extend_from_slice(&crc.to_le_bytes());
                buf.extend_from_slice(&payload);

                // Track next position in this file
                use tokio::io::AsyncSeekExt;
                use tokio::io::SeekFrom;
                match file.seek(SeekFrom::Current(0)).await {
                    Ok(cur) => {
                        next_seq = seq;
                        next_pos = cur;
                    }
                    Err(_) => {
                        next_seq = seq;
                        next_pos = 0;
                    }
                }
            }

            // If we wrote anything, we can stop after one file to keep cycle latency low
            if !buf.is_empty() {
                // Move next pointer to next file if we ended exactly at EOF
                next_seq = if next_pos == 0 { seq + 1 } else { seq };
                break;
            }
        }

        if buf.is_empty() {
            return Ok((buf, 0, 0, start_seq, start_pos));
        }
        Ok((buf, first_offset, last_offset, next_seq, next_pos))
    }

    /// Commit a serialized batch to cloud and metadata, and persist checkpoint.
    ///
    /// Steps:
    /// 1) Put object bytes to cloud `storage/topics/<topic>/objects/<object_id>`
    /// 2) Write object descriptor and `cur` pointer to ETCD
    /// 3) Update `UploaderCheckpoint` with `(next_file_seq, next_byte_pos)` and `last_committed_offset`
    async fn commit_upload(
        &self,
        object_id: &str,
        bytes: &[u8],
        start_offset: u64,
        end_offset: u64,
        next_file_seq: u64,
        next_byte_pos: u64,
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
            last_read_file_seq: next_file_seq,
            last_read_byte_position: next_byte_pos,
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

    // legacy reader removed in favor of `read_frames_by_position()`

    /// Run a single upload cycle. Returns `Ok(true)` if a batch was uploaded, otherwise `Ok(false)`.
    ///
    /// Takes a snapshot of the WAL checkpoint to define an upper watermark, reads raw frames
    /// from the precise resume position `(last_read_file_seq, last_read_byte_position)`,
    /// and if any bytes are produced, uploads them as a single object and commits a new checkpoint.
    async fn run_once(&self) -> Result<bool, PersistentStorageError> {
        // Snapshot state at cycle start
        let up_ckpt = match &self.ckpt_store {
            Some(store) => store.get_uploader().await.unwrap_or_default(),
            None => UploaderCheckpoint::default(),
        };
        let wal_ckpt = match &self.ckpt_store {
            Some(store) => match store.get_wal().await {
                Some(c) => c,
                None => return Ok(false),
            },
            None => return Ok(false),
        };

        // Read frames from (seq,pos) up to snapshot watermark
        let (bytes, start_offset, end_offset, next_seq, next_pos) =
            self.read_frames_by_position(&wal_ckpt, up_ckpt.last_read_file_seq, up_ckpt.last_read_byte_position)
                .await?;
        if bytes.is_empty() {
            return Ok(false);
        }

        // Object name convention: data-<start>-<end>.dnb1
        let object_id = format!("data-{}-{}.dnb1", start_offset, end_offset);

        self
            .commit_upload(&object_id, &bytes, start_offset, end_offset, next_seq, next_pos)
            .await?;
        Ok(true)
    }

    /// Start a background periodic task that uploads batches.
    ///
    /// Best-effort semantics: single-writer assumption (no distributed lease).
    /// On start, attempts to resume from the last uploader checkpoint.
    pub fn start(self: Arc<Self>) -> JoinHandle<Result<(), PersistentStorageError>> {
        tokio::spawn(async move {
            info!(
                target = "uploader",
                topic = %self.cfg.topic_path,
                interval = self.cfg.interval_seconds,
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
                let _ = self.run_once().await?;
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
