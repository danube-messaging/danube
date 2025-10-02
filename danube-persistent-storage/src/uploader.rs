use danube_core::storage::PersistentStorageError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::info;

use crate::checkpoint::CheckpointStore;
use crate::etcd_metadata::{EtcdMetadata, ObjectDescriptor};
use crate::wal::UploaderCheckpoint;
use crate::{CloudStore, CloudWriter};
use tokio::io::AsyncReadExt;

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

    /// Run a single upload cycle. Returns `Ok(true)` if a batch was uploaded, otherwise `Ok(false)`.
    ///
    /// Takes a snapshot of the WAL checkpoint to define an upper watermark and attempts to
    /// stream complete frames directly to cloud storage starting at the precise resume position
    /// `(last_read_file_seq, last_read_byte_position)`. If no complete frame is available for
    /// this cycle, it returns `Ok(false)` without uploading. This keeps cloud uploads aligned
    /// to whole-frame boundaries and avoids buffering locally for cloud uploads.
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

        // Stream frames from (seq,pos) up to snapshot watermark directly to cloud.
        match self
            .stream_frames_to_cloud(
                &wal_ckpt,
                up_ckpt.last_read_file_seq,
                up_ckpt.last_read_byte_position,
            )
            .await?
        {
            Some((object_id, start_offset, end_offset, next_seq, next_pos, meta, offset_index)) => {
                // Write descriptor and checkpoints now that upload is finalized
                self.commit_uploaded_descriptor(
                    &object_id,
                    start_offset,
                    end_offset,
                    meta,
                    next_seq,
                    next_pos,
                    offset_index,
                )
                .await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Stream frames from precise WAL position to cloud, computing first/last offsets and next position.
    /// Returns None if no complete frame was found to upload.
    async fn stream_frames_to_cloud(
        &self,
        wal_ckpt: &crate::checkpoint::WalCheckpoint,
        start_seq: u64,
        start_pos: u64,
    ) -> Result<
        Option<(
            String,
            u64,
            u64,
            u64,
            u64,
            opendal::Metadata,
            Vec<(u64, u64)>,
        )>,
        PersistentStorageError,
    > {
        use std::path::PathBuf;
        use tokio::io::{AsyncSeekExt, SeekFrom};

        // Compose ordered file list up to the active file
        let mut files = wal_ckpt.rotated_files.clone();
        files.push((wal_ckpt.file_seq, PathBuf::from(&wal_ckpt.file_path)));
        files.sort_by(|a, b| a.0.cmp(&b.0));

        // Prepare cloud writer lazily upon seeing first complete frame to know start_offset
        let mut cloud_writer: Option<CloudWriter> = None;
        let mut object_id: Option<String> = None;
        let mut first_offset: Option<u64> = None;
        let mut last_offset: Option<u64> = None;
        let mut next_seq_out = start_seq;
        let mut next_pos_out = start_pos;

        // Parser carry buffer for chunk boundary handling
        let mut carry: Vec<u8> = Vec::new();

        // Limits
        let chunk_size = 2 * 1024 * 1024; // 2 MiB file read buffer
        const INDEX_EVERY_MSGS: usize = 1000; // sparse index granularity by message count
        let mut index_entries: Vec<(u64, u64)> = Vec::new();
        let mut msgs_since_index: usize = 0;
        let mut total_bytes_uploaded: u64 = 0;

        for (seq, path) in files.into_iter().filter(|(s, _)| *s >= start_seq) {
            if path.as_os_str().is_empty() {
                continue;
            }
            let mut file = match tokio::fs::File::open(&path).await {
                Ok(f) => f,
                Err(_) => continue,
            };
            if seq == start_seq && start_pos > 0 {
                file.seek(SeekFrom::Start(start_pos))
                    .await
                    .map_err(|_| PersistentStorageError::Other("seek failed".into()))?;
            }

            let mut buf = vec![0u8; chunk_size];
            loop {
                let n = file
                    .read(&mut buf)
                    .await
                    .map_err(|e| PersistentStorageError::Other(format!("read wal: {}", e)))?;
                if n == 0 {
                    break;
                }
                // Append to carry and parse frame headers/payload boundaries to avoid cutting frames.
                carry.extend_from_slice(&buf[..n]);

                // We can start uploading data as soon as we have at least one full frame in carry.
                // Determine how many complete bytes we can safely upload (ending at frame boundary).
                let safe_len = Self::scan_safe_frame_boundary(&carry);
                if safe_len > 0 {
                    // Initialize writer and object name on first complete frame
                    if first_offset.is_none() {
                        let (first, last) = Self::extract_offsets_in_prefix(&carry[..safe_len]);
                        first_offset = first;
                        last_offset = last;
                        if let Some(s) = first_offset {
                            let obj = format!("data-{}-pending.dnb1", s);
                            let path =
                                format!("storage/topics/{}/objects/{}", self.cfg.topic_path, obj);
                            let writer = self
                                .cloud
                                .open_streaming_writer(&path, 8 * 1024 * 1024, 4)
                                .await?;
                            cloud_writer = Some(writer);
                            object_id = Some(obj);
                        }
                    } else {
                        // Update last_offset as we extend
                        let (_first, last) = Self::extract_offsets_in_prefix(&carry[..safe_len]);
                        if let Some(l) = last {
                            last_offset = Some(l);
                        }
                    }

                    // Build sparse index entries within this safe prefix based on message count.
                    // We scan frames and when msgs_since_index reaches INDEX_EVERY_MSGS we record (offset, byte_pos).
                    let mut idx_scan: usize = 0;
                    while idx_scan + 16 <= safe_len {
                        let off =
                            u64::from_le_bytes(carry[idx_scan..idx_scan + 8].try_into().unwrap());
                        let len = u32::from_le_bytes(
                            carry[idx_scan + 8..idx_scan + 12].try_into().unwrap(),
                        ) as usize;
                        let next = idx_scan + 16 + len;
                        if next > safe_len {
                            break;
                        }
                        if msgs_since_index == 0 {
                            // record index entry at this frame boundary
                            index_entries.push((off, total_bytes_uploaded + idx_scan as u64));
                        }
                        msgs_since_index = (msgs_since_index + 1) % INDEX_EVERY_MSGS;
                        idx_scan = next;
                    }
                    if let Some(w) = cloud_writer.as_mut() {
                        w.write(&carry[..safe_len]).await?;
                    }
                    // Drain uploaded bytes from carry
                    carry.drain(0..safe_len);
                    total_bytes_uploaded += safe_len as u64;
                }
                // Update next position in this file
                next_seq_out = seq;
                next_pos_out = file.stream_position().await.unwrap_or(0);
            }
            // Process only one file per cycle to bound latency
            if first_offset.is_some() {
                break;
            }
        }

        // If we didn't upload anything, return None
        let (_object_id, first_offset) = match (object_id, first_offset) {
            (Some(id), Some(start)) => (id, start),
            _ => return Ok(None),
        };

        // Close writer and finalize metadata
        let mut cw = cloud_writer.unwrap();
        let meta = cw.close().await?;

        // Rename object id to include end offset for descriptor readability
        let end = last_offset.unwrap_or(first_offset);
        let final_object_id = format!("data-{}-{}.dnb1", first_offset, end);
        // Perform server-side copy from pending key to final key and delete pending key so
        // tests and readers can find the final object path.
        let pending_path = format!(
            "storage/topics/{}/objects/{}",
            self.cfg.topic_path,
            format!("data-{}-pending.dnb1", first_offset)
        );
        let final_path = format!(
            "storage/topics/{}/objects/{}",
            self.cfg.topic_path, final_object_id
        );
        // Try server-side copy first; if unsupported, fall back to read+write.
        match self.cloud.copy_object(&pending_path, &final_path).await {
            Ok(()) => {
                let _ = self.cloud.delete_object(&pending_path).await;
            }
            Err(_e) => {
                // Fallback: download pending and upload to final, then delete pending.
                if let Ok(bytes) = self.cloud.get_object(&pending_path).await {
                    let _ = self.cloud.put_object_meta(&final_path, &bytes).await;
                }
                let _ = self.cloud.delete_object(&pending_path).await;
            }
        }

        Ok(Some((
            final_object_id,
            first_offset,
            end,
            next_seq_out,
            next_pos_out,
            meta,
            index_entries,
        )))
    }

    /// Determine the largest prefix length that contains only whole frames.
    fn scan_safe_frame_boundary(buf: &[u8]) -> usize {
        let mut idx = 0usize;
        while idx + 16 <= buf.len() {
            let len = u32::from_le_bytes(buf[idx + 8..idx + 12].try_into().unwrap()) as usize;
            let next = idx + 16 + len;
            if next > buf.len() {
                break;
            }
            idx = next;
        }
        idx
    }

    /// Extract first and last offsets inside a complete frames prefix.
    fn extract_offsets_in_prefix(buf: &[u8]) -> (Option<u64>, Option<u64>) {
        let mut idx = 0usize;
        let mut first: Option<u64> = None;
        let mut last: Option<u64> = None;
        while idx + 16 <= buf.len() {
            let off = u64::from_le_bytes(buf[idx..idx + 8].try_into().unwrap());
            let len = u32::from_le_bytes(buf[idx + 8..idx + 12].try_into().unwrap()) as usize;
            if first.is_none() {
                first = Some(off);
            }
            last = Some(off);
            let next = idx + 16 + len;
            if next > buf.len() {
                break;
            }
            idx = next;
        }
        (first, last)
    }

    /// After a successful streaming upload, write descriptor and update checkpoints/metrics.
    async fn commit_uploaded_descriptor(
        &self,
        object_id: &str,
        start_offset: u64,
        end_offset: u64,
        meta: opendal::Metadata,
        next_file_seq: u64,
        next_byte_pos: u64,
        offset_index: Vec<(u64, u64)>,
    ) -> Result<(), PersistentStorageError> {
        // Write descriptor to ETCD
        let desc = ObjectDescriptor {
            object_id: object_id.to_string(),
            start_offset,
            end_offset,
            size: meta.content_length(),
            etag: meta.etag().map(|s| s.to_string()),
            created_at: chrono::Utc::now().timestamp() as u64,
            completed: true,
            offset_index: if offset_index.is_empty() {
                None
            } else {
                Some(offset_index)
            },
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

        self.last_uploaded_offset
            .store(end_offset, Ordering::Release);
        Ok(())
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
