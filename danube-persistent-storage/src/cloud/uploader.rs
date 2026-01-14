use danube_core::storage::PersistentStorageError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use metrics::counter;
use crate::persistent_metrics::CLOUD_UPLOAD_OBJECTS_TOTAL;

use crate::checkpoint::CheckpointStore;
use crate::cloud::uploader_stream;
use crate::cloud::CloudStore;
use crate::etcd_metadata::{EtcdMetadata, ObjectDescriptor};
use crate::wal::UploaderCheckpoint;

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

    /// Backward-compatible start without an explicit cancellation token.
    /// Used by tests; for production, prefer start_with_cancel so callers can cancel.
    pub fn start(self: Arc<Self>) -> JoinHandle<Result<(), PersistentStorageError>> {
        let token = CancellationToken::new();
        self.start_with_cancel(token)
    }

    /// Start a background periodic task that uploads batches with explicit cancellation token.
    ///
    /// Best-effort semantics: single-writer assumption (no distributed lease).
    /// On start, attempts to resume from the last uploader checkpoint.
    pub fn start_with_cancel(
        self: Arc<Self>,
        cancel: CancellationToken,
    ) -> JoinHandle<Result<(), PersistentStorageError>> {
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
            if let Err(e) = self.run_once().await {
                error!(
                    target = "uploader",
                    topic = %self.cfg.topic_path,
                    error = %e,
                    "uploader run_once failed on startup"
                );
                let provider = self.cloud.provider().to_string();
                counter!(CLOUD_UPLOAD_OBJECTS_TOTAL.name, "topic"=> self.cfg.topic_path.clone(), "provider"=> provider, "result"=> "error").increment(1);
            }

            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(self.cfg.interval_seconds));
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        // graceful drain before exiting
                        loop {
                            match self.run_once().await? { true => continue, false => break }
                        }
                        info!(target = "uploader", topic = %self.cfg.topic_path, "uploader stopped after drain (cancel)");
                        break;
                    }
                    _ = ticker.tick() => {
                        if let Err(e) = self.run_once().await {
                            error!(
                                target = "uploader",
                                topic = %self.cfg.topic_path,
                                error = %e,
                                "uploader run_once failed"
                            );
                            let provider = self.cloud.provider().to_string();
                            counter!(CLOUD_UPLOAD_OBJECTS_TOTAL.name, "topic"=> self.cfg.topic_path.clone(), "provider"=> provider, "result"=> "error").increment(1);
                        }
                    }
                }
            }
            Ok(())
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

        // Stream frames from (seq,pos) up to snapshot watermark directly to cloud via streaming module.
        match uploader_stream::stream_frames_to_cloud(
            &self.cloud,
            &self.cfg.topic_path,
            &wal_ckpt,
            up_ckpt.last_read_file_seq,
            up_ckpt.last_read_byte_position,
            self.cfg.max_object_mb,
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

    /// (streaming implementation lives in `uploader_stream` module)

    /// Write descriptor to ETCD and persist uploader checkpoint.
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

    /// Returns current uploaded offset watermark.
    pub fn last_uploaded_offset(&self) -> u64 {
        self.last_uploaded_offset.load(Ordering::Acquire)
    }

    // drain_and_stop/request_stop are no longer needed; cooperative shutdown via CancellationToken
}

/// Base (broker-level) uploader configuration applied to all per-topic uploaders.
#[derive(Debug, Clone)]
pub struct UploaderBaseConfig {
    pub interval_seconds: u64,
    pub max_object_mb: Option<u64>,
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
    pub max_object_mb: Option<u64>,
}

impl UploaderConfig {
    pub fn from_base(base: &UploaderBaseConfig, topic_path: String, root_prefix: String) -> Self {
        Self {
            interval_seconds: base.interval_seconds,
            topic_path,
            root_prefix,
            max_object_mb: base.max_object_mb,
        }
    }
}

impl Default for UploaderBaseConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 300,
            max_object_mb: None,
        }
    }
}

impl Default for UploaderConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 300,
            topic_path: "default/topic".to_string(),
            root_prefix: "/danube".to_string(),
            max_object_mb: None,
        }
    }
}
