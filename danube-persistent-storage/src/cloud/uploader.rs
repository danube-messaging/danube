use crate::persistent_metrics::CLOUD_UPLOAD_OBJECTS_TOTAL;
use danube_core::storage::PersistentStorageError;
use metrics::counter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::checkpoint::CheckpointStore;
use crate::cloud::uploader_stream;
use crate::cloud::CloudStore;
use crate::storage_metadata::{SegmentDescriptor, StorageMetadata};

/// Uploader streams raw WAL frames to cloud storage and writes object descriptors
/// to the metadata store. It resumes precisely using `UploaderCheckpoint` `(last_read_file_seq,
/// last_read_byte_position)` and never flushes the WAL.
#[derive(Debug)]
pub(crate) struct Uploader {
    cfg: UploaderConfig,
    cloud: CloudStore,
    metadata: StorageMetadata,
    last_uploaded_offset: AtomicU64,
    ckpt_store: Arc<CheckpointStore>,
}

impl Uploader {
    /// Create a new per-topic uploader.
    pub(crate) fn new(
        cfg: UploaderConfig,
        cloud: CloudStore,
        metadata: StorageMetadata,
        ckpt_store: Arc<CheckpointStore>,
    ) -> Result<Self, PersistentStorageError> {
        Ok(Self {
            cfg,
            cloud,
            metadata,
            last_uploaded_offset: AtomicU64::new(0),
            ckpt_store,
        })
    }

    /// Start a background periodic task that uploads batches with explicit cancellation token.
    ///
    /// Best-effort semantics: single-writer assumption (no distributed lease).
    /// On start, attempts to resume from the last uploader checkpoint.
    pub(crate) fn start_with_cancel(
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
            let initial_segment = match self
                .metadata
                .get_current_segment_descriptor(&self.cfg.topic_path)
                .await
            {
                Ok(segment) => segment,
                Err(e) => {
                    warn!(
                        target = "uploader",
                        topic = %self.cfg.topic_path,
                        error = %e,
                        "failed to read current segment descriptor on startup"
                    );
                    None
                }
            };
            if let Some(segment) = initial_segment {
                self.last_uploaded_offset
                    .store(segment.end_offset, Ordering::Release);
                tracing::info!(
                    target = "uploader",
                    last_committed_offset = segment.end_offset,
                    last_segment_id = %segment.segment_id,
                    "resumed uploader from segment metadata"
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
        let durable_segment = match self
            .metadata
            .get_current_segment_descriptor(&self.cfg.topic_path)
            .await
        {
            Ok(segment) => segment,
            Err(e) => {
                warn!(
                    target = "uploader",
                    topic = %self.cfg.topic_path,
                    error = %e,
                    "failed to read current segment descriptor"
                );
                None
            }
        };
        let wal_ckpt = match self.ckpt_store.get_wal().await {
            Some(c) => c,
            None => return Ok(false),
        };

        if let Some(last_committed_offset) = durable_segment.as_ref().map(|segment| segment.end_offset) {
            if let Some((segment_id, start_offset, end_offset, meta, offset_index)) =
                uploader_stream::upload_contiguous_rotated_file_as_segment(
                    &self.cloud,
                    &self.cfg.topic_path,
                    &wal_ckpt,
                    last_committed_offset,
                    self.cfg.max_object_mb,
                )
                .await?
            {
                self.commit_uploaded_segment(
                    &segment_id,
                    start_offset,
                    end_offset,
                    meta,
                    offset_index,
                )
                .await?;
                return Ok(true);
            }
        }

        let (start_seq, start_pos) = match durable_segment {
            Some(durable_segment) => {
                match uploader_stream::resume_position_from_uploaded_offset(
                    &wal_ckpt,
                    durable_segment.end_offset,
                )
                .await?
                {
                    Some(pos) => pos,
                    None => return Ok(false),
                }
            }
            None => match uploader_stream::initial_resume_position(&wal_ckpt) {
                Some(pos) => pos,
                None => return Ok(false),
            },
        };

        match uploader_stream::stream_frames_to_cloud(
            &self.cloud,
            &self.cfg.topic_path,
            &wal_ckpt,
            start_seq,
            start_pos,
            self.cfg.max_object_mb,
        )
        .await?
        {
            Some((segment_id, start_offset, end_offset, _next_seq, _next_pos, meta, offset_index)) => {
                self.commit_uploaded_segment(
                    &segment_id,
                    start_offset,
                    end_offset,
                    meta,
                    offset_index,
                )
                .await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// (streaming implementation lives in `uploader_stream` module)

    /// Write descriptor to metadata store and persist uploader checkpoint.
    async fn commit_uploaded_segment(
        &self,
        segment_id: &str,
        start_offset: u64,
        end_offset: u64,
        meta: opendal::Metadata,
        offset_index: Vec<(u64, u64)>,
    ) -> Result<(), PersistentStorageError> {
        // Write descriptor to metadata store
        let desc = SegmentDescriptor {
            segment_id: segment_id.to_string(),
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
        self.metadata
            .put_segment_descriptor(&self.cfg.topic_path, &start_padded, &desc)
            .await?;
        let _ = self
            .metadata
            .put_current_segment(&self.cfg.topic_path, &start_padded)
            .await;

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

    // drain_and_stop/request_stop are no longer needed; cooperative shutdown via CancellationToken
}

/// Base (broker-level) uploader configuration applied to all per-topic uploaders.
#[derive(Debug, Clone)]
pub(crate) struct UploaderBaseConfig {
    pub interval_seconds: u64,
    pub max_object_mb: Option<u64>,
}

/// Per-topic uploader configuration.
///
/// Fields:
/// - `interval_seconds`: background cycle interval in seconds
/// - `topic_path`: logical topic path (e.g., "ns/topic")
/// - `root_prefix`: metadata root prefix (e.g., "/danube") used for metadata paths
#[derive(Debug, Clone)]
pub(crate) struct UploaderConfig {
    pub interval_seconds: u64,
    pub topic_path: String,  // e.g., "ns/topic"
    pub _root_prefix: String, // e.g., "/danube"
    pub max_object_mb: Option<u64>,
}

impl UploaderConfig {
    pub(crate) fn from_base(
        base: &UploaderBaseConfig,
        topic_path: String,
        root_prefix: String,
    ) -> Self {
        Self {
            interval_seconds: base.interval_seconds,
            topic_path,
            _root_prefix: root_prefix,
            max_object_mb: base.max_object_mb,
        }
    }
}

impl Default for UploaderBaseConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 300,
            max_object_mb: Some(256),
        }
    }
}

impl Default for UploaderConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 300,
            topic_path: "default/topic".to_string(),
            _root_prefix: "/danube".to_string(),
            max_object_mb: Some(256),
        }
    }
}
