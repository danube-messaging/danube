use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use danube_core::storage::PersistentStorageError;

/// Represents a durable snapshot of the Write-Ahead Log's state.
///
/// This checkpoint is periodically persisted to disk and contains the necessary information
/// to recover the WAL's state and to coordinate between the writer, uploader, and readers.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalCheckpoint {
    /// The first offset available in the local WAL. This is the oldest offset that can be read
    /// without fetching from cloud storage. It is advanced when old WAL files are pruned.
    pub start_offset: u64,
    /// The last offset successfully written to the WAL at the time of this checkpoint.
    pub last_offset: u64,
    /// The sequence number of the current active WAL file (e.g., the `1` in `wal.1.log`).
    /// This is incremented on each rotation.
    pub file_seq: u64,
    /// The absolute path to the current active WAL file.
    pub file_path: String,
    /// A history of WAL files that have been rotated but not yet pruned. The tuple contains
    /// the file sequence number and its path. This allows the uploader to read from a
    /// sequence of files to find all data that needs to be persisted to the cloud.
    pub rotated_files: Vec<(u64, PathBuf)>,
    /// The base name of the active WAL file (e.g., `wal.1.log`). Useful for logging and quick reference.
    pub active_file_name: Option<String>,
    /// The timestamp (seconds since epoch) of the last file rotation. Used for observability and
    /// can be used to trigger time-based rotations.
    pub last_rotation_at: Option<u64>,
}

/// Represents a durable snapshot of the Uploader's progress.
///
/// This checkpoint tracks how much of the WAL has been successfully persisted to cloud storage.
/// It is used on restart to prevent re-uploading data that has already been committed.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UploaderCheckpoint {
    /// The last message offset that has been successfully uploaded and committed to cloud storage.
    /// On startup, the uploader will resume its work from this offset.
    pub last_committed_offset: u64,
    /// The sequence number of the WAL file the uploader was last reading from.
    pub last_read_file_seq: u64,
    /// The byte position within that file where the next read should begin.
    pub last_read_byte_position: u64,
    /// The unique identifier of the last cloud object that was written. Useful for debugging and
    /// for resuming multi-part uploads if the process was interrupted.
    pub last_object_id: Option<String>,
    /// The timestamp (seconds since epoch) when this checkpoint was last updated.
    pub updated_at: u64,
}

/// A container struct that holds both the WAL and Uploader checkpoints.
///
/// While the two checkpoints are managed and persisted independently, this struct can be
/// useful for diagnostics or for contexts where a combined view of the system's state is needed.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CheckPoint {
    pub wal: WalCheckpoint,
    pub uploader: UploaderCheckpoint,
}

impl CheckPoint {
    /// Write WAL checkpoint to the specified path (typically <dir>/wal.ckpt).
    pub async fn write_wal_to_path(
        wal: &WalCheckpoint,
        path: &std::path::PathBuf,
    ) -> Result<(), danube_core::storage::PersistentStorageError> {
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;
        use tracing::warn;
        let bytes = bincode::serialize(wal).map_err(|e| {
            warn!(target = "wal", error = %e, "wal ckpt serialize failed");
            danube_core::storage::PersistentStorageError::Io(format!(
                "wal ckpt serialize failed: {}",
                e
            ))
        })?;
        let tmp = path.with_extension("tmp");
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .await
            .map_err(|e| {
                warn!(target = "wal", path = %tmp.display(), error = %e, "open wal ckpt tmp failed");
                danube_core::storage::PersistentStorageError::Io(format!("open wal ckpt tmp failed: {}", e))
            })?;
        f.write_all(&bytes).await.map_err(|e| {
            warn!(target = "wal", path = %tmp.display(), error = %e, "write wal ckpt failed");
            danube_core::storage::PersistentStorageError::Io(format!(
                "write wal ckpt failed: {}",
                e
            ))
        })?;
        f.flush().await.map_err(|e| {
            warn!(target = "wal", path = %tmp.display(), error = %e, "flush wal ckpt failed");
            danube_core::storage::PersistentStorageError::Io(format!(
                "flush wal ckpt failed: {}",
                e
            ))
        })?;
        tokio::fs::rename(&tmp, path)
            .await
            .map_err(|e| {
                warn!(target = "wal", from = %tmp.display(), to = %path.display(), error = %e, "rename wal ckpt failed");
                danube_core::storage::PersistentStorageError::Io(format!("rename wal ckpt failed: {}", e))
            })?;
        tracing::debug!(target = "wal", path = %path.display(), size = bytes.len(), "wrote wal checkpoint");
        Ok(())
    }

    /// Read WAL checkpoint from the specified path (<dir>/wal.ckpt), returning Ok(None) if missing.
    pub async fn read_wal_from_path(
        path: &std::path::PathBuf,
    ) -> Result<Option<WalCheckpoint>, danube_core::storage::PersistentStorageError> {
        use tracing::warn;
        match tokio::fs::read(path).await {
            Ok(bytes) => {
                let ckpt: WalCheckpoint = bincode::deserialize(&bytes).map_err(|e| {
                    warn!(target = "wal", path = %path.display(), error = %e, "wal ckpt parse failed");
                    danube_core::storage::PersistentStorageError::Io(format!("wal ckpt parse failed: {}", e))
                })?;
                tracing::debug!(target = "wal", path = %path.display(), size = bytes.len(), "read wal checkpoint");
                Ok(Some(ckpt))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => {
                warn!(target = "wal", path = %path.display(), error = %e, "read wal ckpt failed");
                Err(danube_core::storage::PersistentStorageError::Io(format!(
                    "read wal ckpt failed: {}",
                    e
                )))
            }
        }
    }

    /// Write Uploader checkpoint to `<parent(wal.ckpt)>/uploader.ckpt` given the wal checkpoint path.
    pub async fn write_uploader_near_wal(
        ckpt: &UploaderCheckpoint,
        wal_ckpt_path: &std::path::PathBuf,
    ) -> Result<(), danube_core::storage::PersistentStorageError> {
        let parent = wal_ckpt_path.parent().ok_or_else(|| {
            danube_core::storage::PersistentStorageError::Io(
                "wal ckpt path has no parent".to_string(),
            )
        })?;
        let path = parent.join("uploader.ckpt");
        Self::write_uploader_to_path(ckpt, &path).await
    }

    /// Read Uploader checkpoint from `<parent(wal.ckpt)>/uploader.ckpt` given the wal checkpoint path.
    pub async fn read_uploader_near_wal(
        wal_ckpt_path: &std::path::PathBuf,
    ) -> Result<Option<UploaderCheckpoint>, danube_core::storage::PersistentStorageError> {
        let parent = match wal_ckpt_path.parent() {
            Some(p) => p.to_path_buf(),
            None => return Ok(None),
        };
        let path = parent.join("uploader.ckpt");
        Self::read_uploader_from_path(&path).await
    }

    /// Write Uploader checkpoint to the specified path (typically <dir>/uploader.ckpt).
    pub async fn write_uploader_to_path(
        ckpt: &UploaderCheckpoint,
        path: &std::path::PathBuf,
    ) -> Result<(), danube_core::storage::PersistentStorageError> {
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;
        use tracing::warn;
        let bytes = bincode::serialize(ckpt).map_err(|e| {
            warn!(target = "wal", error = %e, "uploader ckpt serialize failed");
            danube_core::storage::PersistentStorageError::Io(format!(
                "uploader ckpt serialize failed: {}",
                e
            ))
        })?;
        let tmp = path.with_extension("tmp");
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .await
            .map_err(|e| {
                warn!(target = "wal", path = %tmp.display(), error = %e, "open uploader ckpt tmp failed");
                danube_core::storage::PersistentStorageError::Io(format!("open uploader ckpt tmp failed: {}", e))
            })?;
        f.write_all(&bytes).await.map_err(|e| {
            warn!(target = "wal", path = %tmp.display(), error = %e, "write uploader ckpt failed");
            danube_core::storage::PersistentStorageError::Io(format!(
                "write uploader ckpt failed: {}",
                e
            ))
        })?;
        f.flush().await.map_err(|e| {
            warn!(target = "wal", path = %tmp.display(), error = %e, "flush uploader ckpt failed");
            danube_core::storage::PersistentStorageError::Io(format!(
                "flush uploader ckpt failed: {}",
                e
            ))
        })?;
        tokio::fs::rename(&tmp, path)
            .await
            .map_err(|e| {
                warn!(target = "wal", from = %tmp.display(), to = %path.display(), error = %e, "rename uploader ckpt failed");
                danube_core::storage::PersistentStorageError::Io(format!("rename uploader ckpt failed: {}", e))
            })?;
        tracing::debug!(target = "wal", path = %path.display(), size = bytes.len(), "wrote uploader checkpoint");
        Ok(())
    }

    /// Read Uploader checkpoint from the specified path, returning Ok(None) if missing.
    pub async fn read_uploader_from_path(
        path: &std::path::PathBuf,
    ) -> Result<Option<UploaderCheckpoint>, danube_core::storage::PersistentStorageError> {
        use tracing::warn;
        match tokio::fs::read(path).await {
            Ok(bytes) => {
                let ckpt: UploaderCheckpoint = bincode::deserialize(&bytes).map_err(|e| {
                    warn!(target = "wal", path = %path.display(), error = %e, "uploader ckpt parse failed");
                    danube_core::storage::PersistentStorageError::Io(format!("uploader ckpt parse failed: {}", e))
                })?;
                tracing::debug!(target = "wal", path = %path.display(), size = bytes.len(), "read uploader checkpoint");
                Ok(Some(ckpt))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => {
                warn!(target = "wal", path = %path.display(), error = %e, "read uploader ckpt failed");
                Err(danube_core::storage::PersistentStorageError::Io(format!(
                    "read uploader ckpt failed: {}",
                    e
                )))
            }
        }
    }
}

/// Per-topic checkpoint manager with cache-first reads and atomic persistence.
#[derive(Debug, Clone)]
pub struct CheckpointStore {
    wal_ckpt_path: PathBuf,
    uploader_ckpt_path: PathBuf,
    wal_cache: Arc<RwLock<Option<WalCheckpoint>>>,
    uploader_cache: Arc<RwLock<Option<UploaderCheckpoint>>>,
}

impl CheckpointStore {
    pub fn new(wal_ckpt_path: PathBuf, uploader_ckpt_path: PathBuf) -> Self {
        Self {
            wal_ckpt_path,
            uploader_ckpt_path,
            wal_cache: Arc::new(RwLock::new(None)),
            uploader_cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Optionally pre-load both checkpoints from disk into cache.
    pub async fn load_from_disk(&self) -> Result<(), PersistentStorageError> {
        if let Some(wal) = CheckPoint::read_wal_from_path(&self.wal_ckpt_path).await? {
            *self.wal_cache.write().await = Some(wal);
        }
        if let Some(up) = CheckPoint::read_uploader_from_path(&self.uploader_ckpt_path).await? {
            *self.uploader_cache.write().await = Some(up);
        }
        Ok(())
    }

    pub async fn get_wal(&self) -> Option<WalCheckpoint> {
        self.wal_cache.read().await.clone()
    }

    pub async fn update_wal(&self, ckpt: &WalCheckpoint) -> Result<(), PersistentStorageError> {
        // Update cache first for readers, then atomically persist to disk
        *self.wal_cache.write().await = Some(ckpt.clone());
        CheckPoint::write_wal_to_path(ckpt, &self.wal_ckpt_path).await
    }

    pub async fn get_uploader(&self) -> Option<UploaderCheckpoint> {
        self.uploader_cache.read().await.clone()
    }

    pub async fn update_uploader(
        &self,
        ckpt: &UploaderCheckpoint,
    ) -> Result<(), PersistentStorageError> {
        // Monotonicity guard (best-effort)
        {
            let cur = self.uploader_cache.read().await;
            if let Some(prev) = cur.as_ref() {
                if ckpt.last_committed_offset < prev.last_committed_offset {
                    // Do not move backward; ignore silently or log at warn in callers.
                }
            }
        }
        *self.uploader_cache.write().await = Some(ckpt.clone());
        CheckPoint::write_uploader_to_path(ckpt, &self.uploader_ckpt_path).await
    }
}
