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
    /// (file sequence number, file path, first_offset in that file). This allows the uploader
    /// and deleter to operate without scanning.
    pub rotated_files: Vec<(u64, PathBuf, u64)>,
    /// The base name of the active WAL file (e.g., `wal.1.log`). Useful for logging and quick reference.
    pub active_file_name: Option<String>,
    /// The timestamp (seconds since epoch) of the last file rotation. Used for observability and
    /// can be used to trigger time-based rotations.
    pub last_rotation_at: Option<u64>,
    /// The first offset written to the current active WAL file, if any write has occurred.
    pub active_file_first_offset: Option<u64>,
}

/// Namespace for atomic WAL checkpoint persistence helpers.
pub(crate) struct CheckPoint;

impl CheckPoint {
    /// Write WAL checkpoint to the specified path (typically <dir>/wal.ckpt).
    ///
    /// Persistence model
    /// - Serialize the full checkpoint with `bincode`.
    /// - Write it to a sibling temporary file.
    /// - Flush the temp file and atomically rename it into place.
    ///
    /// Using tmp+rename prevents readers from observing a partially written checkpoint file during
    /// crashes or concurrent recovery.
    pub async fn write_wal_to_path(
        wal: &WalCheckpoint,
        path: &std::path::PathBuf,
    ) -> Result<(), danube_core::storage::PersistentStorageError> {
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;
        use tracing::warn;
        let bytes =
            bincode::serde::encode_to_vec(wal, bincode::config::standard()).map_err(|e| {
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
                let ckpt: WalCheckpoint = bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).map(|(v, _)| v).map_err(|e| {
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

}

/// Per-topic checkpoint manager with cache-first reads and atomic persistence.
#[derive(Debug, Clone)]
pub struct CheckpointStore {
    wal_ckpt_path: PathBuf,
    wal_cache: Arc<RwLock<Option<WalCheckpoint>>>,
}

impl CheckpointStore {
    pub fn new(wal_ckpt_path: PathBuf) -> Self {
        Self {
            wal_ckpt_path,
            wal_cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Optionally pre-load the WAL checkpoint from disk into cache.
    ///
    /// This is a startup optimization: later readers can use `get_wal()` without immediately
    /// re-reading the checkpoint file from disk.
    pub async fn load_from_disk(&self) -> Result<(), PersistentStorageError> {
        if let Some(wal) = CheckPoint::read_wal_from_path(&self.wal_ckpt_path).await? {
            *self.wal_cache.write().await = Some(wal);
        }
        Ok(())
    }

    pub async fn get_wal(&self) -> Option<WalCheckpoint> {
        self.wal_cache.read().await.clone()
    }

    /// Update the in-memory checkpoint cache and persist the same snapshot to disk.
    ///
    /// The cache is updated first so in-process readers immediately observe the newest topology even
    /// before the filesystem write completes. Disk persistence then makes that snapshot recoverable
    /// across process restarts.
    pub async fn update_wal(&self, ckpt: &WalCheckpoint) -> Result<(), PersistentStorageError> {
        // Update cache first for readers, then atomically persist to disk
        *self.wal_cache.write().await = Some(ckpt.clone());
        CheckPoint::write_wal_to_path(ckpt, &self.wal_ckpt_path).await
    }
}
