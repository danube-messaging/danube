use std::path::PathBuf;

use bincode;
use danube_core::storage::PersistentStorageError;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn};

/// Checkpoint for the uploader component that flushes WAL data to cloud/object storage.
///
/// Fields
/// - `last_committed_offset`: highest WAL offset that has been successfully uploaded.
/// - `last_object_id`: optional identifier of the last completed object (for resume/debug).
/// - `updated_at`: unix timestamp (seconds) of when this checkpoint was written.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UploaderCheckpoint {
    pub last_committed_offset: u64,
    pub last_object_id: Option<String>,
    pub updated_at: u64,
}

impl UploaderCheckpoint {
    /// Atomically persist this checkpoint to `path` using bincode encoding (tmp + rename).
    pub async fn write_to_path(&self, path: &PathBuf) -> Result<(), PersistentStorageError> {
        let bytes = bincode::serialize(self).map_err(|e| {
            warn!(target = "wal", error = %e, "uploader ckpt serialize failed");
            PersistentStorageError::Io(format!("uploader ckpt serialize failed: {}", e))
        })?;
        let tmp = path.with_extension("ckpt.tmp");
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .await
            .map_err(|e| {
                warn!(target = "wal", path = %tmp.display(), error = %e, "open uploader ckpt tmp failed");
                PersistentStorageError::Io(format!("open uploader ckpt tmp failed: {}", e))
            })?;
        f.write_all(&bytes)
            .await
            .map_err(|e| {
                warn!(target = "wal", path = %tmp.display(), error = %e, "write uploader ckpt failed");
                PersistentStorageError::Io(format!("write uploader ckpt failed: {}", e))
            })?;
        f.flush()
            .await
            .map_err(|e| {
                warn!(target = "wal", path = %tmp.display(), error = %e, "flush uploader ckpt failed");
                PersistentStorageError::Io(format!("flush uploader ckpt failed: {}", e))
            })?;
        tokio::fs::rename(&tmp, path)
            .await
            .map_err(|e| {
                warn!(target = "wal", from = %tmp.display(), to = %path.display(), error = %e, "rename uploader ckpt failed");
                PersistentStorageError::Io(format!("rename uploader ckpt failed: {}", e))
            })?;
        debug!(target = "wal", path = %path.display(), size = bytes.len(), "wrote uploader checkpoint");
        Ok(())
    }

    /// Read checkpoint from `path` if present; returns `Ok(None)` when the file does not exist.
    pub async fn read_from_path(path: &PathBuf) -> Result<Option<Self>, PersistentStorageError> {
        match tokio::fs::read(path).await {
            Ok(bytes) => {
                let ckpt: UploaderCheckpoint = bincode::deserialize(&bytes).map_err(|e| {
                    warn!(target = "wal", path = %path.display(), error = %e, "uploader ckpt parse failed");
                    PersistentStorageError::Io(format!("uploader ckpt parse failed: {}", e))
                })?;
                debug!(target = "wal", path = %path.display(), size = bytes.len(), "read uploader checkpoint");
                Ok(Some(ckpt))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => {
                warn!(target = "wal", path = %path.display(), error = %e, "read uploader ckpt failed");
                Err(PersistentStorageError::Io(format!("read uploader ckpt failed: {}", e)))
            }
        }
    }
}
