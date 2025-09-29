use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalCheckpoint {
    pub last_offset: u64,
    pub file_seq: u64,
    pub file_path: String,
    // Optional rotation history to help locate frames across rotated files
    pub rotated_files: Vec<(u64, PathBuf)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UploaderCheckpoint {
    pub last_committed_offset: u64,
    pub last_object_id: Option<String>,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CheckPoint {
    pub wal: WalCheckpoint,
    pub uploader: UploaderCheckpoint,
}

// Persistence helpers for uploader checkpoint
impl UploaderCheckpoint {
    pub async fn write_to_path(&self, path: &std::path::PathBuf) -> Result<(), danube_core::storage::PersistentStorageError> {
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;
        use tracing::warn;
        let bytes = bincode::serialize(self).map_err(|e| {
            warn!(target = "wal", error = %e, "uploader ckpt serialize failed");
            danube_core::storage::PersistentStorageError::Io(format!("uploader ckpt serialize failed: {}", e))
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
                danube_core::storage::PersistentStorageError::Io(format!("open uploader ckpt tmp failed: {}", e))
            })?;
        f.write_all(&bytes)
            .await
            .map_err(|e| {
                warn!(target = "wal", path = %tmp.display(), error = %e, "write uploader ckpt failed");
                danube_core::storage::PersistentStorageError::Io(format!("write uploader ckpt failed: {}", e))
            })?;
        f.flush()
            .await
            .map_err(|e| {
                warn!(target = "wal", path = %tmp.display(), error = %e, "flush uploader ckpt failed");
                danube_core::storage::PersistentStorageError::Io(format!("flush uploader ckpt failed: {}", e))
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

    pub async fn read_from_path(path: &std::path::PathBuf) -> Result<Option<Self>, danube_core::storage::PersistentStorageError> {
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
                Err(danube_core::storage::PersistentStorageError::Io(format!("read uploader ckpt failed: {}", e)))
            }
        }
    }
}
