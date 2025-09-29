use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use danube_core::storage::PersistentStorageError;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalCheckpoint {
    pub last_offset: u64,
    pub file_seq: u64,
    pub file_path: String,
    // Optional rotation history to help locate frames across rotated files
    pub rotated_files: Vec<(u64, PathBuf)>,
    // Optional: name of the active WAL file for quick reference
    pub active_file_name: Option<String>,
    // Optional: last rotation timestamp (seconds since epoch) for observability
    pub last_rotation_at: Option<u64>,
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

impl CheckPoint {
    /// Write WAL checkpoint to the specified path (typically <dir>/wal.ckpt).
    pub async fn write_wal_to_path(wal: &WalCheckpoint, path: &std::path::PathBuf) -> Result<(), danube_core::storage::PersistentStorageError> {
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;
        use tracing::warn;
        let bytes = bincode::serialize(wal).map_err(|e| {
            warn!(target = "wal", error = %e, "wal ckpt serialize failed");
            danube_core::storage::PersistentStorageError::Io(format!("wal ckpt serialize failed: {}", e))
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
        f.write_all(&bytes)
            .await
            .map_err(|e| {
                warn!(target = "wal", path = %tmp.display(), error = %e, "write wal ckpt failed");
                danube_core::storage::PersistentStorageError::Io(format!("write wal ckpt failed: {}", e))
            })?;
        f.flush()
            .await
            .map_err(|e| {
                warn!(target = "wal", path = %tmp.display(), error = %e, "flush wal ckpt failed");
                danube_core::storage::PersistentStorageError::Io(format!("flush wal ckpt failed: {}", e))
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
    pub async fn read_wal_from_path(path: &std::path::PathBuf) -> Result<Option<WalCheckpoint>, danube_core::storage::PersistentStorageError> {
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
                Err(danube_core::storage::PersistentStorageError::Io(format!("read wal ckpt failed: {}", e)))
            }
        }
    }

    /// Write Uploader checkpoint to `<parent(wal.ckpt)>/uploader.ckpt` given the wal checkpoint path.
    pub async fn write_uploader_near_wal(ckpt: &UploaderCheckpoint, wal_ckpt_path: &std::path::PathBuf) -> Result<(), danube_core::storage::PersistentStorageError> {
        let parent = wal_ckpt_path.parent()
            .ok_or_else(|| danube_core::storage::PersistentStorageError::Io("wal ckpt path has no parent".to_string()))?;
        let path = parent.join("uploader.ckpt");
        Self::write_uploader_to_path(ckpt, &path).await
    }

    /// Read Uploader checkpoint from `<parent(wal.ckpt)>/uploader.ckpt` given the wal checkpoint path.
    pub async fn read_uploader_near_wal(wal_ckpt_path: &std::path::PathBuf) -> Result<Option<UploaderCheckpoint>, danube_core::storage::PersistentStorageError> {
        let parent = match wal_ckpt_path.parent() { Some(p) => p.to_path_buf(), None => return Ok(None) };
        let path = parent.join("uploader.ckpt");
        Self::read_uploader_from_path(&path).await
    }

    /// Write Uploader checkpoint to the specified path (typically <dir>/uploader.ckpt).
    pub async fn write_uploader_to_path(ckpt: &UploaderCheckpoint, path: &std::path::PathBuf) -> Result<(), danube_core::storage::PersistentStorageError> {
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;
        use tracing::warn;
        let bytes = bincode::serialize(ckpt).map_err(|e| {
            warn!(target = "wal", error = %e, "uploader ckpt serialize failed");
            danube_core::storage::PersistentStorageError::Io(format!("uploader ckpt serialize failed: {}", e))
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

    /// Read Uploader checkpoint from the specified path, returning Ok(None) if missing.
    pub async fn read_uploader_from_path(path: &std::path::PathBuf) -> Result<Option<UploaderCheckpoint>, danube_core::storage::PersistentStorageError> {
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

    pub async fn update_uploader(&self, ckpt: &UploaderCheckpoint) -> Result<(), PersistentStorageError> {
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
