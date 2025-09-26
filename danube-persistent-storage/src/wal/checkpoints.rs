use std::path::PathBuf;

use bincode;
use danube_core::storage::PersistentStorageError;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UploaderCheckpoint {
    pub last_committed_offset: u64,
    pub last_object_id: Option<String>,
    pub updated_at: u64,
}

impl UploaderCheckpoint {
    pub async fn write_to_path(&self, path: &PathBuf) -> Result<(), PersistentStorageError> {
        let bytes = bincode::serialize(self)
            .map_err(|e| PersistentStorageError::Io(format!("uploader ckpt serialize failed: {}", e)))?;
        let tmp = path.with_extension("ckpt.tmp");
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .await
            .map_err(|e| PersistentStorageError::Io(format!("open uploader ckpt tmp failed: {}", e)))?;
        f.write_all(&bytes)
            .await
            .map_err(|e| PersistentStorageError::Io(format!("write uploader ckpt failed: {}", e)))?;
        f.flush()
            .await
            .map_err(|e| PersistentStorageError::Io(format!("flush uploader ckpt failed: {}", e)))?;
        tokio::fs::rename(&tmp, path)
            .await
            .map_err(|e| PersistentStorageError::Io(format!("rename uploader ckpt failed: {}", e)))?;
        Ok(())
    }

    pub async fn read_from_path(path: &PathBuf) -> Result<Option<Self>, PersistentStorageError> {
        match tokio::fs::read(path).await {
            Ok(bytes) => {
                let ckpt: UploaderCheckpoint = bincode::deserialize(&bytes).map_err(|e| {
                    PersistentStorageError::Io(format!("uploader ckpt parse failed: {}", e))
                })?;
                Ok(Some(ckpt))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PersistentStorageError::Io(format!(
                "read uploader ckpt failed: {}",
                e
            ))),
        }
    }
}
