use danube_core::storage::PersistentStorageError;

#[derive(Debug, Clone)]
pub enum CloudBackend {
    S3,
    Gcs,
    Fs,
    Memory,
}

#[derive(Debug, Clone)]
pub struct CloudConfig {
    pub backend: CloudBackend,
    pub root: String,
}

#[derive(Debug, Clone)]
pub struct CloudStore {
    cfg: CloudConfig,
}

impl CloudStore {
    pub fn new(cfg: CloudConfig) -> Result<Self, PersistentStorageError> {
        Ok(Self { cfg })
    }

    pub async fn put_object(
        &self,
        path: &str,
        _bytes: &[u8],
    ) -> Result<(), PersistentStorageError> {
        // Placeholder: no-op until opendal is integrated
        tracing::debug!(target: "cloud_store", "put_object stub: {}/{} ({} bytes)", self.cfg.root, path, _bytes.len());
        Ok(())
    }

    pub async fn get_object(&self, path: &str) -> Result<Vec<u8>, PersistentStorageError> {
        // Placeholder: no-op
        tracing::debug!(target: "cloud_store", "get_object stub: {}/{}", self.cfg.root, path);
        Ok(Vec::new())
    }
}
