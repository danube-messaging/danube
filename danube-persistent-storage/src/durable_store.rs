use crate::cloud::{BackendConfig, CloudStore};
use async_trait::async_trait;
use danube_core::storage::PersistentStorageError;

#[async_trait]
pub trait DurableStore: Send + Sync {
    async fn put_bytes(&self, path: &str, bytes: &[u8]) -> Result<(), PersistentStorageError>;
    async fn get_bytes(&self, path: &str) -> Result<Vec<u8>, PersistentStorageError>;
    async fn delete(&self, path: &str) -> Result<(), PersistentStorageError>;
}

#[derive(Debug, Clone)]
pub enum DurableStoreConfig {
    Local {
        root: String,
    },
    SharedFs {
        root: String,
    },
    Cloud {
        backend: BackendConfig,
    },
}

#[derive(Debug, Clone)]
pub struct OpendalDurableStore {
    inner: CloudStore,
}

impl OpendalDurableStore {
    pub fn new(inner: CloudStore) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> CloudStore {
        self.inner
    }

    pub fn as_cloud_store(&self) -> &CloudStore {
        &self.inner
    }
}

#[async_trait]
impl DurableStore for OpendalDurableStore {
    async fn put_bytes(&self, path: &str, bytes: &[u8]) -> Result<(), PersistentStorageError> {
        self.inner.put_object(path, bytes).await
    }

    async fn get_bytes(&self, path: &str) -> Result<Vec<u8>, PersistentStorageError> {
        self.inner.get_object(path).await
    }

    async fn delete(&self, path: &str) -> Result<(), PersistentStorageError> {
        self.inner.delete_object(path).await
    }
}
