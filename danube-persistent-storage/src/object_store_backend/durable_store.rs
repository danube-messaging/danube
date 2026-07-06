use crate::durable_store::{DurableObjectMetadata, DurableRangeReader, DurableStore};
use crate::object_store_backend::{BackendConfig, ObjStore};
use async_trait::async_trait;
use danube_core::storage::PersistentStorageError;

#[derive(Debug, Clone)]
pub struct ObjStoreDurable {
    inner: ObjStore,
}

impl ObjStoreDurable {
    pub(crate) fn new(inner: ObjStore) -> Self {
        Self { inner }
    }

    pub(crate) fn from_backend(cfg: BackendConfig) -> Result<Self, PersistentStorageError> {
        Ok(Self {
            inner: ObjStore::new(cfg)?,
        })
    }
}

#[async_trait]
impl DurableStore for ObjStoreDurable {
    fn provider(&self) -> &str {
        self.inner.provider()
    }

    async fn put_segment(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<DurableObjectMetadata, PersistentStorageError> {
        let meta = self.inner.put_object_meta(path, bytes).await?;
        Ok(DurableObjectMetadata::new(meta.content_length, meta.etag))
    }

    async fn open_segment_reader(
        &self,
        path: &str,
        start_byte: u64,
    ) -> Result<DurableRangeReader, PersistentStorageError> {
        let reader = self.inner.open_ranged_reader(path, start_byte).await?;
        Ok(DurableRangeReader::new(reader))
    }

    async fn delete_segment(&self, path: &str) -> Result<(), PersistentStorageError> {
        self.inner.delete_object(path).await
    }
}
