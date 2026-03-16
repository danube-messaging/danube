use crate::durable_store::{DurableObjectMetadata, DurableRangeReader, DurableStore};
use crate::opendal::{BackendConfig, OpendalStore};
use async_trait::async_trait;
use danube_core::storage::PersistentStorageError;

#[derive(Debug, Clone)]
pub struct OpendalDurableStore {
    inner: OpendalStore,
}

impl OpendalDurableStore {
    pub fn new(inner: OpendalStore) -> Self {
        Self { inner }
    }

    pub fn from_backend(cfg: BackendConfig) -> Result<Self, PersistentStorageError> {
        Ok(Self {
            inner: OpendalStore::new(cfg)?,
        })
    }
}

#[async_trait]
impl DurableStore for OpendalDurableStore {
    fn provider(&self) -> &str {
        self.inner.provider()
    }

    async fn put_segment(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<DurableObjectMetadata, PersistentStorageError> {
        self.inner
            .put_object_meta(path, bytes)
            .await
            .map(DurableObjectMetadata::from_opendal)
    }

    async fn open_segment_reader(
        &self,
        path: &str,
        start_byte: u64,
    ) -> Result<DurableRangeReader, PersistentStorageError> {
        let reader = self.inner.open_ranged_reader(path, start_byte).await?;
        Ok(DurableRangeReader::from_raw_parts(
            reader.inner,
            reader.offset,
            reader.size,
        ))
    }

    async fn delete_segment(&self, path: &str) -> Result<(), PersistentStorageError> {
        self.inner.delete_object(path).await
    }
}
