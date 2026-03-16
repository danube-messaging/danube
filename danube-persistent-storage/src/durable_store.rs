use crate::opendal::{BackendConfig, OpendalRangeReader, OpendalStore};
use async_trait::async_trait;
use danube_core::storage::PersistentStorageError;

#[derive(Debug, Clone)]
pub struct DurableObjectMetadata {
    content_length: u64,
    etag: Option<String>,
}

impl DurableObjectMetadata {
    pub(crate) fn from_opendal(meta: opendal::Metadata) -> Self {
        Self {
            content_length: meta.content_length(),
            etag: meta.etag().map(|etag| etag.to_string()),
        }
    }

    pub fn content_length(&self) -> u64 {
        self.content_length
    }

    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }
}

pub struct DurableRangeReader {
    inner: opendal::Reader,
    offset: u64,
    size: u64,
}

impl DurableRangeReader {
    fn from_backend_reader(reader: OpendalRangeReader) -> Self {
        Self {
            inner: reader.inner,
            offset: reader.offset,
            size: reader.size,
        }
    }

    pub async fn read_chunk(
        &mut self,
        chunk_size: usize,
    ) -> Result<Vec<u8>, PersistentStorageError> {
        if self.offset >= self.size {
            return Ok(Vec::new());
        }
        let end = std::cmp::min(self.offset + chunk_size as u64, self.size);
        let buf = self
            .inner
            .read(self.offset..end)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("durable read: {}", e)))?;
        self.offset += buf.len() as u64;
        Ok(buf.to_vec())
    }
}

#[async_trait]
pub trait DurableStore: Send + Sync + std::fmt::Debug {
    fn provider(&self) -> &str;
    async fn put_segment(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<DurableObjectMetadata, PersistentStorageError>;
    async fn open_segment_reader(
        &self,
        path: &str,
        start_byte: u64,
    ) -> Result<DurableRangeReader, PersistentStorageError>;
    async fn delete_segment(&self, path: &str) -> Result<(), PersistentStorageError>;
}

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
        self.inner
            .open_ranged_reader(path, start_byte)
            .await
            .map(DurableRangeReader::from_backend_reader)
    }

    async fn delete_segment(&self, path: &str) -> Result<(), PersistentStorageError> {
        self.inner.delete_object(path).await
    }
}
