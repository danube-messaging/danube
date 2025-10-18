use crate::cloud::storage_config::BackendConfig;
// Re-export split helpers for backward compatibility with existing tests
use danube_core::storage::PersistentStorageError;
use opendal::Operator;

#[derive(Debug, Clone)]
pub struct CloudStore {
    /// Optional extra prefix for key joining (used by Local backends)
    pub(crate) root_prefix: String,
    /// Opendal operator
    pub(crate) op: Operator,
}

impl CloudStore {
    pub fn new(cfg: BackendConfig) -> Result<Self, PersistentStorageError> {
        let (op, root_prefix) = cfg.build_operator()?;
        Ok(Self { root_prefix, op })
    }

    pub async fn put_object(&self, path: &str, bytes: &[u8]) -> Result<(), PersistentStorageError> {
        // Use Writer-based API to allow backend MPU and return Metadata; discard it here.
        let _ = self.put_object_meta(path, bytes).await?;
        Ok(())
    }

    pub async fn get_object(&self, path: &str) -> Result<Vec<u8>, PersistentStorageError> {
        let key = self.join(path);
        let data = self.op.read(&key).await.map_err(|e| {
            PersistentStorageError::Other(format!("cloud get_object {}: {}", key, e))
        })?;
        Ok(data.to_vec())
    }

    /// Write an object using the streaming writer API and return backend-provided metadata.
    /// This enables multipart uploads and exposes fields like ETag where supported.
    pub async fn put_object_meta(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<opendal::Metadata, PersistentStorageError> {
        let key = self.join(path);
        let mut writer =
            self.op.writer(&key).await.map_err(|e| {
                PersistentStorageError::Other(format!("cloud writer {}: {}", key, e))
            })?;
        // Write requires owned data for the async future; pass a Buffer
        let buf = opendal::Buffer::from(bytes.to_vec());
        writer
            .write(buf)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("cloud write {}: {}", key, e)))?;
        let meta = writer
            .close()
            .await
            .map_err(|e| PersistentStorageError::Other(format!("cloud close {}: {}", key, e)))?;
        Ok(meta)
    }

    /// Create a streaming writer to the given path with configurable chunk size and concurrency.
    /// This uses OpenDAL's writer_with API which enables multipart uploads for cloud backends.
    pub async fn open_streaming_writer(
        &self,
        path: &str,
        chunk_size: usize,
        concurrent: usize,
    ) -> Result<CloudWriter, PersistentStorageError> {
        let key = self.join(path);
        let writer = self
            .op
            .writer_with(&key)
            .chunk(chunk_size)
            .concurrent(concurrent)
            .await
            .map_err(|e| {
                PersistentStorageError::Other(format!("cloud writer_with {}: {}", key, e))
            })?;
        Ok(CloudWriter {
            inner: writer,
            bytes_written: 0,
        })
    }

    /// Create a reader for the given path and initialize its starting byte offset.
    /// We will perform range reads via Reader::read(start..end) using this starting offset.
    pub async fn open_ranged_reader(
        &self,
        path: &str,
        start_byte: u64,
    ) -> Result<CloudRangeReader, PersistentStorageError> {
        let key = self.join(path);
        let reader =
            self.op.reader(&key).await.map_err(|e| {
                PersistentStorageError::Other(format!("cloud reader {}: {}", key, e))
            })?;
        // Stat to get content length for safe range bounds
        let meta = self
            .op
            .stat(&key)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("cloud stat {}: {}", key, e)))?;
        let size = meta.content_length();
        Ok(CloudRangeReader {
            inner: reader,
            offset: start_byte,
            size,
        })
    }

    #[inline]
    fn join(&self, path: &str) -> String {
        let p = path.trim_matches('/');
        if self.root_prefix.is_empty() {
            p.to_string()
        } else {
            format!("{}/{}", self.root_prefix.trim_matches('/'), p)
        }
    }

    /// Copy an object from one key to another. Some backends may implement this
    /// as server-side copy. For services without native rename, this is the
    /// recommended way to materialize the final key.
    pub async fn copy_object(
        &self,
        from_path: &str,
        to_path: &str,
    ) -> Result<(), PersistentStorageError> {
        let from = self.join(from_path);
        let to = self.join(to_path);
        self.op.copy(&from, &to).await.map_err(|e| {
            PersistentStorageError::Other(format!("cloud copy {} -> {}: {}", from, to, e))
        })
    }

    /// Delete an object by key. No-op if object doesn't exist.
    pub async fn delete_object(&self, path: &str) -> Result<(), PersistentStorageError> {
        let key = self.join(path);
        self.op
            .delete(&key)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("cloud delete {}: {}", key, e)))
    }
}

/// Streaming cloud writer wrapper.
pub struct CloudWriter {
    pub(crate) inner: opendal::Writer,
    pub(crate) bytes_written: u64,
}

impl CloudWriter {
    pub async fn write(&mut self, buf: &[u8]) -> Result<(), PersistentStorageError> {
        let buffer = opendal::Buffer::from(buf.to_vec());
        self.inner
            .write(buffer)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("cloud write: {}", e)))?;
        self.bytes_written += buf.len() as u64;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<opendal::Metadata, PersistentStorageError> {
        self.inner
            .close()
            .await
            .map_err(|e| PersistentStorageError::Other(format!("cloud close: {}", e)))
    }
}

/// Ranged cloud reader wrapper that supports chunked reads via explicit ranges.
pub struct CloudRangeReader {
    pub(crate) inner: opendal::Reader,
    pub(crate) offset: u64,
    pub(crate) size: u64,
}

impl CloudRangeReader {
    /// Read the next chunk of up to chunk_size bytes.
    pub async fn read_chunk(
        &mut self,
        chunk_size: usize,
    ) -> Result<Vec<u8>, PersistentStorageError> {
        if self.offset >= self.size {
            return Ok(Vec::new());
        }
        let end: u64 = std::cmp::min(self.offset + (chunk_size as u64), self.size);
        let buf = self
            .inner
            .read(self.offset..end)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("cloud read: {}", e)))?;
        self.offset += buf.len() as u64;
        Ok(buf.to_vec())
    }
}

// helper functions moved into storage_config.rs
