use crate::opendal::storage_config::BackendConfig;
// Re-export split helpers for backward compatibility with existing tests
use danube_core::storage::PersistentStorageError;
use opendal::Operator;

#[derive(Debug, Clone)]
pub struct OpendalStore {
    /// Optional extra prefix for key joining (used by Local backends)
    pub(crate) root_prefix: String,
    /// Opendal operator
    pub(crate) op: Operator,
    /// Provider identifier for metrics labeling (e.g., s3, gcs, azblob, fs, memory)
    pub(crate) provider: String,
}

impl OpendalStore {
    pub fn new(cfg: BackendConfig) -> Result<Self, PersistentStorageError> {
        let (op, root_prefix, provider) = cfg.build_operator()?;
        Ok(Self { root_prefix, op, provider })
    }

    #[inline]
    pub fn provider(&self) -> &str { &self.provider }

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
                PersistentStorageError::Other(format!("opendal writer {}: {}", key, e))
            })?;
        // Write requires owned data for the async future; pass a Buffer
        let buf = opendal::Buffer::from(bytes.to_vec());
        writer
            .write(buf)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("opendal write {}: {}", key, e)))?;
        let meta = writer
            .close()
            .await
            .map_err(|e| PersistentStorageError::Other(format!("opendal close {}: {}", key, e)))?;
        Ok(meta)
    }

    /// Create a reader for the given path and initialize its starting byte offset.
    /// We will perform range reads via Reader::read(start..end) using this starting offset.
    pub async fn open_ranged_reader(
        &self,
        path: &str,
        start_byte: u64,
    ) -> Result<OpendalRangeReader, PersistentStorageError> {
        let key = self.join(path);
        let reader =
            self.op.reader(&key).await.map_err(|e| {
                PersistentStorageError::Other(format!("opendal reader {}: {}", key, e))
            })?;
        // Stat to get content length for safe range bounds
        let meta = self
            .op
            .stat(&key)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("opendal stat {}: {}", key, e)))?;
        let size = meta.content_length();
        Ok(OpendalRangeReader {
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

    /// Delete an object by key. No-op if object doesn't exist.
    pub async fn delete_object(&self, path: &str) -> Result<(), PersistentStorageError> {
        let key = self.join(path);
        self.op
            .delete(&key)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("opendal delete {}: {}", key, e)))
    }
}

/// Ranged opendal reader wrapper that supports chunked reads via explicit ranges.
pub struct OpendalRangeReader {
    pub(crate) inner: opendal::Reader,
    pub(crate) offset: u64,
    pub(crate) size: u64,
}

impl OpendalRangeReader {
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
            .map_err(|e| PersistentStorageError::Other(format!("opendal read: {}", e)))?;
        self.offset += buf.len() as u64;
        Ok(buf.to_vec())
    }
}

// helper functions moved into storage_config.rs
