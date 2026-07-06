use crate::durable_store::DurableChunkReader;
use crate::object_store_backend::config::BackendConfig;
use async_trait::async_trait;
use bytes::Bytes;
use danube_core::storage::PersistentStorageError;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart,
};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::{Stream, StreamExt};

/// Objects larger than this threshold use multipart upload for parallel chunked
/// transfer. Below this size, a single atomic `put()` is used.
const MULTIPART_THRESHOLD: usize = 8 * 1024 * 1024; // 8 MiB

#[derive(Debug, Clone)]
pub(crate) struct ObjStore {
    /// Optional extra prefix for key joining (used by Local and Memory backends)
    pub(crate) root_prefix: String,
    /// object_store implementation
    pub(crate) store: Arc<dyn ObjectStore>,
    /// Provider identifier for metrics labeling (e.g., s3, gcs, azblob, fs, memory)
    pub(crate) provider: String,
}

/// Metadata returned from a put operation.
pub(crate) struct PutMeta {
    pub content_length: u64,
    pub etag: Option<String>,
}

impl ObjStore {
    pub(crate) fn new(cfg: BackendConfig) -> Result<Self, PersistentStorageError> {
        let (store, root_prefix, provider) = cfg.build_object_store()?;
        Ok(Self {
            root_prefix,
            store,
            provider,
        })
    }

    #[inline]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Write an object and return metadata (content_length, etag).
    ///
    /// For objects below `MULTIPART_THRESHOLD` (8 MiB), uses a single atomic `put()`.
    /// For larger objects, uses `put_multipart` + `WriteMultipart` which splits the
    /// data into parts and uploads them in parallel for better throughput.
    pub async fn put_object_meta(
        &self,
        path: &str,
        data: &[u8],
    ) -> Result<PutMeta, PersistentStorageError> {
        let key = self.join(path);
        let location = Path::from(key.as_str());

        let result = if data.len() < MULTIPART_THRESHOLD {
            // Small object — single atomic put
            let payload = PutPayload::from(Bytes::copy_from_slice(data));
            self.store.put(&location, payload).await.map_err(|e| {
                PersistentStorageError::Other(format!("object_store put {}: {}", key, e))
            })?
        } else {
            // Large object — multipart upload for parallel chunked transfer
            let upload = self.store.put_multipart(&location).await.map_err(|e| {
                PersistentStorageError::Other(format!(
                    "object_store put_multipart {}: {}",
                    key, e
                ))
            })?;
            let mut writer = WriteMultipart::new(upload);
            writer.write(data);
            writer.finish().await.map_err(|e| {
                PersistentStorageError::Other(format!(
                    "object_store multipart finish {}: {}",
                    key, e
                ))
            })?
        };

        Ok(PutMeta {
            content_length: data.len() as u64,
            etag: result.e_tag,
        })
    }

    /// Create a streaming reader for the given path starting at `start_byte`.
    ///
    /// Opens a single `get_opts` request with `GetRange::Offset(start_byte)` and
    /// converts the result into an async byte stream. This uses **one HTTP connection**
    /// for the entire read instead of issuing per-chunk `get_range()` round-trips.
    pub async fn open_ranged_reader(
        &self,
        path: &str,
        start_byte: u64,
    ) -> Result<ObjStoreRangeReader, PersistentStorageError> {
        let key = self.join(path);
        let location = Path::from(key.as_str());

        // Build a streaming GET from the start_byte offset to EOF
        let options = if start_byte > 0 {
            GetOptions {
                range: Some(GetRange::Offset(start_byte)),
                ..Default::default()
            }
        } else {
            GetOptions::default()
        };

        let get_result = self.store.get_opts(&location, options).await.map_err(|e| {
            PersistentStorageError::Other(format!("object_store get_opts {}: {}", key, e))
        })?;

        // Total size of the range we'll read
        let range = &get_result.range;
        let total_bytes = (range.end - range.start) as u64;

        // Convert to a streaming byte source — single HTTP connection
        let stream = get_result.into_stream();

        Ok(ObjStoreRangeReader {
            stream: Some(stream),
            buffer: VecDeque::new(),
            bytes_read: 0,
            total_bytes,
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
        let location = Path::from(key.as_str());
        self.store.delete(&location).await.map_err(|e| {
            PersistentStorageError::Other(format!("object_store delete {}: {}", key, e))
        })
    }

    /// Read an object in full (used in tests).
    #[cfg(test)]
    pub async fn get_object(&self, path: &str) -> Result<Vec<u8>, PersistentStorageError> {
        let key = self.join(path);
        let location = Path::from(key.as_str());
        let result = self.store.get(&location).await.map_err(|e| {
            PersistentStorageError::Other(format!("object_store get {}: {}", key, e))
        })?;
        let bytes = result.bytes().await.map_err(|e| {
            PersistentStorageError::Other(format!("object_store read bytes {}: {}", key, e))
        })?;
        Ok(bytes.to_vec())
    }
}

/// Streaming ranged reader backed by a single `get_opts().into_stream()` connection.
///
/// Instead of issuing N separate `get_range()` HTTP requests (one per chunk),
/// this reader opens a single streaming HTTP response and buffers incoming
/// byte chunks internally. Each `read_chunk()` call drains from the buffer,
/// fetching more stream chunks only as needed.
pub struct ObjStoreRangeReader {
    /// The underlying byte stream (consumed progressively, set to None at EOF).
    stream: Option<
        Pin<Box<dyn Stream<Item = Result<Bytes, object_store::Error>> + Send>>,
    >,
    /// Internal buffer of bytes received from the stream but not yet returned.
    buffer: VecDeque<u8>,
    /// Total bytes read so far (for remaining_bytes calculation).
    bytes_read: u64,
    /// Total bytes available in this range (from the GetResult range).
    total_bytes: u64,
}

impl ObjStoreRangeReader {
    /// Read the next chunk of up to `chunk_size` bytes.
    ///
    /// Drains from the internal buffer first. If the buffer is insufficient,
    /// pulls more data from the underlying stream until `chunk_size` bytes
    /// are available or the stream ends.
    pub async fn read_chunk(
        &mut self,
        chunk_size: usize,
    ) -> Result<Vec<u8>, PersistentStorageError> {
        // Fill buffer from stream until we have enough or EOF
        while self.buffer.len() < chunk_size {
            if let Some(stream) = self.stream.as_mut() {
                match stream.next().await {
                    Some(Ok(bytes)) => {
                        self.buffer.extend(bytes.iter());
                    }
                    Some(Err(e)) => {
                        return Err(PersistentStorageError::Other(format!(
                            "object_store stream read: {}",
                            e
                        )));
                    }
                    None => {
                        // Stream exhausted
                        self.stream = None;
                        break;
                    }
                }
            } else {
                break;
            }
        }

        // Drain up to chunk_size bytes from buffer
        let drain_len = std::cmp::min(chunk_size, self.buffer.len());
        if drain_len == 0 {
            return Ok(Vec::new());
        }

        let chunk: Vec<u8> = self.buffer.drain(..drain_len).collect();
        self.bytes_read += chunk.len() as u64;
        Ok(chunk)
    }
}

#[async_trait]
impl DurableChunkReader for ObjStoreRangeReader {
    fn remaining_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.bytes_read)
    }

    async fn read_chunk(
        &mut self,
        chunk_size: usize,
    ) -> Result<Vec<u8>, PersistentStorageError> {
        ObjStoreRangeReader::read_chunk(self, chunk_size).await
    }
}
