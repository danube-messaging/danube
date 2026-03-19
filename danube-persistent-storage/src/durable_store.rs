use async_trait::async_trait;
use danube_core::storage::PersistentStorageError;

#[derive(Debug, Clone)]
pub struct DurableObjectMetadata {
    content_length: u64,
    etag: Option<String>,
}

pub(crate) fn segment_object_path(topic_path: &str, segment_id: &str) -> String {
    format!("storage/topics/{}/segments/{}", topic_path, segment_id)
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

/// Sequential reader over a durable segment object.
///
/// The reader starts at the `start_byte` chosen by
/// `DurableStore::open_segment_reader()` and advances its internal cursor after
/// each successful `read_chunk()` call. Callers should continue reading until
/// `read_chunk()` returns an empty buffer, which indicates end-of-object.
pub struct DurableRangeReader {
    inner: opendal::Reader,
    offset: u64,
    size: u64,
}

impl DurableRangeReader {
    pub(crate) fn from_raw_parts(inner: opendal::Reader, offset: u64, size: u64) -> Self {
        Self {
            inner,
            offset,
            size,
        }
    }

    /// Return the number of unread bytes remaining in this range reader.
    pub fn remaining_bytes(&self) -> u64 {
        self.size.saturating_sub(self.offset)
    }

    /// Read the next chunk of bytes and advance the internal cursor.
    ///
    /// At most `chunk_size` bytes are returned. Once the reader reaches the end
    /// of the durable object, this returns an empty buffer on subsequent calls.
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

/// DurableStore is the central abstraction for durable segment I/O.
///
/// It persists exported or sealed immutable topic segments, opens readers over
/// those durable segments, and deletes them when retention or topic cleanup
/// requires it.
///
/// This trait does not define producer ack semantics, active WAL durability, or
/// background export and recovery policy. Those higher-level runtime guarantees
/// are owned by `StorageFactory`, `WalStorage`, `SegmentCatalog`, and
/// `MobilityState`.
#[async_trait]
pub trait DurableStore: Send + Sync + std::fmt::Debug {
    /// Return a stable provider label for logging, metrics, and diagnostics.
    fn provider(&self) -> &str;

    /// Persist a complete immutable segment object at `path`.
    ///
    /// Callers are expected to treat segment paths as stable immutable keys and
    /// should not rewrite the same path with different bytes.
    ///
    /// On success, the returned metadata describes the durable object that was
    /// made available for later historical reads.
    async fn put_segment(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<DurableObjectMetadata, PersistentStorageError>;

    /// Open a reader for a previously persisted segment object.
    ///
    /// `start_byte` is a byte offset within the durable segment and is used by
    /// historical readers to begin scanning near a requested message offset.
    async fn open_segment_reader(
        &self,
        path: &str,
        start_byte: u64,
    ) -> Result<DurableRangeReader, PersistentStorageError>;

    /// Delete a segment object that is no longer referenced by retention or
    /// topic cleanup metadata.
    async fn delete_segment(&self, path: &str) -> Result<(), PersistentStorageError>;
}
