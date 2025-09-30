use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};

use crate::cloud_store::CloudStore;
use crate::etcd_metadata::{EtcdMetadata, ObjectDescriptor};

/// CloudReader reads historical messages for a topic from cloud objects
/// that contain raw WAL frames as uploaded by the Uploader.
///
/// Frame format per record (little-endian):
/// - `u64 offset`
/// - `u32 len`
/// - `u32 crc`
/// - `len` bytes (bincode-serialized `StreamMessage`)
#[derive(Clone, Debug)]
pub struct CloudReader {
    cloud: CloudStore,
    etcd: EtcdMetadata,
    topic_path: String,
}

impl CloudReader {
    /// Create a new CloudReader for a given topic path.
    ///
    /// `topic_path` should be the logical topic identifier used in metadata
    /// (e.g., "ns/topic").
    pub fn new(cloud: CloudStore, etcd: EtcdMetadata, topic_path: String) -> Self {
        Self {
            cloud,
            etcd,
            topic_path,
        }
    }

    /// Return the topic path associated with this CloudReader.
    pub fn topic_path(&self) -> &str {
        &self.topic_path
    }

    /// Expose the underlying Etcd metadata helper (primarily for tests).
    pub fn etcd(&self) -> &EtcdMetadata {
        &self.etcd
    }

    /// Read messages in the inclusive range `[start, end_inclusive]` from cloud objects.
    /// If `end_inclusive` is None, read all available objects starting at `start`.
    ///
    /// Objects are discovered via ETCD object descriptors and filtered by
    /// `[start_offset, end_offset]` overlap. Each selected object is decoded
    /// as a sequence of raw WAL frames. Frames outside the requested range
    /// are filtered out. The resulting stream yields messages ordered by offset.
    pub async fn read_range(
        &self,
        start: u64,
        end_inclusive: Option<u64>,
    ) -> Result<TopicStream, PersistentStorageError> {
        let from_padded = format!("{:020}", start);
        let descriptors = self
            .etcd
            .get_object_descriptors_range(self.topic_path(), &from_padded, None)
            .await?;
        // Filter descriptors to those overlapping our range
        let filtered: Vec<ObjectDescriptor> = descriptors
            .into_iter()
            .filter(|d| d.end_offset >= start)
            .filter(|d| match end_inclusive {
                Some(end) => d.start_offset <= end,
                None => true,
            })
            .collect();

        // Collect all messages from selected objects
        let mut all_msgs: Vec<(u64, StreamMessage)> = Vec::new();
        for desc in filtered {
            let key = format!(
                "storage/topics/{}/objects/{}",
                self.topic_path(),
                desc.object_id
            );
            let bytes = self.cloud.get_object(&key).await?;
            let mut msgs = parse_raw_frames(&bytes)?;
            // Filter offsets within the requested range
            msgs.retain(|(off, _)| {
                *off >= start && end_inclusive.map(|e| *off <= e).unwrap_or(true)
            });
            all_msgs.extend(msgs);
        }
        // Ensure global ordering by offset across objects
        all_msgs.sort_by_key(|(o, _)| *o);
        let stream = tokio_stream::iter(all_msgs.into_iter().map(|(_, m)| Ok(m)));
        Ok(Box::pin(stream))
    }
}

/// Parse a cloud object composed of concatenated raw WAL frames and return
/// a vector of `(offset, StreamMessage)` pairs. Stops gracefully at the first
/// partial frame at the end of the object.
fn parse_raw_frames(bytes: &[u8]) -> Result<Vec<(u64, StreamMessage)>, PersistentStorageError> {
    let mut idx = 0usize;
    let mut out = Vec::new();
    while idx + 8 + 4 + 4 <= bytes.len() {
        let off = u64::from_le_bytes(bytes[idx..idx + 8].try_into().unwrap());
        idx += 8;
        let len = u32::from_le_bytes(bytes[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;
        let _crc = u32::from_le_bytes(bytes[idx..idx + 4].try_into().unwrap());
        idx += 4;
        if idx + len > bytes.len() {
            // Partial payload at end => stop without error
            break;
        }
        let rec = &bytes[idx..idx + len];
        idx += len;
        let msg: StreamMessage = bincode::deserialize(rec).map_err(|e| {
            PersistentStorageError::Other(format!("cloud_reader: bincode decode failed: {}", e))
        })?;
        out.push((off, msg));
    }
    Ok(out)
}
