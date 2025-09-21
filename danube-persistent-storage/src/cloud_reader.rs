use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};

use crate::cloud_store::CloudStore;
use crate::etcd_metadata::{EtcdMetadata, ObjectDescriptor};

#[derive(Clone, Debug)]
pub struct CloudReader {
    cloud: CloudStore,
    etcd: EtcdMetadata,
    topic_path: String,
}

impl CloudReader {
    pub fn new(cloud: CloudStore, etcd: EtcdMetadata, topic_path: String) -> Self {
        Self {
            cloud,
            etcd,
            topic_path,
        }
    }

    pub fn topic_path(&self) -> &str {
        &self.topic_path
    }

    pub fn etcd(&self) -> &EtcdMetadata {
        &self.etcd
    }

    /// Read messages in the inclusive range [start, end_inclusive] from cloud objects.
    /// If end_inclusive is None, read all available objects starting at `start`.
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
            let mut msgs = parse_dnb1(&bytes)?;
            // Filter offsets within the requested range
            msgs.retain(|(off, _)| {
                *off >= start && end_inclusive.map(|e| *off <= e).unwrap_or(true)
            });
            all_msgs.extend(msgs);
        }
        all_msgs.sort_by_key(|(o, _)| *o);
        let stream = tokio_stream::iter(all_msgs.into_iter().map(|(_, m)| Ok(m)));
        Ok(Box::pin(stream))
    }
}

fn parse_dnb1(bytes: &[u8]) -> Result<Vec<(u64, StreamMessage)>, PersistentStorageError> {
    // minimal validation of header
    if bytes.len() < 4 + 1 + 4 {
        return Err(PersistentStorageError::Other(
            "cloud_reader: object too small".to_string(),
        ));
    }
    if &bytes[0..4] != b"DNB1" {
        return Err(PersistentStorageError::Other(
            "cloud_reader: invalid magic (expected DNB1)".to_string(),
        ));
    }
    let version = bytes[4];
    if version != 1u8 {
        return Err(PersistentStorageError::Other(format!(
            "cloud_reader: unsupported version {}",
            version
        )));
    }
    let count = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let mut idx = 9usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if idx + 8 + 4 > bytes.len() {
            return Err(PersistentStorageError::Other(
                "cloud_reader: truncated record header".to_string(),
            ));
        }
        let off = u64::from_le_bytes(bytes[idx..idx + 8].try_into().unwrap());
        idx += 8;
        let len = u32::from_le_bytes(bytes[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;
        if idx + len > bytes.len() {
            return Err(PersistentStorageError::Other(
                "cloud_reader: truncated record body".to_string(),
            ));
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
