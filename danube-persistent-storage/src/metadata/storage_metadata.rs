use std::sync::Arc;

use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_core::storage::PersistentStorageError;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct StorageMetadata {
    store: Arc<dyn MetadataStore>,
    root: String,
}

impl std::fmt::Debug for StorageMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageMetadata")
            .field("root", &self.root)
            .finish()
    }
}

fn default_ref_count() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentDescriptor {
    pub segment_id: String,
    pub start_offset: u64,
    pub end_offset: u64,
    pub size: u64,
    pub etag: Option<String>,
    pub created_at: u64,
    pub completed: bool,
    /// Optional sparse index mapping message offsets to byte positions within the object.
    /// Each entry is (offset, byte_pos).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_index: Option<Vec<(u64, u64)>>,
    /// Number of topics referencing this underlying segment object (>= 1).
    #[serde(default = "default_ref_count")]
    pub ref_count: u32,
    /// Originating topic path where the physical object is stored when branched (None = current topic).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_topic: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStateSealed {
    pub sealed: bool,
    /// Highest offset already accepted by the local WAL at the time of seal.
    ///
    /// This is used to resume from the next offset on takeover when sealed
    /// continuity is authoritative.
    pub last_committed_offset: u64,
    pub broker_id: u64,
    pub timestamp: u64,
}

impl StorageMetadata {
    pub fn new(store: Arc<dyn MetadataStore>, root: String) -> Self {
        Self { store, root }
    }

    pub async fn put_segment_descriptor(
        &self,
        topic_path: &str,
        start_offset_padded: &str,
        desc: &SegmentDescriptor,
    ) -> Result<(), PersistentStorageError> {
        let key = format!(
            "{}/storage/topics/{}/segments/{}",
            self.root, topic_path, start_offset_padded
        );
        let value =
            serde_json::to_value(desc).map_err(|e| PersistentStorageError::Other(e.to_string()))?;
        self.store
            .put(&key, value, MetaOptions::None)
            .await
            .map_err(|e| PersistentStorageError::Metadata(e.to_string()))?;
        Ok(())
    }

    /// Optional pointer to the current rolling object key (stores the start_offset_padded).
    pub async fn put_current_segment(
        &self,
        topic_path: &str,
        start_offset_padded: &str,
    ) -> Result<(), PersistentStorageError> {
        let key = format!("{}/storage/topics/{}/segments/cur", self.root, topic_path);
        let val = serde_json::json!({ "start": start_offset_padded });
        self.store
            .put(&key, val, MetaOptions::None)
            .await
            .map_err(|e| PersistentStorageError::Metadata(e.to_string()))?;
        Ok(())
    }

    pub async fn get_segment_descriptor(
        &self,
        topic_path: &str,
        start_offset_padded: &str,
    ) -> Result<Option<SegmentDescriptor>, PersistentStorageError> {
        let key = format!(
            "{}/storage/topics/{}/segments/{}",
            self.root, topic_path, start_offset_padded
        );
        match self.store.get(&key, MetaOptions::None).await {
            Ok(Some(value)) => {
                let desc = serde_json::from_value::<SegmentDescriptor>(value)
                    .map_err(|e| PersistentStorageError::Other(e.to_string()))?;
                Ok(Some(desc))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(PersistentStorageError::Metadata(e.to_string())),
        }
    }

    /// Resolve the current durable segment through the `cur` pointer indirection.
    ///
    /// The `segments/cur` key stores only the padded start offset of the latest published
    /// segment. This helper first reads that pointer and then loads the full descriptor from the
    /// corresponding `segments/<start_offset_padded>` entry.
    pub async fn get_current_segment_descriptor(
        &self,
        topic_path: &str,
    ) -> Result<Option<SegmentDescriptor>, PersistentStorageError> {
        let key = format!("{}/storage/topics/{}/segments/cur", self.root, topic_path);
        let start_offset_padded = match self.store.get(&key, MetaOptions::None).await {
            Ok(Some(value)) => value
                .get("start")
                .and_then(|start| start.as_str())
                .map(|start| start.to_string()),
            Ok(None) => None,
            Err(e) => return Err(PersistentStorageError::Metadata(e.to_string())),
        };
        match start_offset_padded {
            Some(start_offset_padded) => {
                self.get_segment_descriptor(topic_path, &start_offset_padded)
                    .await
            }
            None => Ok(None),
        }
    }

    /// List all durable segment descriptors stored for a topic.
    ///
    /// This performs a prefix scan over the topic's segment metadata and returns descriptors sorted
    /// by `start_offset`, giving callers a deterministic logical history order regardless of backend
    /// enumeration order.
    pub async fn get_segment_descriptors(
        &self,
        topic_path: &str,
    ) -> Result<Vec<SegmentDescriptor>, PersistentStorageError> {
        let prefix = format!("{}/storage/topics/{}/segments/", self.root, topic_path);
        let kvs = self
            .store
            .get_bulk(&prefix)
            .await
            .map_err(|e| PersistentStorageError::Metadata(e.to_string()))?;
        let mut out = Vec::new();
        for kv in kvs {
            if let Ok(desc) = serde_json::from_slice::<SegmentDescriptor>(&kv.value) {
                out.push(desc)
            }
        }
        // Sort by start_offset to ensure deterministic order
        out.sort_by_key(|d| d.start_offset);
        Ok(out)
    }

    /// Fetch descriptors starting at or after `from_padded` (inclusive). If `to_padded` is Some,
    /// it will stop at keys <= `to_padded`.
    ///
    /// Range matching is done lexicographically on zero-padded start-offset keys, which preserves
    /// numeric ordering while still using simple string comparison against backend keys.
    /// The `cur` pointer entry is explicitly skipped because it is metadata indirection, not a
    /// descriptor payload.
    pub async fn get_segment_descriptors_range(
        &self,
        topic_path: &str,
        from_padded: &str,
        to_padded: Option<&str>,
    ) -> Result<Vec<SegmentDescriptor>, PersistentStorageError> {
        let prefix = format!("{}/storage/topics/{}/segments/", self.root, topic_path);
        let kvs = self
            .store
            .get_bulk(&prefix)
            .await
            .map_err(|e| PersistentStorageError::Metadata(e.to_string()))?;
        let mut out = Vec::new();
        for kv in kvs {
            let key_str = kv.key.clone();
            // Expect keys like .../objects/<start_offset_padded>
            if let Some(idx) = key_str.rsplit('/').next() {
                if idx == "cur" {
                    continue;
                }
                if idx >= from_padded && to_padded.map(|t| idx <= t).unwrap_or(true) {
                    if let Ok(desc) = serde_json::from_slice::<SegmentDescriptor>(&kv.value) {
                        out.push(desc);
                    }
                }
            }
        }
        out.sort_by_key(|d| d.start_offset);
        Ok(out)
    }

    /// Write sealed state marker under `/storage/topics/<ns>/<topic>/state`.
    pub async fn put_storage_state_sealed(
        &self,
        topic_path: &str,
        state: &StorageStateSealed,
    ) -> Result<(), PersistentStorageError> {
        let key = format!("{}/storage/topics/{}/state", self.root, topic_path);
        let value = serde_json::to_value(state)
            .map_err(|e| PersistentStorageError::Other(e.to_string()))?;
        self.store
            .put(&key, value, MetaOptions::None)
            .await
            .map_err(|e| PersistentStorageError::Metadata(e.to_string()))?;
        Ok(())
    }

    /// Get sealed state marker from `/storage/topics/<ns>/<topic>/state`.
    /// Returns None if the state does not exist.
    pub async fn get_storage_state_sealed(
        &self,
        topic_path: &str,
    ) -> Result<Option<StorageStateSealed>, PersistentStorageError> {
        let key = format!("{}/storage/topics/{}/state", self.root, topic_path);
        match self.store.get(&key, MetaOptions::None).await {
            Ok(Some(value)) => {
                let state = serde_json::from_value::<StorageStateSealed>(value)
                    .map_err(|e| PersistentStorageError::Other(e.to_string()))?;
                Ok(Some(state))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(PersistentStorageError::Metadata(e.to_string())),
        }
    }

    /// Delete sealed state marker from `/storage/topics/<ns>/<topic>/state`.
    /// Used after successfully loading a topic from sealed state to prevent re-use.
    pub async fn delete_storage_state_sealed(
        &self,
        topic_path: &str,
    ) -> Result<(), PersistentStorageError> {
        let key = format!("{}/storage/topics/{}/state", self.root, topic_path);
        self.store
            .delete(&key)
            .await
            .map_err(|e| PersistentStorageError::Metadata(e.to_string()))?;
        Ok(())
    }

    /// Delete all storage metadata for a topic under `/storage/topics/<topic_path>/`.
    /// This includes object descriptors, current pointer, and sealed state.
    ///
    /// This is a prefix-based metadata sweep. It removes every metadata key for the topic rather
    /// than trying to delete descriptor, pointer, and state entries individually.
    pub async fn delete_storage_topic(
        &self,
        topic_path: &str,
    ) -> Result<(), PersistentStorageError> {
        let prefix = format!("{}/storage/topics/{}/", self.root, topic_path);
        let kvs = self
            .store
            .get_bulk(&prefix)
            .await
            .map_err(|e| PersistentStorageError::Metadata(e.to_string()))?;
        for kv in kvs {
            let _ = self
                .store
                .delete(&kv.key)
                .await
                .map_err(|e| PersistentStorageError::Metadata(e.to_string()))?;
        }
        Ok(())
    }

    /// Zero-copy branch sealed segment descriptors from `source_topic` to `target_topic`
    /// up to optional `max_offset` (inclusive on `start_offset`), incrementing `ref_count`.
    pub async fn branch_topic_segments(
        &self,
        source_topic: &str,
        target_topic: &str,
        max_offset: Option<u64>,
    ) -> Result<usize, PersistentStorageError> {
        let descriptors = self.get_segment_descriptors(source_topic).await?;
        let mut copied = 0usize;
        let mut last_padded: Option<String> = None;

        for mut desc in descriptors {
            if let Some(limit) = max_offset {
                if desc.start_offset > limit {
                    continue;
                }
            }
            let next_rc = desc.ref_count.max(1).saturating_add(1);
            desc.ref_count = next_rc;
            let start_padded = format!("{:020}", desc.start_offset);

            // Persist incremented ref_count on source topic descriptor
            self.put_segment_descriptor(source_topic, &start_padded, &desc)
                .await?;

            // Write branched descriptor under target_topic pointing to the physical object topic
            let mut branched = desc.clone();
            if branched.object_topic.is_none() {
                branched.object_topic = Some(source_topic.to_string());
            }
            self.put_segment_descriptor(target_topic, &start_padded, &branched)
                .await?;

            last_padded = Some(start_padded);
            copied += 1;
        }

        if let Some(cur) = last_padded {
            self.put_current_segment(target_topic, &cur).await?;
        }

        Ok(copied)
    }
}
