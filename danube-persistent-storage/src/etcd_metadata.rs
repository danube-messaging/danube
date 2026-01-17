use danube_core::storage::PersistentStorageError;
use danube_metadata_store::{MetaOptions, MetadataStorage, MetadataStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct EtcdMetadata {
    store: MetadataStorage,
    root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectDescriptor {
    pub object_id: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStateSealed {
    pub sealed: bool,
    pub last_committed_offset: u64,
    pub broker_id: u64,
    pub timestamp: u64,
}

impl EtcdMetadata {
    pub fn new(store: MetadataStorage, root: String) -> Self {
        Self { store, root }
    }

    pub async fn put_object_descriptor(
        &self,
        topic_path: &str,
        start_offset_padded: &str,
        desc: &ObjectDescriptor,
    ) -> Result<(), PersistentStorageError> {
        let key = format!(
            "{}/storage/topics/{}/objects/{}",
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
    pub async fn put_current_pointer(
        &self,
        topic_path: &str,
        start_offset_padded: &str,
    ) -> Result<(), PersistentStorageError> {
        let key = format!("{}/storage/topics/{}/objects/cur", self.root, topic_path);
        let val = serde_json::json!({ "start": start_offset_padded });
        self.store
            .put(&key, val, MetaOptions::None)
            .await
            .map_err(|e| PersistentStorageError::Metadata(e.to_string()))?;
        Ok(())
    }

    pub async fn get_object_descriptors(
        &self,
        topic_path: &str,
    ) -> Result<Vec<ObjectDescriptor>, PersistentStorageError> {
        let prefix = format!("{}/storage/topics/{}/objects/", self.root, topic_path);
        let kvs = self
            .store
            .get_bulk(&prefix)
            .await
            .map_err(|e| PersistentStorageError::Metadata(e.to_string()))?;
        let mut out = Vec::new();
        for kv in kvs {
            if let Ok(desc) = serde_json::from_slice::<ObjectDescriptor>(&kv.value) {
                out.push(desc)
            }
        }
        // Sort by start_offset to ensure deterministic order
        out.sort_by_key(|d| d.start_offset);
        Ok(out)
    }

    /// Fetch descriptors starting at or after `from_padded` (inclusive). If `to_padded` is Some,
    /// it will stop at keys <= `to_padded`.
    pub async fn get_object_descriptors_range(
        &self,
        topic_path: &str,
        from_padded: &str,
        to_padded: Option<&str>,
    ) -> Result<Vec<ObjectDescriptor>, PersistentStorageError> {
        let prefix = format!("{}/storage/topics/{}/objects/", self.root, topic_path);
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
                    if let Ok(desc) = serde_json::from_slice::<ObjectDescriptor>(&kv.value) {
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
}
