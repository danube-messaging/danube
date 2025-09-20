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
        Ok(out)
    }
}
