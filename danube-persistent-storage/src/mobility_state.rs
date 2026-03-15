use crate::storage_metadata::{StorageMetadata, StorageStateSealed};
use danube_core::storage::PersistentStorageError;

#[derive(Debug, Clone)]
pub struct MobilityState {
    metadata: StorageMetadata,
}

impl MobilityState {
    pub fn new(metadata: StorageMetadata) -> Self {
        Self { metadata }
    }

    pub async fn load(
        &self,
        topic_path: &str,
    ) -> Result<Option<StorageStateSealed>, PersistentStorageError> {
        self.metadata.get_storage_state_sealed(topic_path).await
    }

    pub async fn store(
        &self,
        topic_path: &str,
        state: &StorageStateSealed,
    ) -> Result<(), PersistentStorageError> {
        self.metadata.put_storage_state_sealed(topic_path, state).await
    }

    pub async fn clear(&self, topic_path: &str) -> Result<(), PersistentStorageError> {
        self.metadata.delete_storage_state_sealed(topic_path).await
    }

    pub fn metadata(&self) -> &StorageMetadata {
        &self.metadata
    }
}
