use crate::storage_metadata::{SegmentDescriptor, StorageMetadata};
use danube_core::storage::PersistentStorageError;

#[derive(Debug, Clone)]
pub struct SegmentCatalog {
    metadata: StorageMetadata,
}

impl SegmentCatalog {
    pub fn new(metadata: StorageMetadata) -> Self {
        Self { metadata }
    }

    pub async fn put_segment(
        &self,
        topic_path: &str,
        start_offset_padded: &str,
        descriptor: &SegmentDescriptor,
    ) -> Result<(), PersistentStorageError> {
        self.metadata
            .put_segment_descriptor(topic_path, start_offset_padded, descriptor)
            .await
    }

    pub async fn list_segments(
        &self,
        topic_path: &str,
    ) -> Result<Vec<SegmentDescriptor>, PersistentStorageError> {
        self.metadata.get_segment_descriptors(topic_path).await
    }

    pub async fn list_segments_range(
        &self,
        topic_path: &str,
        from_padded: &str,
        to_padded: Option<&str>,
    ) -> Result<Vec<SegmentDescriptor>, PersistentStorageError> {
        self.metadata
            .get_segment_descriptors_range(topic_path, from_padded, to_padded)
            .await
    }

    pub async fn set_current_segment(
        &self,
        topic_path: &str,
        start_offset_padded: &str,
    ) -> Result<(), PersistentStorageError> {
        self.metadata
            .put_current_segment(topic_path, start_offset_padded)
            .await
    }

    pub async fn delete_topic(&self, topic_path: &str) -> Result<(), PersistentStorageError> {
        self.metadata.delete_storage_topic(topic_path).await
    }

    pub fn metadata(&self) -> &StorageMetadata {
        &self.metadata
    }
}
