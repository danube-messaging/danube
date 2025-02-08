use async_trait::async_trait;
use bincode;
use danube_core::storage::{Segment, StorageBackend, StorageBackendError};
use std::{path::PathBuf, sync::Arc};
use tokio::{fs, sync::RwLock};

use crate::errors::PersistentStorageError;

// DiskStorage is a storage backend that stores segments on disk.
// This creates a directory structure like:
// base_path/
//     default/
//         some_topic/
//             segment_0.bin
//             segment_1.bin
//     other_namespace/
//         another_topic/
//             segment_0.bin

#[derive(Debug)]
pub struct DiskStorage {
    base_path: PathBuf,
}

// TODO!: This has a bug as the topic name is like /default/topic_name which is not an allowed directory structure !!!
impl DiskStorage {
    pub fn new(path: impl Into<String>) -> Self {
        let base_path = PathBuf::from(path.into());
        std::fs::create_dir_all(&base_path).expect("Failed to create storage directory");
        DiskStorage { base_path }
    }
    fn resolve_segment_path(&self, topic_name: &str, id: usize) -> Option<PathBuf> {
        // Add validation
        if topic_name.contains("//") {
            return None;
        }

        let topic_parts = topic_name.trim_start_matches('/').split('/');
        let mut full_path = self.base_path.clone();
        for part in topic_parts {
            full_path = full_path.join(part);
        }
        Some(full_path.join(format!("segment_{}.bin", id)))
    }

    async fn segment_path(
        &self,
        topic_name: &str,
        id: usize,
    ) -> Result<PathBuf, PersistentStorageError> {
        let path = self
            .resolve_segment_path(topic_name, id)
            .ok_or_else(|| PersistentStorageError::InvalidPath("Invalid topic name".to_string()))?;

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(PersistentStorageError::from)?;
        }

        Ok(path)
    }
}

#[async_trait]
impl StorageBackend for DiskStorage {
    async fn get_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> std::result::Result<Option<Arc<RwLock<Segment>>>, StorageBackendError> {
        let path = match self.resolve_segment_path(topic_name, id) {
            Some(path) => path,
            None => return Err(StorageBackendError::Disk("Invalid topic path".to_string())),
        };

        if !path.exists() {
            return Ok(None);
        }

        let bytes = fs::read(path).await.map_err(|e| {
            StorageBackendError::Disk(format!("Failed to read segment file: {}", e))
        })?;

        let segment: Segment = bincode::deserialize(&bytes).map_err(|e| {
            StorageBackendError::Disk(format!("Failed to deserialize segment: {}", e))
        })?;

        Ok(Some(Arc::new(RwLock::new(segment))))
    }

    async fn put_segment(
        &self,
        topic_name: &str,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> std::result::Result<(), StorageBackendError> {
        let path = self.segment_path(topic_name, id).await?;

        let segment_data = segment.read().await;
        let bytes = bincode::serialize(&*segment_data).map_err(PersistentStorageError::from)?;
        fs::write(path, bytes)
            .await
            .map_err(PersistentStorageError::from)?;
        Ok(())
    }

    async fn remove_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> std::result::Result<(), StorageBackendError> {
        let path = match self.resolve_segment_path(topic_name, id) {
            Some(path) => path,
            None => return Err(StorageBackendError::Disk("Invalid topic path".to_string())),
        };

        if path.exists() {
            fs::remove_file(path).await.map_err(|e| {
                StorageBackendError::Disk(format!("Failed to remove segment file: {}", e))
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use danube_core::message::{MessageID, StreamMessage};
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn create_test_message() -> StreamMessage {
        StreamMessage {
            request_id: 1,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test_topic".to_string(),
                broker_addr: "localhost:6650".to_string(),
                segment_id: 1,
                segment_offset: 0,
            },
            payload: vec![1, 2, 3],
            publish_time: 123456789,
            producer_name: "test_producer".to_string(),
            subscription_name: Some("test_subscription".to_string()),
            attributes: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_disk_storage() {
        let temp_dir = tempdir().unwrap();
        let storage = DiskStorage::new(temp_dir.path().to_str().unwrap());
        let topic_name = "/default/test_topic";
        let invalid_topic = "invalid//topic";

        // Test invalid topic name
        let result = storage.get_segment(invalid_topic, 1).await;
        assert!(matches!(result, Err(StorageBackendError::Disk(_))));

        // Test segment doesn't exist initially
        let segment = storage.get_segment(topic_name, 1).await.unwrap();
        assert!(segment.is_none());

        // Create and store a segment
        let test_message = create_test_message();
        let mut segment = Segment::new(1, 1024);
        segment.messages.push(test_message);
        let segment = Arc::new(RwLock::new(segment));

        storage
            .put_segment(topic_name, 1, segment.clone())
            .await
            .unwrap();

        // Verify segment exists and content matches
        let retrieved = storage.get_segment(topic_name, 1).await.unwrap().unwrap();
        assert_eq!(retrieved.read().await.id, 1);
        assert_eq!(retrieved.read().await.messages.len(), 1);

        // Remove segment
        storage.remove_segment(topic_name, 1).await.unwrap();

        // Verify segment is gone
        let segment = storage.get_segment(topic_name, 1).await.unwrap();
        assert!(segment.is_none());

        // Test removing non-existent segment
        let result = storage.remove_segment(topic_name, 999).await;
        assert!(result.is_ok());
    }
}
