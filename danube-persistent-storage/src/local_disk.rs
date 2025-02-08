use async_trait::async_trait;
use bincode;
use danube_core::storage::{Segment, StorageBackend, StorageBackendError};
use std::{path::PathBuf, sync::Arc};
use tokio::{fs, sync::RwLock};

use crate::errors::PersistentStorageError;

// DiskStorage is a storage backend that stores segments on disk.
// This creates a directory structure like:
// base_path/
//     topic1/
//         segment_0.bin
//         segment_1.bin
//     topic2/
//         segment_0.bin
//         segment_1.bin

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
    fn segment_path(&self, topic_name: &str, id: usize) -> PathBuf {
        let topic_dir = self.base_path.join(topic_name);
        std::fs::create_dir_all(&topic_dir).expect("Failed to create topic directory");
        topic_dir.join(format!("segment_{}.bin", id))
    }
    #[allow(dead_code)]
    async fn contains_segment(&self, topic_name: &str, id: usize) -> bool {
        let path = self.segment_path(topic_name, id);
        path.exists()
    }
}

#[async_trait]
impl StorageBackend for DiskStorage {
    async fn get_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> std::result::Result<Option<Arc<RwLock<Segment>>>, StorageBackendError> {
        let path = self.segment_path(topic_name, id);

        if !path.exists() {
            return Ok(None);
        }

        let bytes = fs::read(path).await.map_err(PersistentStorageError::from)?;
        let segment: Segment =
            bincode::deserialize(&bytes).map_err(PersistentStorageError::from)?;
        Ok(Some(Arc::new(RwLock::new(segment))))
    }

    async fn put_segment(
        &self,
        topic_name: &str,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> std::result::Result<(), StorageBackendError> {
        let path = self.segment_path(topic_name, id);
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
        let path = self.segment_path(topic_name, id);
        if path.exists() {
            fs::remove_file(path)
                .await
                .map_err(PersistentStorageError::from)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_disk_storage() {
        let temp_dir = tempdir().unwrap();
        let storage = DiskStorage::new(temp_dir.path().to_str().unwrap());
        let topic_name = "test_topic";

        // Test segment doesn't exist initially
        let segment = storage.get_segment(topic_name, 1).await.unwrap();
        assert!(segment.is_none());

        // Create and store a segment
        let segment = Arc::new(RwLock::new(Segment::new(1, 1)));
        storage
            .put_segment(topic_name, 1, segment.clone())
            .await
            .unwrap();

        // Verify segment exists
        let retrieved = storage.get_segment(topic_name, 1).await.unwrap().unwrap();
        assert_eq!(retrieved.read().await.id, 1);

        // Remove segment
        storage.remove_segment(topic_name, 1).await.unwrap();
        let segment = storage.get_segment(topic_name, 1).await.unwrap();
        assert!(segment.is_none());
    }
}
