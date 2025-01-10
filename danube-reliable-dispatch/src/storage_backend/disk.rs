use async_trait::async_trait;
use bincode;
use danube_core::storage::Segment;
use std::{path::PathBuf, sync::Arc};
use tokio::{fs, sync::RwLock};

use crate::{errors::Result, storage_backend::StorageBackend};

#[derive(Debug)]
pub struct DiskStorage {
    base_path: PathBuf,
}

impl DiskStorage {
    pub fn new(path: impl Into<String>) -> Self {
        let base_path = PathBuf::from(path.into());
        std::fs::create_dir_all(&base_path).expect("Failed to create storage directory");
        DiskStorage { base_path }
    }
    fn segment_path(&self, id: usize) -> PathBuf {
        self.base_path.join(format!("segment_{}.bin", id))
    }
    #[allow(dead_code)]
    async fn contains_segment(&self, id: usize) -> Result<bool> {
        let path = self.segment_path(id);
        Ok(path.exists())
    }
}

#[async_trait]
impl StorageBackend for DiskStorage {
    async fn get_segment(&self, id: usize) -> Result<Option<Arc<RwLock<Segment>>>> {
        let path = self.segment_path(id);

        if !path.exists() {
            return Ok(None);
        }

        let bytes = fs::read(path).await?;
        let segment: Segment = bincode::deserialize(&bytes)?;
        Ok(Some(Arc::new(RwLock::new(segment))))
    }

    async fn put_segment(&self, id: usize, segment: Arc<RwLock<Segment>>) -> Result<()> {
        let path = self.segment_path(id);
        let segment_data = segment.read().await;
        let bytes = bincode::serialize(&*segment_data)?;
        fs::write(path, bytes).await?;
        Ok(())
    }

    async fn remove_segment(&self, id: usize) -> Result<()> {
        let path = self.segment_path(id);
        if path.exists() {
            fs::remove_file(path).await?;
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

        // Test segment doesn't exist initially
        assert!(!storage.contains_segment(1).await.unwrap());

        // Create and store a segment
        let segment = Arc::new(RwLock::new(Segment::new(1, 1)));
        storage.put_segment(1, segment.clone()).await.unwrap();

        // Verify segment exists
        assert!(storage.contains_segment(1).await.unwrap());

        // Retrieve and verify segment
        let retrieved = storage.get_segment(1).await.unwrap().unwrap();
        assert_eq!(retrieved.read().await.id, 1);

        // Remove segment
        storage.remove_segment(1).await.unwrap();
        assert!(!storage.contains_segment(1).await.unwrap());
    }
}
