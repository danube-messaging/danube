use async_trait::async_trait;
use danube_core::storage::{Segment, StorageBackend, StorageBackendError, StorageConfig};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub fn create_message_storage(storage_config: &StorageConfig) -> Arc<dyn StorageBackend> {
    match storage_config {
        StorageConfig::InMemory => Arc::new(InMemoryStorage::new()),
        StorageConfig::Disk(_disk_config) => {
            todo!()
        }
        StorageConfig::S3(_s3_config) => {
            todo!()
        }
    }
}

#[derive(Debug)]
pub struct InMemoryStorage {
    segments: DashMap<usize, Arc<RwLock<Segment>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            segments: DashMap::new(),
        }
    }
}

#[async_trait]
impl StorageBackend for InMemoryStorage {
    async fn get_segment(
        &self,
        id: usize,
    ) -> Result<Option<Arc<RwLock<Segment>>>, StorageBackendError> {
        self.segments
            .get(&id)
            .map(|segment| Some(segment.clone()))
            .ok_or_else(|| StorageBackendError::Memory(format!("Segment {} not found", id)))
    }

    async fn put_segment(
        &self,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> Result<(), StorageBackendError> {
        self.segments.insert(id, segment);
        Ok(())
    }

    async fn remove_segment(&self, id: usize) -> Result<(), StorageBackendError> {
        match self.segments.remove(&id) {
            Some(_) => Ok(()),
            None => Err(StorageBackendError::Memory(format!(
                "Cannot remove segment {}: not found",
                id
            ))),
        }
    }
}
