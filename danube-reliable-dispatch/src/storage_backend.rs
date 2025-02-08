use async_trait::async_trait;
use danube_core::storage::{Segment, StorageBackend, StorageBackendError, StorageConfig};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use danube_persistent_storage::DiskStorage;

pub fn create_message_storage(storage_config: &StorageConfig) -> Arc<dyn StorageBackend> {
    match storage_config {
        StorageConfig::InMemory => Arc::new(InMemoryStorage::new()),
        StorageConfig::Disk(disk_config) => Arc::new(DiskStorage::new(&disk_config.path)),
        StorageConfig::Managed(managed_config) => Arc::new(ManagedStorage::new(managed_config)),
    }
}

#[derive(Debug)]
pub struct InMemoryStorage {
    // topic_name -> (segment_id -> segment)
    segments: DashMap<String, DashMap<usize, Arc<RwLock<Segment>>>>,
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
        topic_name: &str,
        id: usize,
    ) -> Result<Option<Arc<RwLock<Segment>>>, StorageBackendError> {
        Ok(self
            .segments
            .get(topic_name)
            .and_then(|topic_segments| topic_segments.value().get(&id).map(|seg| seg.clone())))
    }

    async fn put_segment(
        &self,
        topic_name: &str,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> Result<(), StorageBackendError> {
        let topic_segments = self
            .segments
            .entry(topic_name.to_string())
            .or_insert_with(|| DashMap::new());
        topic_segments.insert(id, segment);
        Ok(())
    }

    async fn remove_segment(&self, topic_name: &str, id: usize) -> Result<(), StorageBackendError> {
        if let Some(topic_segments) = self.segments.get(topic_name) {
            topic_segments.remove(&id);
        }
        Ok(())
    }
}
