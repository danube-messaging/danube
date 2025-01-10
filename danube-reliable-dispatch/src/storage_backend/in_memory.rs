use async_trait::async_trait;
use danube_core::storage::Segment;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::storage_backend::StorageBackend;

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
    ) -> Result<Option<Arc<RwLock<Segment>>>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.segments.get(&id).map(|segment| segment.clone()))
    }

    async fn put_segment(
        &self,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.segments.insert(id, segment);
        Ok(())
    }

    async fn remove_segment(
        &self,
        id: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.segments.remove(&id);
        Ok(())
    }
}
