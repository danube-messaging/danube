use danube_core::storage::{Segment, StorageBackend};
use moka::future::Cache as MokaCache;
use std::sync::Arc;
use tokio::{sync::RwLock, time::Duration};

use crate::errors::{ReliableDispatchError, Result};

#[derive(Debug, Clone)]
pub struct TopicCache {
    // Primary fast memory cache
    memory_cache: MokaCache<String, Arc<RwLock<Segment>>>,
    // Storage backend for segments
    storage: Arc<dyn StorageBackend>,
}

impl TopicCache {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        let memory_cache = MokaCache::builder()
            // Max 100 segment entries
            .max_capacity(100)
            // Time to idle (TTI):  10 minutes
            // A cached entry will be expired after the specified duration past from get or insert.
            .time_to_idle(Duration::from_secs(10 * 60))
            // Create the cache.
            .build();

        Self {
            memory_cache,
            storage,
        }
    }

    pub async fn get_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> Result<Option<Arc<RwLock<Segment>>>> {
        let key = format!("{}:{}", topic_name, id);

        // Try memory cache first
        if let Some(segment) = self.memory_cache.get(&key).await {
            return Ok(Some(segment));
        }

        // Try storage backend
        match self.storage.get_segment(topic_name, id).await {
            Ok(segment) => {
                // Update memory cache
                if let Some(ref segment) = segment {
                    self.memory_cache.insert(key, segment.clone()).await;
                }
                Ok(segment)
            }
            Err(e) => Err(ReliableDispatchError::StorageError(e.to_string())),
        }
    }

    pub async fn put_segment(
        &self,
        topic_name: &str,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> Result<()> {
        let key = format!("{}:{}", topic_name, id);

        // Update memory cache
        self.memory_cache.insert(key, segment.clone()).await;

        // Update storage backend
        self.storage.put_segment(topic_name, id, segment).await?;
        Ok(())
    }

    pub async fn remove_segment(&self, topic_name: &str, id: usize) -> Result<()> {
        let key = format!("{}:{}", topic_name, id);

        // Remove from memory cache
        self.memory_cache.remove(&key).await;

        // Remove from storage backend
        self.storage.remove_segment(topic_name, id).await?;
        Ok(())
    }
}
