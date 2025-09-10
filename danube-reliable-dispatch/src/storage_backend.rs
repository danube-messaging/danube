use crate::errors::{ReliableDispatchError, Result};
use async_trait::async_trait;
use danube_core::message::StreamMessage;
use danube_core::storage::{
    PersistentStorage, PersistentStorageError, Segment, StorageBackend, StorageBackendError,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

/// In-memory storage backend for testing and development
#[derive(Debug)]
pub struct InMemoryStorage {
    segments: Arc<Mutex<HashMap<String, HashMap<usize, Arc<RwLock<Segment>>>>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            segments: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl StorageBackend for InMemoryStorage {
    async fn get_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> std::result::Result<Option<Arc<RwLock<Segment>>>, StorageBackendError> {
        let segments = self.segments.lock().await;
        if let Some(topic_segments) = segments.get(topic_name) {
            Ok(topic_segments.get(&id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn put_segment(
        &self,
        topic_name: &str,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> std::result::Result<(), StorageBackendError> {
        let mut segments = self.segments.lock().await;
        let topic_segments = segments
            .entry(topic_name.to_string())
            .or_insert_with(HashMap::new);
        topic_segments.insert(id, segment);
        Ok(())
    }

    async fn remove_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> std::result::Result<(), StorageBackendError> {
        let mut segments = self.segments.lock().await;
        if let Some(topic_segments) = segments.get_mut(topic_name) {
            topic_segments.remove(&id);
        }
        Ok(())
    }
}

/// Adapter that bridges between PersistentStorage and StorageBackend traits
/// This allows gradual migration from segment-based to streaming storage
#[derive(Debug)]
pub struct PersistentStorageAdapter {
    persistent_storage: Arc<dyn PersistentStorage>,
    // Legacy segment cache for compatibility
    segment_cache: Arc<Mutex<HashMap<String, HashMap<usize, Arc<RwLock<Segment>>>>>>,
    // Current segment ID per topic
    current_segment_ids: Arc<Mutex<HashMap<String, usize>>>,
}

impl PersistentStorageAdapter {
    pub fn new(persistent_storage: Arc<dyn PersistentStorage>) -> Self {
        Self {
            persistent_storage,
            segment_cache: Arc::new(Mutex::new(HashMap::new())),
            current_segment_ids: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Convert messages to a segment for legacy compatibility
    async fn messages_to_segment(
        &self,
        topic_name: &str,
        messages: Vec<StreamMessage>,
    ) -> Arc<RwLock<Segment>> {
        let mut segment_ids = self.current_segment_ids.lock().await;
        let segment_id = segment_ids.entry(topic_name.to_string()).or_insert(0);
        *segment_id += 1;

        let mut segment = Segment::new(*segment_id, messages.len());
        for message in messages {
            segment.add_message(message);
        }

        Arc::new(RwLock::new(segment))
    }
}

#[async_trait]
impl StorageBackend for PersistentStorageAdapter {
    async fn get_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> std::result::Result<Option<Arc<RwLock<Segment>>>, StorageBackendError> {
        let cache = self.segment_cache.lock().await;
        if let Some(topic_segments) = cache.get(topic_name) {
            Ok(topic_segments.get(&id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn put_segment(
        &self,
        topic_name: &str,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> std::result::Result<(), StorageBackendError> {
        // Store segment in cache for legacy access
        {
            let mut cache = self.segment_cache.lock().await;
            let topic_segments = cache
                .entry(topic_name.to_string())
                .or_insert_with(HashMap::new);
            topic_segments.insert(id, segment.clone());
        }

        // Extract messages from segment and store via PersistentStorage
        let segment_guard = segment.read().await;
        let messages = segment_guard.messages.clone();
        drop(segment_guard);

        if !messages.is_empty() {
            self.persistent_storage
                .store_messages(topic_name, messages)
                .await
                .map_err(|e| StorageBackendError::Managed(e.to_string()))?;
        }

        Ok(())
    }

    async fn remove_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> std::result::Result<(), StorageBackendError> {
        let mut cache = self.segment_cache.lock().await;
        if let Some(topic_segments) = cache.get_mut(topic_name) {
            topic_segments.remove(&id);
        }
        Ok(())
    }
}

/// Factory function to create storage backends based on configuration
pub fn create_message_storage(storage_type: &str) -> Result<Box<dyn StorageBackend>> {
    match storage_type {
        "inmemory" => Ok(Box::new(InMemoryStorage::new())),
        _ => Err(ReliableDispatchError::StorageError(format!(
            "Unsupported storage type: {}",
            storage_type
        ))),
    }
}

/// Factory function to create PersistentStorage adapter
pub fn create_persistent_storage_adapter(
    persistent_storage: Arc<dyn PersistentStorage>,
) -> Box<dyn StorageBackend> {
    Box::new(PersistentStorageAdapter::new(persistent_storage))
}
