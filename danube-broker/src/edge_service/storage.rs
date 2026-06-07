use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;
use tracing::debug;

use danube_core::message::StreamMessage;
use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_core::storage::PersistentStorage;
use danube_persistent_storage::StorageFactory;

use crate::metadata_storage::MetadataStorage;

/// Raft key prefix for tracking the last replicated edge offset per topic.
const EDGE_REPLICATED_PREFIX: &str = "/edge/replicated";

/// Broker-side storage for edge replication.
///
/// Caches per-topic storage handles to avoid repeated `StorageFactory`
/// lookups on every batch (same pattern as the normal producer's `TopicStore`).
pub(crate) struct EdgeReplicationStorage {
    storage_factory: StorageFactory,
    meta_store: MetadataStorage,
    /// Cached storage handles per topic. Resolved once, reused for all batches.
    storage_cache: Mutex<HashMap<String, Arc<dyn PersistentStorage>>>,
}

impl EdgeReplicationStorage {
    pub(crate) fn new(storage_factory: StorageFactory, meta_store: MetadataStorage) -> Self {
        Self {
            storage_factory,
            meta_store,
            storage_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create a cached storage handle for a topic.
    async fn get_storage(&self, topic_name: &str) -> Result<Arc<dyn PersistentStorage>> {
        {
            let cache = self.storage_cache.lock().await;
            if let Some(storage) = cache.get(topic_name) {
                return Ok(storage.clone());
            }
        }

        let storage = self
            .storage_factory
            .for_topic(topic_name)
            .await
            .map_err(|e| anyhow::anyhow!("failed to get storage: {}", e))?;

        let mut cache = self.storage_cache.lock().await;
        cache.insert(topic_name.to_string(), storage.clone());
        Ok(storage)
    }

    /// Write a batch of messages directly to the topic's WAL.
    ///
    /// Uses `edge_batch_offset` as an idempotency key to prevent duplicate writes.
    /// Returns the last WAL offset written on the cluster side.
    pub(crate) async fn ingest_batch(
        &self,
        topic_name: &str,
        edge_batch_offset: u64,
        messages: Vec<StreamMessage>,
    ) -> Result<u64> {
        // --- Deduplication check ---
        let replicated_key = Self::replicated_key(topic_name);
        if let Ok(Some(val)) = self.meta_store.get(&replicated_key, MetaOptions::None).await {
            if let Some(last_ingested) = val.as_u64() {
                if edge_batch_offset <= last_ingested {
                    debug!(
                        topic = %topic_name,
                        edge_batch_offset,
                        last_replicated = last_ingested,
                        "batch already replicated, skipping WAL write"
                    );
                    return Ok(last_ingested);
                }
            }
        }

        // --- Write batch to WAL (cached handle) ---
        let storage = self.get_storage(topic_name).await?;
        let count = messages.len();

        let (_first, last) = storage
            .append_batch(topic_name, &messages)
            .await
            .map_err(|e| anyhow::anyhow!("batch append failed: {}", e))?;

        // --- Update dedup marker ---
        if let Err(e) = self
            .meta_store
            .put(
                &replicated_key,
                serde_json::Value::from(edge_batch_offset),
                MetaOptions::None,
            )
            .await
        {
            debug!(
                topic = %topic_name,
                error = %e,
                "failed to update replicated offset marker (non-fatal)"
            );
        }

        debug!(
            topic = %topic_name,
            count,
            last_offset = last,
            edge_batch_offset,
            "ingested replicated batch into WAL"
        );

        Ok(last)
    }

    /// Remove cached WAL handle and dedup marker for a deleted topic.
    #[allow(dead_code)]
    pub(crate) async fn delete_replicated_marker(&self, topic_name: &str) {
        // Remove cached storage handle
        {
            let mut cache = self.storage_cache.lock().await;
            cache.remove(topic_name);
        }

        let replicated_key = Self::replicated_key(topic_name);
        if let Err(e) = self.meta_store.delete(&replicated_key).await {
            debug!(
                topic = %topic_name,
                error = %e,
                "failed to delete replicated offset marker (non-fatal)"
            );
        }
    }

    /// Build the Raft key for a topic's replicated offset marker.
    fn replicated_key(topic_name: &str) -> String {
        format!("{}{}", EDGE_REPLICATED_PREFIX, topic_name)
    }
}
