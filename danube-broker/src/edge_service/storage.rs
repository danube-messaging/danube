//! Edge replication storage — WAL ingestion and dedup tracking.
//!
//! Uses the broker's `StorageFactory` for WAL access and `MetadataStorage`
//! for dedup markers. Topic metadata management (create/delete) is handled
//! by `TopicCluster` — the same code path used for Standalone/Cluster modes.

use anyhow::Result;
use tracing::debug;

use danube_core::message::StreamMessage;
use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_persistent_storage::StorageFactory;

use crate::metadata_storage::MetadataStorage;

/// Raft key prefix for tracking the last replicated edge offset per topic.
const EDGE_REPLICATED_PREFIX: &str = "/edge/replicated";

/// Broker-side storage for edge replication.
///
/// Provides two key capabilities:
/// 1. **WAL batch ingestion** — writes directly to topic WALs via `append_batch()`
/// 2. **Idempotency** — tracks last replicated offset per topic in Raft
///
/// Topic metadata (create/delete) is **not** handled here — it's delegated to
/// `TopicCluster` to avoid duplicating the existing metadata management logic.
pub(crate) struct EdgeReplicationStorage {
    storage_factory: StorageFactory,
    meta_store: MetadataStorage,
}

impl EdgeReplicationStorage {
    pub(crate) fn new(storage_factory: StorageFactory, meta_store: MetadataStorage) -> Self {
        Self {
            storage_factory,
            meta_store,
        }
    }

    /// Write a batch of messages directly to the topic's WAL.
    ///
    /// Uses `edge_batch_offset` (the edge-side WAL offset) as an idempotency key
    /// to prevent duplicate writes on retry. If a batch with an offset ≤ the last
    /// ingested offset arrives, it is silently acked without writing to WAL.
    ///
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

        // --- Write batch to WAL ---
        let wal_storage = self
            .storage_factory
            .for_topic(topic_name)
            .await
            .map_err(|e| anyhow::anyhow!("failed to get WAL storage: {}", e))?;

        let count = messages.len();

        let (_first, last) = wal_storage
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
            // Non-fatal: if the marker fails to persist, the worst case is
            // a duplicate write on retry (at-least-once, not exactly-once)
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

    /// Delete the replicated offset marker for a topic.
    ///
    /// Called during edge topic deletion to clean up the dedup state.
    pub(crate) async fn delete_replicated_marker(&self, topic_name: &str) {
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
    ///
    /// E.g., topic `/edge1/sensors` → key `/edge/replicated/edge1/sensors`
    fn replicated_key(topic_name: &str) -> String {
        format!("{}{}", EDGE_REPLICATED_PREFIX, topic_name)
    }
}
