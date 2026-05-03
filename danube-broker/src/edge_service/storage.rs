//! Edge replication storage — WAL ingestion and metadata management.
//!
//! Uses the broker's `StorageFactory` for WAL access, `Resources`
//! for metadata management, and `MetadataStorage` for dedup markers.

use anyhow::Result;
use tracing::{debug, info};

use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_core::message::StreamMessage;
use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_persistent_storage::StorageFactory;

use crate::metadata_storage::MetadataStorage;
use crate::policies::Policies;
use crate::resources::Resources;

/// Raft key prefix for tracking the last replicated edge offset per topic.
const EDGE_REPLICATED_PREFIX: &str = "/edge/replicated";

/// Broker-side storage for edge replication.
///
/// Provides three key capabilities:
/// 1. **Metadata management** — ensures namespaces/topics exist in Raft
/// 2. **WAL batch ingestion** — writes directly to topic WALs via `append_batch()`
/// 3. **Idempotency** — tracks last replicated offset per topic in Raft
pub(crate) struct EdgeReplicationStorage {
    resources: Resources,
    storage_factory: StorageFactory,
    meta_store: MetadataStorage,
}

impl EdgeReplicationStorage {
    pub(crate) fn new(
        resources: Resources,
        storage_factory: StorageFactory,
        meta_store: MetadataStorage,
    ) -> Self {
        Self {
            resources,
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

    /// Ensure topic metadata and WAL exist on the cluster.
    ///
    /// Creates:
    /// - Namespace (extracted from topic path)
    /// - Topic entry with Reliable dispatch strategy
    /// - WAL storage via StorageFactory
    ///
    /// Idempotent: returns Ok if the topic already exists.
    pub(crate) async fn ensure_topic(&self, topic_name: &str) -> Result<()> {
        // Extract namespace from topic name (e.g., "/edge1/sensor" → "edge1")
        let parts: Vec<&str> = topic_name.split('/').collect();
        if parts.len() < 3 {
            return Err(anyhow::anyhow!(
                "invalid topic format: '{}', expected /namespace/topic",
                topic_name
            ));
        }
        let ns_name = parts[1];

        // Ensure namespace exists
        if !self.resources.namespace.namespace_exist(ns_name).await? {
            self.resources
                .namespace
                .create_namespace(ns_name, Some(&Policies::new()))
                .await?;
            info!(
                namespace = %ns_name,
                "edge namespace created on cluster"
            );
        }

        // Check if topic already exists (idempotent)
        if self
            .resources
            .namespace
            .check_if_topic_exist(ns_name, topic_name)
            .await
        {
            debug!(
                topic = %topic_name,
                "edge topic already exists, skipping creation"
            );
            return Ok(());
        }

        // Create topic metadata in Raft
        self.resources
            .namespace
            .create_new_topic(topic_name)
            .await?;

        // Add delivery strategy (always Reliable for edge topics)
        self.resources
            .topic
            .add_topic_delivery(topic_name, ConfigDispatchStrategy::Reliable)
            .await?;

        // Ensure WAL exists
        let _wal = self
            .storage_factory
            .for_topic(topic_name)
            .await
            .map_err(|e| anyhow::anyhow!("failed to create WAL storage: {}", e))?;

        info!(
            topic = %topic_name,
            "edge topic created on cluster (metadata + WAL)"
        );
        Ok(())
    }

    /// Build the Raft key for a topic's replicated offset marker.
    ///
    /// E.g., topic `/edge1/sensors` → key `/edge/replicated/edge1/sensors`
    fn replicated_key(topic_name: &str) -> String {
        format!("{}{}", EDGE_REPLICATED_PREFIX, topic_name)
    }
}
