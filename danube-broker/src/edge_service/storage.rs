//! Edge replication storage — WAL ingestion and metadata management.
//!
//! Uses the broker's `StorageFactory` for WAL access and `Resources`
//! for metadata management (namespace/topic creation in Raft).

use anyhow::Result;
use tracing::{debug, info};

use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_core::message::StreamMessage;
use danube_persistent_storage::StorageFactory;

use crate::policies::Policies;
use crate::resources::Resources;

/// Broker-side storage for edge replication.
///
/// Provides two key capabilities:
/// 1. **Metadata management** — ensures namespaces/topics exist in Raft
/// 2. **WAL batch ingestion** — writes directly to topic WALs via `append_batch()`
pub(crate) struct EdgeReplicationStorage {
    resources: Resources,
    storage_factory: StorageFactory,
}

impl EdgeReplicationStorage {
    pub(crate) fn new(resources: Resources, storage_factory: StorageFactory) -> Self {
        Self {
            resources,
            storage_factory,
        }
    }

    /// Write a batch of messages directly to the topic's WAL.
    ///
    /// Returns the last WAL offset written on the cluster side.
    pub(crate) async fn ingest_batch(
        &self,
        topic_name: &str,
        messages: Vec<StreamMessage>,
    ) -> Result<u64> {
        // Get or create the WAL for this topic (cached in DashMap)
        let wal_storage = self
            .storage_factory
            .for_topic(topic_name)
            .await
            .map_err(|e| anyhow::anyhow!("failed to get WAL storage: {}", e))?;

        let count = messages.len();

        // Write directly to WAL via batch append (one offset bump, one cache lock)
        let (_first, last) = wal_storage
            .append_batch(topic_name, &messages)
            .await
            .map_err(|e| anyhow::anyhow!("batch append failed: {}", e))?;

        debug!(
            topic = %topic_name,
            count,
            last_offset = last,
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
}
