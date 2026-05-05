//! Replication checkpoint persistence via Raft metadata store.
//!
//! Tracks the last successfully replicated WAL offset per topic so the edge
//! can resume from the correct position after restart.
//!
//! Checkpoints are stored in Raft under `/edge/checkpoints/{topic_name}`.

use anyhow::Result;
use std::sync::Arc;
use tracing::debug;

use danube_core::metadata::{MetaOptions, MetadataStore};

/// Base path for edge replication checkpoints in the metadata store.
const CHECKPOINT_PREFIX: &str = "/edge/checkpoints";

/// Checkpoint store backed by the Raft metadata store.
///
/// Thread-safe via the underlying `MetadataStore` implementation.
pub struct CheckpointStore {
    store: Arc<dyn MetadataStore>,
}

impl CheckpointStore {
    /// Create a new checkpoint store using the given metadata store.
    pub fn new(store: Arc<dyn MetadataStore>) -> Self {
        Self { store }
    }

    /// Get the last replicated offset for a topic.
    pub async fn load(&self, topic_name: &str) -> Option<u64> {
        let key = Self::key_for(topic_name);
        match self.store.get(&key, MetaOptions::None).await {
            Ok(Some(value)) => value.as_u64(),
            _ => None,
        }
    }

    /// Save a checkpoint for a topic.
    pub async fn save(&self, topic_name: &str, offset: u64) -> Result<()> {
        let key = Self::key_for(topic_name);
        let value = serde_json::Value::from(offset);
        self.store
            .put(&key, value, MetaOptions::None)
            .await
            .map_err(|e| anyhow::anyhow!("checkpoint save failed: {}", e))?;

        debug!(topic = %topic_name, offset, "saved replication checkpoint");
        Ok(())
    }

    /// Build the metadata store key for a topic checkpoint.
    ///
    /// E.g., topic `/edge1/sensors` → key `/edge/checkpoints/edge1/sensors`
    fn key_for(topic_name: &str) -> String {
        format!("{}{}", CHECKPOINT_PREFIX, topic_name)
    }
}
