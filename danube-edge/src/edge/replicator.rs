//! Edge replicator orchestrator.
//!
//! Manages per-topic replication tasks. The broker calls `add_topic()` when
//! a new topic is created on the edge, and the orchestrator spawns a
//! `TopicReplicator` task that tails the local WAL and streams batches
//! to the cloud cluster.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use danube_core::metadata::MetadataStore;
use danube_persistent_storage::StorageFactory;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::edge::checkpoint::CheckpointStore;
use crate::edge::cluster_client::EdgeCloudClient;
use crate::edge::topic_replicator::TopicReplicator;

/// Configuration for the edge replicator.
pub struct EdgeReplicatorConfig {
    /// Number of messages per batch before sending to cloud.
    pub batch_size: usize,
    /// Maximum time to wait before sending a partial batch.
    pub batch_timeout: Duration,
}

impl Default for EdgeReplicatorConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_timeout: Duration::from_millis(1000),
        }
    }
}

/// Edge replicator orchestrator.
///
/// Manages the lifecycle of per-topic `TopicReplicator` tasks. Each topic
/// gets its own background task that tails the WAL and streams to the cloud.
pub struct EdgeReplicator {
    cloud_client: Arc<EdgeCloudClient>,
    storage_factory: StorageFactory,
    config: EdgeReplicatorConfig,
    checkpoint: Arc<CheckpointStore>,
    tasks: Mutex<HashMap<String, JoinHandle<()>>>,
}

impl EdgeReplicator {
    /// Create a new edge replicator.
    ///
    /// Checkpoint state is persisted in Raft via the `metadata_store`.
    pub fn new(
        cloud_client: Arc<EdgeCloudClient>,
        storage_factory: StorageFactory,
        metadata_store: Arc<dyn MetadataStore>,
        config: EdgeReplicatorConfig,
    ) -> Self {
        let checkpoint = Arc::new(CheckpointStore::new(metadata_store));

        Self {
            cloud_client,
            storage_factory,
            config,
            checkpoint,
            tasks: Mutex::new(HashMap::new()),
        }
    }

    /// Start replicating a topic to the cloud.
    ///
    /// Spawns a background `TopicReplicator` task that:
    /// 1. Obtains the WAL handle via `StorageFactory`
    /// 2. Tails from the last checkpointed offset
    /// 3. Batches and sends messages to the cloud with retry
    ///
    /// Idempotent: if the topic is already being replicated, this is a no-op.
    pub async fn add_topic(&self, topic_name: &str) {
        let mut tasks = self.tasks.lock().await;

        if tasks.contains_key(topic_name) {
            info!(
                topic = %topic_name,
                "topic already being replicated, skipping"
            );
            return;
        }

        // Get WAL storage for this topic
        let wal_storage = match self.storage_factory.for_topic(topic_name).await {
            Ok(ws) => ws,
            Err(e) => {
                warn!(
                    topic = %topic_name,
                    error = %e,
                    "failed to get WAL storage for topic replication"
                );
                return;
            }
        };

        let topic = topic_name.to_string();
        let client = self.cloud_client.clone();
        let checkpoint = self.checkpoint.clone();
        let batch_size = self.config.batch_size;
        let batch_timeout = self.config.batch_timeout;

        let handle = tokio::spawn(async move {
            TopicReplicator::run(topic, wal_storage, client, checkpoint, batch_size, batch_timeout)
                .await;
        });

        tasks.insert(topic_name.to_string(), handle);
        info!(topic = %topic_name, "started topic replication");
    }

    /// Stop replicating a topic.
    ///
    /// Aborts the background task. The last checkpoint is preserved in Raft,
    /// so replication can resume later from where it left off.
    pub async fn remove_topic(&self, topic_name: &str) {
        let mut tasks = self.tasks.lock().await;
        if let Some(handle) = tasks.remove(topic_name) {
            handle.abort();
            info!(topic = %topic_name, "stopped topic replication");
        }
    }

    /// Get the list of topics currently being replicated.
    pub async fn active_topics(&self) -> Vec<String> {
        let tasks = self.tasks.lock().await;
        tasks.keys().cloned().collect()
    }

    /// Get a reference to the checkpoint store.
    pub fn checkpoint_store(&self) -> &Arc<CheckpointStore> {
        &self.checkpoint
    }

    /// Get a reference to the cloud client.
    pub fn cloud_client(&self) -> &Arc<EdgeCloudClient> {
        &self.cloud_client
    }
}
