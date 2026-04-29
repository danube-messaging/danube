//! Replication storage trait — abstracts WAL ingestion for cluster-side replication.
//!
//! The broker implements this trait using `StorageFactory` and `WalStorage`.
//! The replicator service uses it to write batches directly to WAL without
//! going through the producer pipeline.

use anyhow::Result;
use async_trait::async_trait;
use danube_core::message::StreamMessage;

/// Trait for writing replicated data directly to storage.
///
/// The cluster broker implements this using `StorageFactory::for_topic()` → `WalStorage::append_batch()`.
/// This bypasses the entire producer pipeline (no producer registration, schema validation,
/// rate limiting, or dispatcher wake — pure WAL writes).
#[async_trait]
pub trait ReplicationStorage: Send + Sync + 'static {
    /// Ensure a topic's WAL storage exists (creating it if needed) and write a batch
    /// of messages directly to it.
    ///
    /// Returns the last WAL offset written on the cluster side.
    async fn ingest_batch(&self, topic_name: &str, messages: Vec<StreamMessage>) -> Result<u64>;

    /// Ensure topic metadata exists in the cluster metadata store.
    ///
    /// This creates:
    /// - Topic entry with Reliable dispatch strategy
    /// - WAL storage via StorageFactory
    ///
    /// Idempotent: returns Ok if the topic already exists.
    async fn ensure_topic(&self, topic_name: &str) -> Result<()>;

    /// Ensure namespace exists in the cluster metadata store.
    ///
    /// Idempotent: returns Ok if the namespace already exists.
    async fn ensure_namespace(&self, namespace: &str) -> Result<()>;
}
