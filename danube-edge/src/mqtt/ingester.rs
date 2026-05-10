//! Per-topic batching ingester for MQTT messages.
//!
//! Mirrors the `EdgeReplicator` pattern but for the *inbound* path:
//! messages are buffered per-Danube-topic in memory and flushed to the
//! local WAL via `append_batch()` when either `batch_size` messages
//! accumulate or `batch_timeout` elapses.
//!
//! The `EdgeReplicator`'s WAL tail reader automatically picks up new
//! data — no explicit wakeup is needed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use danube_core::message::StreamMessage;
use danube_persistent_storage::{StorageFactory, WalStorage};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::readiness::TopicReadiness;

/// Configuration for the ingester's batching behavior.
#[derive(Debug, Clone)]
pub struct MqttIngesterConfig {
    /// Flush when this many messages accumulate for a single topic.
    pub batch_size: usize,
    /// Flush after this timeout even if `batch_size` is not reached.
    pub batch_timeout: Duration,
}

impl Default for MqttIngesterConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_timeout: Duration::from_millis(500),
        }
    }
}

/// Per-topic in-memory buffer.
struct TopicBuffer {
    messages: Vec<StreamMessage>,
    wal: WalStorage,
    last_flush: Instant,
}

/// High-throughput MQTT message ingester.
///
/// Accepts validated `StreamMessage`s from MQTT sessions, buffers them
/// per-Danube-topic, and flushes batches to the WAL. This avoids the
/// per-message overhead of `Topic::publish_message_async` (producer
/// registry locks, single appends, etc.).
pub struct MqttIngester {
    storage_factory: StorageFactory,
    config: MqttIngesterConfig,
    buffers: Arc<Mutex<HashMap<String, TopicBuffer>>>,
    /// Shared readiness tracker. Messages for not-ready topics are rejected.
    readiness: TopicReadiness,
}

impl MqttIngester {
    /// Create a new ingester.
    ///
    /// Call `provision_topic()` for each Danube topic from the config
    /// before accepting MQTT traffic, then spawn `run_flush_loop()`.
    pub fn new(
        storage_factory: StorageFactory,
        config: MqttIngesterConfig,
        readiness: TopicReadiness,
    ) -> Self {
        Self {
            storage_factory,
            config,
            buffers: Arc::new(Mutex::new(HashMap::new())),
            readiness,
        }
    }

    /// Pre-provision a Danube topic: resolve its WAL handle and create
    /// an empty buffer. Called at edge bootstrap for each topic in the
    /// MQTT config's `topic_mappings`.
    pub async fn provision_topic(&self, topic_name: &str) -> Result<()> {
        let wal = self
            .storage_factory
            .for_topic(topic_name)
            .await
            .map_err(|e| anyhow::anyhow!("failed to get WAL for topic '{}': {}", topic_name, e))?;

        let mut buffers = self.buffers.lock().await;
        buffers.entry(topic_name.to_string()).or_insert(TopicBuffer {
            messages: Vec::with_capacity(self.config.batch_size),
            wal,
            last_flush: Instant::now(),
        });

        info!(topic = %topic_name, "MQTT ingester: topic provisioned");
        Ok(())
    }

    /// Enqueue a message for batched WAL ingestion.
    ///
    /// **Readiness gate**: rejects messages if the topic is not fully ready
    /// (local + cluster + schema). QoS 1 MQTT clients will retry later.
    ///
    /// **Schema stamping**: if a schema is configured for this topic, stamps
    /// `schema_id` and `schema_version` on the message before buffering.
    ///
    /// If the buffer for this topic reaches `batch_size`, it is flushed
    /// immediately (inline). Otherwise the background flush loop will
    /// pick it up on the next timeout tick.
    ///
    /// Returns an error if the topic was not provisioned or is not ready.
    pub async fn ingest(&self, topic_name: &str, mut message: StreamMessage) -> Result<()> {
        // Gate: reject if topic is not ready
        if !self.readiness.is_ready(topic_name).await {
            return Err(anyhow::anyhow!(
                "topic '{}' is not ready for ingestion (waiting for cluster registration or schema resolution)",
                topic_name
            ));
        }

        // Stamp schema metadata if a schema is configured for this topic
        if let Some(schema_info) = self.readiness.get_schema_info(topic_name).await {
            message.schema_id = Some(schema_info.schema_id);
            message.schema_version = Some(schema_info.schema_version);
        }

        let mut buffers = self.buffers.lock().await;

        let buffer = buffers.get_mut(topic_name).ok_or_else(|| {
            anyhow::anyhow!(
                "MQTT ingester: topic '{}' not provisioned (no mapping in config)",
                topic_name
            )
        })?;

        buffer.messages.push(message);

        // Flush inline if batch is full
        if buffer.messages.len() >= self.config.batch_size {
            Self::flush_buffer(topic_name, buffer).await?;
        }

        Ok(())
    }

    /// Background flush loop. Runs forever, checking all topic buffers
    /// every `batch_timeout` and flushing any that have pending messages
    /// older than the timeout.
    ///
    /// Spawn this as a Tokio task at edge startup.
    pub async fn run_flush_loop(self: Arc<Self>) {
        let interval = self.config.batch_timeout;
        info!(
            batch_size = self.config.batch_size,
            batch_timeout_ms = interval.as_millis() as u64,
            "MQTT ingester: starting flush loop"
        );

        loop {
            tokio::time::sleep(interval).await;

            let mut buffers = self.buffers.lock().await;
            for (topic_name, buffer) in buffers.iter_mut() {
                if buffer.messages.is_empty() {
                    continue;
                }

                let age = buffer.last_flush.elapsed();
                if age >= interval {
                    if let Err(e) = Self::flush_buffer(topic_name, buffer).await {
                        error!(
                            topic = %topic_name,
                            error = %e,
                            "MQTT ingester: flush failed"
                        );
                    }
                }
            }
        }
    }

    /// Flush a topic buffer to the WAL via `append_batch`.
    async fn flush_buffer(topic_name: &str, buffer: &mut TopicBuffer) -> Result<()> {
        let count = buffer.messages.len();
        if count == 0 {
            return Ok(());
        }

        let messages: Vec<StreamMessage> = buffer.messages.drain(..).collect();

        match buffer.wal.append_batch(topic_name, &messages).await {
            Ok((first, last)) => {
                debug!(
                    topic = %topic_name,
                    count,
                    first_offset = first,
                    last_offset = last,
                    "MQTT ingester: batch flushed to WAL"
                );
            }
            Err(e) => {
                warn!(
                    topic = %topic_name,
                    count,
                    error = %e,
                    "MQTT ingester: WAL append_batch failed, messages lost"
                );
                return Err(anyhow::anyhow!("WAL append_batch failed: {}", e));
            }
        }

        buffer.last_flush = Instant::now();
        Ok(())
    }
}
