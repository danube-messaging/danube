/// `TieredStorage` — the single orchestrator for Danube's tiered storage.
///
/// This struct implements `PersistentStorage` and manages up to three
/// independent storage tiers:
///
/// - **Hot** (`WalStorage`): Local disk WAL — always present.
/// - **Warm** (`ValkeyStreamReader`): Valkey Redis Streams — present when `write_buffer` is configured.
/// - **Cold** (`DurableHistoryReader`): S3/Cloud Storage — present when durable storage is configured.
///
/// ## Write Path
/// - If Warm is configured: double-write (WAL + XADD to Valkey) + WAIT for quorum.
/// - If Warm is NOT configured: write to WAL only.
///
/// ## Read Path
/// - `StartPosition::Latest`: bypass all tiering → WAL broadcast stream (fast path).
/// - `StartPosition::Offset(n)`: stitch `cold.chain(warm).chain(hot)` based on tier boundaries.
///
/// ## Supported Configurations
/// 1. WAL only (edge nodes, dev/test)
/// 2. WAL + Valkey (HA without cloud archival)
/// 3. WAL + S3 (archival without HA buffer)
/// 4. WAL + Valkey + S3 (full production deployment)

use crate::durable_history_reader::DurableHistoryReader;
use crate::persistent_metrics::DURABLE_HISTORY_TO_HOT_TOTAL;
use crate::valkey::config::{WaitTimeoutPolicy, WriteBufferConfig};
use crate::valkey::stream_reader::ValkeyStreamReader;
use crate::valkey::ValkeyClient;
use crate::wal_storage::WalStorage;
use async_trait::async_trait;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorage, PersistentStorageError, StartPosition, TopicStream};
use metrics::counter;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

/// The single orchestrator for tiered storage.
#[derive(Debug)]
pub struct TieredStorage {
    /// Hot Tier — local disk WAL (always present).
    hot: WalStorage,
    /// Warm Tier — Valkey Redis Streams (optional).
    warm: Option<WarmTier>,
    /// Cold Tier — S3/Cloud durable history (optional).
    cold: Option<DurableHistoryReader>,
    /// Logical topic path for key construction and logging.
    topic_path: String,
    /// When true, the hot tier boundary uses `earliest_cached_offset` instead
    /// of the WAL checkpoint (sealed recovery mode after topic mobility).
    history_cutover_from_hot: bool,
}

/// Warm Tier state: Valkey client + stream reader + write buffer config.
#[derive(Debug)]
struct WarmTier {
    reader: ValkeyStreamReader,
    client: Arc<ValkeyClient>,
    config: WriteBufferConfig,
}

impl TieredStorage {
    /// Create a new `TieredStorage`.
    ///
    /// - `hot`: The local WAL (always required).
    /// - `warm`: Optional Valkey stream reader + client + config for double-writes and warm reads.
    /// - `cold`: Optional durable history reader for S3 reads.
    /// - `topic_path`: Logical topic path (e.g., "default/my-topic").
    pub fn new(
        hot: WalStorage,
        warm: Option<(ValkeyStreamReader, Arc<ValkeyClient>, WriteBufferConfig)>,
        cold: Option<DurableHistoryReader>,
        topic_path: String,
    ) -> Self {
        let warm = warm.map(|(reader, client, config)| WarmTier {
            reader,
            client,
            config,
        });

        info!(
            topic = %topic_path,
            has_warm = warm.is_some(),
            has_cold = cold.is_some(),
            "TieredStorage initialized"
        );

        Self {
            hot,
            warm,
            cold,
            topic_path,
            history_cutover_from_hot: false,
        }
    }

    /// Enable sealed recovery mode: the hot tier boundary will use `earliest_cached_offset`
    /// instead of the WAL checkpoint.
    pub fn with_hot_cutover(mut self) -> Self {
        self.history_cutover_from_hot = true;
        self
    }

    /// The Valkey stream key for this topic.
    fn stream_key(&self) -> String {
        format!("/topics/{}/stream", self.topic_path)
    }

    /// Serialize a `StreamMessage` to bytes using bincode.
    pub(crate) fn serialize_message(msg: &StreamMessage) -> Result<Vec<u8>, PersistentStorageError> {
        let config = bincode::config::standard();
        bincode::serde::encode_to_vec(msg, config).map_err(|e| {
            PersistentStorageError::Other(format!("bincode serialize failed: {}", e))
        })
    }

    /// Deserialize a `StreamMessage` from bincode bytes.
    ///
    /// Used during crash recovery and warm-tier reads to reconstruct messages
    /// from Valkey stream entries.
    pub(crate) fn deserialize_message(bytes: &[u8]) -> Result<StreamMessage, PersistentStorageError> {
        let config = bincode::config::standard();
        let (msg, _): (StreamMessage, _) =
            bincode::serde::decode_from_slice(bytes, config).map_err(|e| {
                PersistentStorageError::Other(format!("bincode deserialize failed: {}", e))
            })?;
        Ok(msg)
    }

    /// Handle the result of a WAIT command according to the configured policy.
    fn handle_wait_result(
        &self,
        result: &crate::valkey::WaitResult,
        config: &WriteBufferConfig,
        offset: u64,
    ) -> Result<(), PersistentStorageError> {
        if result.timed_out {
            match config.on_wait_timeout {
                WaitTimeoutPolicy::Ack => {
                    warn!(
                        offset = offset,
                        confirmed = result.confirmed_replicas,
                        requested = config.wait_replicas,
                        "WAIT timed out but policy is Ack — proceeding"
                    );
                    Ok(())
                }
                WaitTimeoutPolicy::Fail => {
                    error!(
                        offset = offset,
                        confirmed = result.confirmed_replicas,
                        requested = config.wait_replicas,
                        "WAIT timed out and policy is Fail — returning error"
                    );
                    Err(PersistentStorageError::Other(format!(
                        "write buffer WAIT timed out: {} of {} replicas confirmed",
                        result.confirmed_replicas, config.wait_replicas
                    )))
                }
            }
        } else {
            Ok(())
        }
    }

    /// Compute the first offset available in the Hot Tier (local WAL).
    ///
    /// - In sealed recovery mode (`history_cutover_from_hot`), uses `earliest_cached_offset`.
    /// - In normal mode, uses the WAL checkpoint's `start_offset`.
    async fn hot_start_offset(&self) -> u64 {
        if self.history_cutover_from_hot {
            self.hot
                .earliest_cached_offset()
                .await
                .unwrap_or_else(|| self.hot.current_offset())
        } else {
            self.hot.first_local_offset().await
        }
    }
}

#[async_trait]
impl PersistentStorage for TieredStorage {
    /// Write path: append to local WAL, then optionally double-write to Valkey.
    async fn append_message(
        &self,
        topic_name: &str,
        msg: StreamMessage,
    ) -> Result<u64, PersistentStorageError> {
        // 1. Local WAL append (fast, local disk)
        let offset = self.hot.append_message(topic_name, msg.clone()).await?;

        // 2. If Warm Tier is configured, write to Valkey + WAIT
        if let Some(ref warm) = self.warm {
            let bytes = Self::serialize_message(&msg)?;
            let key = self.stream_key();
            let id = format!("{}-1", offset);

            let wait_result = warm
                .client
                .xadd_and_wait(
                    &key,
                    &id,
                    "payload",
                    &bytes,
                    warm.config.wait_replicas,
                    warm.config.wait_timeout_ms,
                )
                .await
                .map_err(|e| {
                    PersistentStorageError::Other(format!("write buffer XADD+WAIT failed: {}", e))
                })?;

            self.handle_wait_result(&wait_result, &warm.config, offset)?;
        }

        Ok(offset)
    }

    /// Batch write path: append batch to local WAL, then optionally pipeline XADD + WAIT.
    async fn append_batch(
        &self,
        topic_name: &str,
        messages: &[StreamMessage],
    ) -> Result<(u64, u64), PersistentStorageError> {
        if messages.is_empty() {
            return Err(PersistentStorageError::Other("empty batch".to_string()));
        }

        // 1. Local WAL batch append
        let (first, last) = self.hot.append_batch(topic_name, messages).await?;

        // 2. If Warm Tier is configured, pipeline XADD + WAIT
        if let Some(ref warm) = self.warm {
            let mut entries = Vec::with_capacity(messages.len());
            let mut offset = first;
            for msg in messages {
                let bytes = Self::serialize_message(msg)?;
                entries.push((format!("{}-1", offset), "payload".to_string(), bytes));
                offset += 1;
            }

            let key = self.stream_key();
            let wait_result = warm
                .client
                .xadd_batch_and_wait(
                    &key,
                    &entries,
                    warm.config.wait_replicas,
                    warm.config.wait_timeout_ms,
                )
                .await
                .map_err(|e| {
                    PersistentStorageError::Other(format!(
                        "write buffer batch XADD+WAIT failed: {}",
                        e
                    ))
                })?;

            self.handle_wait_result(&wait_result, &warm.config, last)?;
        }

        Ok((first, last))
    }

    /// Read path: the single orchestrator for tiered reads.
    ///
    /// - `StartPosition::Latest`: bypass all tiering → WAL broadcast stream.
    /// - `StartPosition::Offset(n)`: stitch `cold.chain(warm).chain(hot)` based on boundaries.
    async fn create_reader(
        &self,
        topic_name: &str,
        start: StartPosition,
    ) -> Result<TopicStream, PersistentStorageError> {
        // --- Live Tail Fast Path ---
        if let StartPosition::Latest = start {
            debug!(
                target = "tiered_storage",
                topic = %self.topic_path,
                "creating live-tail reader (bypassing tiering)"
            );
            return self
                .hot
                .create_hot_reader(topic_name, self.hot.current_offset(), true)
                .await;
        }

        let start_offset = match start {
            StartPosition::Offset(o) => o,
            _ => unreachable!(),
        };

        // --- Historical Read Path ---
        let hot_start = self.hot_start_offset().await;

        // If the requested offset is within the Hot Tier, go directly to WAL
        if start_offset >= hot_start {
            debug!(
                target = "tiered_storage",
                topic = %self.topic_path,
                start = start_offset,
                hot_start = hot_start,
                "creating reader from Hot Tier (WAL only)"
            );
            return self
                .hot
                .create_hot_reader(topic_name, start_offset, false)
                .await;
        }

        // Determine Warm Tier boundary
        let warm_start = if let Some(ref warm) = self.warm {
            warm.reader.first_offset().await.unwrap_or(hot_start)
        } else {
            hot_start
        };

        info!(
            target = "tiered_storage",
            topic = %self.topic_path,
            start = start_offset,
            warm_start = warm_start,
            hot_start = hot_start,
            has_cold = self.cold.is_some(),
            has_warm = self.warm.is_some(),
            "creating tiered reader"
        );

        // --- Cold Tier (S3) ---
        let cold_stream: Option<TopicStream> = if let Some(ref cold) = self.cold {
            if start_offset < warm_start {
                let end = warm_start.saturating_sub(1);
                info!(
                    target = "tiered_storage",
                    topic = %self.topic_path,
                    start = start_offset,
                    end = end,
                    "reading from Cold Tier (S3)"
                );
                counter!(DURABLE_HISTORY_TO_HOT_TOTAL.name, "topic" => topic_name.to_string())
                    .increment(1);
                Some(cold.read_range(start_offset, Some(end)).await?)
            } else {
                None
            }
        } else {
            // No Cold Tier — if the requested offset is before the Warm Tier, it's a gap
            if start_offset < warm_start {
                warn!(
                    target = "tiered_storage",
                    topic = %self.topic_path,
                    start = start_offset,
                    warm_start = warm_start,
                    "requested offset is before warm tier but no cold tier is configured — data may be unavailable"
                );
            }
            None
        };

        // --- Warm Tier (Valkey) ---
        let warm_stream: Option<TopicStream> = if let Some(ref warm) = self.warm {
            let warm_read_start = std::cmp::max(start_offset, warm_start);
            if warm_read_start < hot_start {
                let end = hot_start.saturating_sub(1);
                debug!(
                    target = "tiered_storage",
                    topic = %self.topic_path,
                    start = warm_read_start,
                    end = end,
                    "reading from Warm Tier (Valkey)"
                );
                Some(warm.reader.read_range(warm_read_start, end).await?)
            } else {
                None
            }
        } else {
            None
        };

        // --- Hot Tier (WAL) ---
        // When no Cold or Warm tier contributed a stream, let the WAL try from
        // the original requested offset — the WAL's StatefulReader can still
        // replay from physical files on disk even if they predate the checkpoint.
        let hot_read_start = if cold_stream.is_none() && warm_stream.is_none() {
            start_offset
        } else {
            std::cmp::max(start_offset, hot_start)
        };
        let hot_stream = self
            .hot
            .create_hot_reader(topic_name, hot_read_start, false)
            .await?;

        // --- Stitch streams: Cold → Warm → Hot ---
        let combined: TopicStream = match (cold_stream, warm_stream) {
            (Some(cold), Some(warm)) => {
                Box::pin(cold.chain(warm).chain(hot_stream))
            }
            (Some(cold), None) => {
                Box::pin(cold.chain(hot_stream))
            }
            (None, Some(warm)) => {
                Box::pin(warm.chain(hot_stream))
            }
            (None, None) => {
                hot_stream
            }
        };

        Ok(combined)
    }

    async fn ack_checkpoint(
        &self,
        _topic_name: &str,
        _up_to_offset: u64,
    ) -> Result<(), PersistentStorageError> {
        // ack_checkpoint is a no-op for the local WAL tier
        Ok(())
    }

    async fn flush(&self, _topic_name: &str) -> Result<(), PersistentStorageError> {
        self.hot.flush().await
    }

    fn current_offset(&self) -> u64 {
        self.hot.current_offset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use danube_core::message::MessageID;
    use std::collections::HashMap;

    fn make_test_message(offset: u64) -> StreamMessage {
        StreamMessage {
            request_id: offset,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test-topic".to_string(),
                broker_addr: "localhost:6650".to_string(),
                topic_offset: offset,
            },
            payload: format!("payload-{}", offset).into_bytes().into(),
            publish_time: offset,
            producer_name: "producer-1".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
            schema_id: None,
            schema_version: None,
            routing_key: None,
        }
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let msg = make_test_message(42);
        let bytes = TieredStorage::serialize_message(&msg).expect("serialize");
        let recovered = TieredStorage::deserialize_message(&bytes).expect("deserialize");
        assert_eq!(recovered.request_id, 42);
        assert_eq!(recovered.payload.as_ref(), b"payload-42");
    }

    #[test]
    fn test_serialize_multiple_messages() {
        let msg = make_test_message(1);
        let bytes1 = TieredStorage::serialize_message(&msg).expect("serialize 1");
        let bytes2 = TieredStorage::serialize_message(&msg).expect("serialize 2");
        assert_eq!(bytes1, bytes2, "same message should produce same bytes");
    }

    #[test]
    fn test_serialize_different_offsets() {
        let msg1 = make_test_message(1);
        let msg2 = make_test_message(2);
        let bytes1 = TieredStorage::serialize_message(&msg1).expect("serialize 1");
        let bytes2 = TieredStorage::serialize_message(&msg2).expect("serialize 2");
        assert_ne!(bytes1, bytes2, "different offsets should produce different bytes");
    }

    #[test]
    fn test_deserialize_invalid_bytes() {
        let bytes = vec![0xFF, 0xFE, 0xFD]; // garbage
        let result = TieredStorage::deserialize_message(&bytes);
        assert!(result.is_err());
    }
}
