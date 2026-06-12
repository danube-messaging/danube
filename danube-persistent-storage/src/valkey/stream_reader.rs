/// `ValkeyStreamReader` — reads bounded ranges from a Valkey Redis Stream
/// and yields `StreamMessage` items as a `TopicStream`.
///
/// This is the Warm Tier read component. It is the inverse of the write path
/// (`XADD` in `TieredStorage::append_message`): it calls `XRANGE` to fetch
/// entries and deserializes each entry's `payload` field from bincode back
/// into a `StreamMessage`.

use super::client::ValkeyClient;
use crate::tiered_storage::TieredStorage;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};
use std::sync::Arc;
use tracing::warn;

/// Provides warm-tier reads from a Valkey Redis Stream for a single topic.
#[derive(Debug, Clone)]
pub struct ValkeyStreamReader {
    client: Arc<ValkeyClient>,
    /// The stream key, e.g. `/topics/default/my-topic/stream`
    stream_key: String,
}

impl ValkeyStreamReader {
    pub fn new(client: Arc<ValkeyClient>, topic_path: &str) -> Self {
        let stream_key = format!("/topics/{}/stream", topic_path);
        Self { client, stream_key }
    }

    /// Return the oldest offset available in the Valkey stream, or `None` if the stream is empty.
    ///
    /// This performs a single `XRANGE key - + COUNT 1` call (~0.1ms on LAN).
    pub async fn first_offset(&self) -> Option<u64> {
        match self.client.xrange_first(&self.stream_key).await {
            Ok(offset) => offset,
            Err(e) => {
                warn!(
                    target = "valkey_stream_reader",
                    stream_key = %self.stream_key,
                    error = %e,
                    "failed to query first offset from Valkey stream"
                );
                None
            }
        }
    }

    /// Read a bounded range of messages from the Valkey stream: `[start_offset, end_inclusive]`.
    ///
    /// Each stream entry is expected to have a single `payload` field containing
    /// a bincode-serialized `StreamMessage`. Stream IDs follow the format `<offset>-1`.
    ///
    /// Returns a `TopicStream` that yields messages in offset order.
    pub async fn read_range(
        &self,
        start_offset: u64,
        end_inclusive: u64,
    ) -> Result<TopicStream, PersistentStorageError> {
        let start_id = format!("{}-1", start_offset);
        let end_id = format!("{}-1", end_inclusive);

        let entries = self
            .client
            .xrange(&self.stream_key, &start_id, &end_id)
            .await
            .map_err(|e| {
                PersistentStorageError::Other(format!(
                    "Valkey XRANGE failed for {}: {}",
                    self.stream_key, e
                ))
            })?;

        // Deserialize all entries eagerly (they are already in memory from XRANGE)
        let mut messages: Vec<StreamMessage> = Vec::with_capacity(entries.len());
        for (stream_id, bytes) in entries {
            match TieredStorage::deserialize_message(&bytes) {
                Ok(msg) => messages.push(msg),
                Err(e) => {
                    warn!(
                        target = "valkey_stream_reader",
                        stream_key = %self.stream_key,
                        stream_id = %stream_id,
                        error = %e,
                        "failed to deserialize message from Valkey stream — skipping"
                    );
                }
            }
        }

        // Convert into a stream using async_stream (consistent with DurableHistoryReader)
        let s = async_stream::try_stream! {
            for msg in messages {
                yield msg;
            }
        };
        Ok(Box::pin(s))
    }
}
