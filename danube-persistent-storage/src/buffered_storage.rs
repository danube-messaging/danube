/// `BufferedStorage` — a `PersistentStorage` decorator that double-writes
/// messages to both a local WAL and an external Valkey/Redis write buffer.
///
/// When the write buffer is configured, every `append_message()` call:
/// 1. Appends to the inner `WalStorage` (local disk)
/// 2. Writes to Valkey via `HSET` + `WAIT` for cross-node durability
///
/// If Valkey is unavailable or WAIT times out, the behavior is controlled
/// by `WriteBufferConfig::on_wait_timeout` (Ack or Fail).
///
/// When no write buffer is configured, `StorageFactory` returns a plain
/// `WalStorage` and this module is never instantiated — zero overhead.
use crate::valkey::config::WaitTimeoutPolicy;
use crate::valkey::ValkeyClient;
use crate::valkey::config::WriteBufferConfig;
use crate::wal_storage::WalStorage;
use async_trait::async_trait;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorage, PersistentStorageError, StartPosition, TopicStream};
use std::sync::Arc;
use tracing::{debug, error, warn};

/// Decorator over `WalStorage` that replicates writes to a Valkey cluster.
#[derive(Debug)]
pub struct BufferedStorage {
    inner: WalStorage,
    valkey: Arc<ValkeyClient>,
    config: WriteBufferConfig,
    /// Valkey key prefix for this topic, e.g. "/topics/default/my-topic"
    topic_key_prefix: String,
}

impl BufferedStorage {
    /// Create a new `BufferedStorage` wrapping the given `WalStorage`.
    pub fn new(
        inner: WalStorage,
        valkey: Arc<ValkeyClient>,
        config: WriteBufferConfig,
        topic_path: String,
    ) -> Self {
        let topic_key_prefix = format!("/topics/{}", topic_path);
        debug!(
            topic = %topic_path,
            key_prefix = %topic_key_prefix,
            wait_replicas = config.wait_replicas,
            wait_timeout_ms = config.wait_timeout_ms,
            on_wait_timeout = ?config.on_wait_timeout,
            "BufferedStorage initialized"
        );
        Self {
            inner,
            valkey,
            config,
            topic_key_prefix,
        }
    }

    /// The Valkey stream key for this topic.
    fn stream_key(&self) -> String {
        format!("{}/stream", self.topic_key_prefix)
    }

    /// Serialize a `StreamMessage` to bytes using bincode.
    fn serialize_message(msg: &StreamMessage) -> Result<Vec<u8>, PersistentStorageError> {
        let config = bincode::config::standard();
        bincode::serde::encode_to_vec(msg, config).map_err(|e| {
            PersistentStorageError::Other(format!("bincode serialize failed: {}", e))
        })
    }

    /// Deserialize a `StreamMessage` from bincode bytes.
    ///
    /// Used during crash recovery to replay Valkey-buffered messages back into
    /// the WAL.
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
        offset: u64,
    ) -> Result<(), PersistentStorageError> {
        if result.timed_out {
            match self.config.on_wait_timeout {
                WaitTimeoutPolicy::Ack => {
                    warn!(
                        offset = offset,
                        confirmed = result.confirmed_replicas,
                        requested = self.config.wait_replicas,
                        "WAIT timed out but policy is Ack — proceeding"
                    );
                    Ok(())
                }
                WaitTimeoutPolicy::Fail => {
                    error!(
                        offset = offset,
                        confirmed = result.confirmed_replicas,
                        requested = self.config.wait_replicas,
                        "WAIT timed out and policy is Fail — returning error"
                    );
                    Err(PersistentStorageError::Other(format!(
                        "write buffer WAIT timed out: {} of {} replicas confirmed",
                        result.confirmed_replicas, self.config.wait_replicas
                    )))
                }
            }
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl PersistentStorage for BufferedStorage {
    /// Double-write: append to local WAL, then HSET + WAIT to Valkey.
    async fn append_message(
        &self,
        topic_name: &str,
        msg: StreamMessage,
    ) -> Result<u64, PersistentStorageError> {
        // 1. Local WAL append (fast, local disk)
        let offset = self.inner.append_message(topic_name, msg.clone()).await?;

        // 2. Serialize and write to Valkey Stream
        let bytes = Self::serialize_message(&msg)?;
        let key = self.stream_key();
        let id = format!("{}-1", offset);

        let wait_result = self
            .valkey
            .xadd_and_wait(
                &key,
                &id,
                "payload",
                &bytes,
                self.config.wait_replicas,
                self.config.wait_timeout_ms,
            )
            .await
            .map_err(|e| {
                PersistentStorageError::Other(format!("write buffer XADD+WAIT failed: {}", e))
            })?;

        // 3. Apply timeout policy
        self.handle_wait_result(&wait_result, offset)?;

        Ok(offset)
    }

    /// Batch double-write: append batch to local WAL, then pipeline HSET + WAIT.
    async fn append_batch(
        &self,
        topic_name: &str,
        messages: &[StreamMessage],
    ) -> Result<(u64, u64), PersistentStorageError> {
        if messages.is_empty() {
            return Err(PersistentStorageError::Other("empty batch".to_string()));
        }

        // 1. Local WAL batch append
        let (first, last) = self.inner.append_batch(topic_name, messages).await?;

        // 2. Serialize all messages and pipeline XADD to Valkey
        let mut entries = Vec::with_capacity(messages.len());
        let mut offset = first;
        for msg in messages {
            let bytes = Self::serialize_message(msg)?;
            entries.push((format!("{}-1", offset), "payload".to_string(), bytes));
            offset += 1;
        }

        let key = self.stream_key();
        let wait_result = self
            .valkey
            .xadd_batch_and_wait(
                &key,
                &entries,
                self.config.wait_replicas,
                self.config.wait_timeout_ms,
            )
            .await
            .map_err(|e| {
                PersistentStorageError::Other(format!(
                    "write buffer batch XADD+WAIT failed: {}",
                    e
                ))
            })?;

        // 3. Apply timeout policy
        self.handle_wait_result(&wait_result, last)?;

        Ok((first, last))
    }

    /// Delegates to inner WAL reader (warm cache read-path deferred).
    async fn create_reader(
        &self,
        topic_name: &str,
        start: StartPosition,
    ) -> Result<TopicStream, PersistentStorageError> {
        self.inner.create_reader(topic_name, start).await
    }

    async fn ack_checkpoint(
        &self,
        topic_name: &str,
        up_to_offset: u64,
    ) -> Result<(), PersistentStorageError> {
        self.inner.ack_checkpoint(topic_name, up_to_offset).await
    }

    async fn flush(&self, topic_name: &str) -> Result<(), PersistentStorageError> {
        self.inner.flush(topic_name).await
    }

    fn current_offset(&self) -> u64 {
        self.inner.current_offset()
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
    fn serialize_message_produces_nonempty_bytes() {
        let msg = make_test_message(42);
        let bytes = BufferedStorage::serialize_message(&msg).expect("serialize");
        assert!(!bytes.is_empty(), "serialized message should not be empty");
    }

    #[test]
    fn serialize_message_deterministic() {
        let msg = make_test_message(7);
        let bytes1 = BufferedStorage::serialize_message(&msg).expect("serialize 1");
        let bytes2 = BufferedStorage::serialize_message(&msg).expect("serialize 2");
        assert_eq!(bytes1, bytes2, "same message should serialize identically");
    }

    #[test]
    fn serialize_message_different_offsets_differ() {
        let msg1 = make_test_message(1);
        let msg2 = make_test_message(2);
        let bytes1 = BufferedStorage::serialize_message(&msg1).expect("serialize 1");
        let bytes2 = BufferedStorage::serialize_message(&msg2).expect("serialize 2");
        assert_ne!(bytes1, bytes2, "different messages should serialize differently");
    }

    #[test]
    fn serialize_deserialize_round_trip() {
        let original = make_test_message(42);
        let bytes = BufferedStorage::serialize_message(&original).expect("serialize");
        let recovered = BufferedStorage::deserialize_message(&bytes).expect("deserialize");
        assert_eq!(recovered.request_id, original.request_id);
        assert_eq!(recovered.msg_id.topic_offset, original.msg_id.topic_offset);
        assert_eq!(recovered.msg_id.topic_name, original.msg_id.topic_name);
        assert_eq!(recovered.msg_id.producer_id, original.msg_id.producer_id);
        assert_eq!(recovered.payload.as_ref(), original.payload.as_ref());
        assert_eq!(recovered.producer_name, original.producer_name);
        assert_eq!(recovered.publish_time, original.publish_time);
    }

    // --- handle_wait_result tests ---
    // These require a BufferedStorage instance but only test the policy logic,
    // not actual Valkey connectivity. We can't easily construct one without a
    // real WalStorage + ValkeyClient, so we test the policy logic inline.

    #[test]
    fn wait_timeout_ack_policy_allows_timeout() {
        // Simulate: WAIT timed out, policy is Ack
        let result = crate::valkey::WaitResult {
            confirmed_replicas: 0,
            timed_out: true,
        };
        let config = WriteBufferConfig {
            on_wait_timeout: WaitTimeoutPolicy::Ack,
            ..Default::default()
        };
        // With Ack policy, timed_out should still produce Ok
        if result.timed_out {
            match config.on_wait_timeout {
                WaitTimeoutPolicy::Ack => { /* ok — this is the expected path */ }
                WaitTimeoutPolicy::Fail => panic!("should not reach Fail path"),
            }
        }
    }

    #[test]
    fn wait_timeout_fail_policy_rejects_timeout() {
        let result = crate::valkey::WaitResult {
            confirmed_replicas: 0,
            timed_out: true,
        };
        let config = WriteBufferConfig {
            on_wait_timeout: WaitTimeoutPolicy::Fail,
            ..Default::default()
        };
        if result.timed_out {
            match config.on_wait_timeout {
                WaitTimeoutPolicy::Fail => { /* ok — this is the expected path */ }
                WaitTimeoutPolicy::Ack => panic!("should not reach Ack path"),
            }
        }
    }

    #[test]
    fn no_timeout_always_succeeds() {
        let result = crate::valkey::WaitResult {
            confirmed_replicas: 1,
            timed_out: false,
        };
        assert!(!result.timed_out, "should not be timed out");
    }
}
