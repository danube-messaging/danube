use crate::persistent_metrics::{
    WAL_APPEND_BYTES_TOTAL, WAL_APPEND_TOTAL, WAL_READER_CREATE_TOTAL,
};
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, StartPosition, TopicStream};
use metrics::counter;
use tracing::info;

use crate::wal::Wal;

/// `WalStorage` — the Hot Tier.
///
/// A pure local-disk append-only log. It only knows how to append messages,
/// flush to disk, and create a local reader via `tail_reader()`.
#[derive(Debug, Default, Clone)]
pub struct WalStorage {
    wal: Wal,
}

impl WalStorage {
    pub fn from_wal(wal: Wal) -> Self {
        Self { wal }
    }

    /// Return the next offset that will be assigned to the next appended message.
    ///
    /// This method is extremely lightweight (atomic load, no locking) and can be called
    /// frequently for lag detection without performance concerns.
    pub fn current_offset(&self) -> u64 {
        self.wal.current_offset()
    }

    /// Return the highest offset already accepted by the local WAL.
    pub fn last_committed_offset(&self) -> u64 {
        self.wal.last_committed_offset()
    }

    /// Return the oldest offset still retained locally in the WAL.
    ///
    /// Uses the WAL checkpoint to find the start of the oldest retained file.
    /// Returns 0 if no checkpoint is available (all data is local).
    pub async fn first_local_offset(&self) -> u64 {
        let wal_checkpoint = self.wal.current_wal_checkpoint().await;
        wal_checkpoint.map_or(0, |ckpt| ckpt.start_offset)
    }

    /// Return the earliest offset in the in-memory cache.
    ///
    /// Used during sealed recovery (hot cutover) to determine the hot tier boundary.
    pub async fn earliest_cached_offset(&self) -> Option<u64> {
        self.wal.earliest_cached_offset().await
    }

    /// Create a reader for the local WAL.
    ///
    /// If `live` is true, returns a broadcast stream that yields new messages as they arrive.
    /// If `live` is false, returns a stateful reader that replays from local WAL files + cache.
    pub async fn create_hot_reader(
        &self,
        topic_name: &str,
        from: u64,
        live: bool,
    ) -> Result<TopicStream, PersistentStorageError> {
        let stream = self.wal.tail_reader(from, live).await;
        if stream.is_ok() {
            counter!(WAL_READER_CREATE_TOTAL.name, "topic"=> topic_name.to_string(), "mode"=> "wal_only").increment(1);
        }
        stream
    }

    /// Append a single message to the local WAL and return the assigned offset.
    pub async fn append_message(
        &self,
        topic_name: &str,
        msg: StreamMessage,
    ) -> Result<u64, PersistentStorageError> {
        self.wal.set_topic_for_metrics(topic_name.to_string()).await;
        let payload_len = msg.payload.len() as u64;
        let res = self.wal.append(&msg).await;
        if let Ok(_off) = res {
            counter!(WAL_APPEND_TOTAL.name, "topic"=> topic_name.to_string()).increment(1);
            counter!(WAL_APPEND_BYTES_TOTAL.name, "topic"=> topic_name.to_string())
                .increment(payload_len);
        }
        res
    }

    /// Append a batch of messages atomically to the local WAL.
    ///
    /// Optimized: single atomic offset bump, single channel send,
    /// single cache lock, pre-encoded frames.
    pub async fn append_batch(
        &self,
        topic_name: &str,
        messages: &[StreamMessage],
    ) -> Result<(u64, u64), PersistentStorageError> {
        self.wal.set_topic_for_metrics(topic_name.to_string()).await;
        let total_bytes: u64 = messages.iter().map(|m| m.payload.len() as u64).sum();
        let count = messages.len() as u64;
        let result = self.wal.append_batch(messages).await;
        if result.is_ok() {
            counter!(WAL_APPEND_TOTAL.name, "topic" => topic_name.to_string()).increment(count);
            counter!(WAL_APPEND_BYTES_TOTAL.name, "topic" => topic_name.to_string())
                .increment(total_bytes);
        }
        result
    }

    /// Create a reader from the local WAL starting at the given position.
    pub async fn create_reader(
        &self,
        topic_name: &str,
        start: StartPosition,
    ) -> Result<TopicStream, PersistentStorageError> {
        let (start_offset, live) = match start {
            StartPosition::Latest => (self.wal.current_offset(), true),
            StartPosition::Offset(offset) => (offset, false),
        };

        info!(
            target = "wal_storage",
            start = start_offset,
            "creating reader from WAL only"
        );
        self.create_hot_reader(topic_name, start_offset, live).await
    }

    pub async fn flush(&self) -> Result<(), PersistentStorageError> {
        self.wal.flush().await
    }
}
