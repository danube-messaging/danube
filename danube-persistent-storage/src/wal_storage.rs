use crate::persistent_metrics::{
    DURABLE_HISTORY_TO_HOT_TOTAL, WAL_APPEND_BYTES_TOTAL, WAL_APPEND_TOTAL,
    WAL_READER_CREATE_TOTAL,
};
use async_trait::async_trait;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorage, PersistentStorageError, StartPosition, TopicStream};
use metrics::counter;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::durable_history_reader::DurableHistoryReader;
use crate::durable_store::DurableStore;
use crate::hot_log::HotLog;
use crate::metadata::StorageMetadata;

#[derive(Debug, Default, Clone)]
pub struct WalStorage {
    hot_log: HotLog,
    durable_store: Option<Arc<dyn DurableStore>>,
    metadata: Option<StorageMetadata>,
    topic_path: Option<String>,
    history_cutover_from_hot: bool,
}

impl WalStorage {
    pub fn from_hot_log(hot_log: HotLog) -> Self {
        Self {
            hot_log,
            durable_store: None,
            metadata: None,
            topic_path: None,
            history_cutover_from_hot: false,
        }
    }

    pub fn from_wal(wal: crate::wal::Wal) -> Self {
        Self::from_hot_log(wal.into())
    }

    /// Enable durable historical reads by wiring DurableStore + StorageMetadata and logical topic path.
    pub(crate) fn with_durable_history(
        mut self,
        durable_store: Arc<dyn DurableStore>,
        metadata: StorageMetadata,
        topic_path: String,
    ) -> Self {
        self.durable_store = Some(durable_store);
        self.metadata = Some(metadata);
        self.topic_path = Some(topic_path);
        if let Some(tp) = &self.topic_path {
            info!(target = "wal_storage", topic = %tp, "durable history enabled for topic");
        }
        self
    }

    pub(crate) fn with_hot_cutover(mut self) -> Self {
        self.history_cutover_from_hot = true;
        self
    }

    /// Returns the current committed offset (head of the log).
    /// This is the offset that will be assigned to the NEXT message.
    ///
    /// This method is extremely lightweight (atomic load, no locking) and can be called
    /// frequently for lag detection without performance concerns.
    pub fn current_offset(&self) -> u64 {
        self.hot_log.current_offset()
    }

    /// Convenience: append a message directly to the underlying WAL.
    ///
    /// Note: Integration tests use `storage.append(&msg)`; this helper forwards to `Wal::append`.
    #[allow(dead_code)]
    pub(crate) async fn append(&self, msg: &StreamMessage) -> Result<u64, PersistentStorageError> {
        self.hot_log.append(msg).await
    }

    async fn create_hot_reader(
        &self,
        topic_name: &str,
        from: u64,
        live: bool,
    ) -> Result<TopicStream, PersistentStorageError> {
        let stream = self.hot_log.tail_reader(from, live).await;
        if stream.is_ok() {
            counter!(WAL_READER_CREATE_TOTAL.name, "topic"=> topic_name.to_string(), "mode"=> "wal_only").increment(1);
        }
        stream
    }

    async fn hot_start_offset(&self) -> u64 {
        if self.history_cutover_from_hot {
            self.hot_log
                .earliest_cached_offset()
                .await
                .unwrap_or_else(|| self.hot_log.current_offset())
        } else {
            let wal_checkpoint = self.hot_log.current_wal_checkpoint().await;
            wal_checkpoint.map_or(0, |ckpt| ckpt.start_offset)
        }
    }

    async fn create_durable_history_reader(
        &self,
        topic_name: &str,
        durable_store: Arc<dyn DurableStore>,
        metadata: StorageMetadata,
        topic_path: String,
        start_offset: u64,
        hot_start_offset: u64,
    ) -> Result<TopicStream, PersistentStorageError> {
        let reader = DurableHistoryReader::new(durable_store, metadata, topic_path);
        let durable_stream = reader
            .read_range(start_offset, Some(hot_start_offset - 1))
            .await?;
        let hot_stream = self.hot_log.tail_reader(hot_start_offset, false).await?;
        let chained = durable_stream.chain(hot_stream);
        counter!(WAL_READER_CREATE_TOTAL.name, "topic"=> topic_name.to_string(), "mode"=> "durable_history_then_hot").increment(1);
        counter!(DURABLE_HISTORY_TO_HOT_TOTAL.name, "topic"=> topic_name.to_string())
            .increment(1);
        Ok(Box::pin(chained))
    }
}

#[async_trait]
impl PersistentStorage for WalStorage {
    async fn append_message(
        &self,
        topic_name: &str,
        msg: StreamMessage,
    ) -> Result<u64, PersistentStorageError> {
        self.hot_log.set_topic_for_metrics(topic_name.to_string()).await;
        let payload_len = msg.payload.len() as u64;
        let res = self.hot_log.append(&msg).await;
        if let Ok(_off) = res {
            counter!(WAL_APPEND_TOTAL.name, "topic"=> topic_name.to_string()).increment(1);
            counter!(WAL_APPEND_BYTES_TOTAL.name, "topic"=> topic_name.to_string())
                .increment(payload_len);
        }
        res
    }

    async fn create_reader(
        &self,
        topic_name: &str,
        start: StartPosition,
    ) -> Result<TopicStream, PersistentStorageError> {
        // This function requires cloud and metadata to be configured for the tiered reading logic.
        let (durable_store, metadata, topic_path) = match (
            self.durable_store.as_ref(),
            self.metadata.as_ref(),
            self.topic_path.as_ref(),
        ) {
            (Some(c), Some(e), Some(tp)) => (c.clone(), e.clone(), tp.clone()),
            _ => {
                let (from, live) = match start {
                    StartPosition::Latest => (self.hot_log.current_offset(), true),
                    StartPosition::Offset(o) => (o, false),
                };
                warn!(
                    target = "wal_storage",
                    start = from,
                    "durable history is not configured; creating reader from WAL only"
                );
                return self.create_hot_reader(topic_name, from, live).await;
            }
        };

        let (start_offset, live) = match start {
            StartPosition::Latest => (self.hot_log.current_offset(), true),
            StartPosition::Offset(o) => (o, false),
        };

        let hot_start_offset = self.hot_start_offset().await;

        if start_offset >= hot_start_offset {
            info!(
                target = "wal_storage",
                topic = %topic_path,
                start = start_offset,
                hot_start = hot_start_offset,
                "creating reader from WAL only (request is within local retention)"
            );
            self.create_hot_reader(topic_name, start_offset, live).await
        } else {
            info!(
                target = "wal_storage",
                topic = %topic_path,
                start = start_offset,
                hot_start = hot_start_offset,
                "creating reader from durable history plus hot state"
            );
            self.create_durable_history_reader(
                topic_name,
                durable_store,
                metadata,
                topic_path,
                start_offset,
                hot_start_offset,
            )
            .await
        }
    }

    async fn ack_checkpoint(
        &self,
        _topic_name: &str,
        _up_to_offset: u64,
    ) -> Result<(), PersistentStorageError> {
        Ok(())
    }

    async fn flush(&self, _topic_name: &str) -> Result<(), PersistentStorageError> {
        self.hot_log.flush().await
    }
}
