use crate::persistent_metrics::{
    CLOUD_HANDOFF_TO_WAL_TOTAL, WAL_APPEND_BYTES_TOTAL, WAL_APPEND_TOTAL, WAL_READER_CREATE_TOTAL,
};
use async_trait::async_trait;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorage, PersistentStorageError, StartPosition, TopicStream};
use metrics::counter;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::cloud::{CloudReader, CloudStore};
use crate::hot_log::HotLog;
use crate::storage_metadata::StorageMetadata;

#[derive(Debug, Default, Clone)]
pub struct WalStorage {
    hot_log: HotLog,
    durable_store: Option<CloudStore>,
    metadata: Option<StorageMetadata>,
    topic_path: Option<String>,
}

impl WalStorage {
    pub fn from_hot_log(hot_log: HotLog) -> Self {
        Self {
            hot_log,
            durable_store: None,
            metadata: None,
            topic_path: None,
        }
    }

    pub fn from_wal(wal: crate::wal::Wal) -> Self {
        Self::from_hot_log(wal.into())
    }

    /// Enable durable historical reads by wiring CloudStore + StorageMetadata and logical topic path.
    pub(crate) fn with_durable_history(
        mut self,
        durable_store: CloudStore,
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
                    "cloud is not configured; creating reader from WAL only"
                );
                let stream = self.hot_log.tail_reader(from, live).await;
                if stream.is_ok() {
                    counter!(WAL_READER_CREATE_TOTAL.name, "topic"=> topic_name.to_string(), "mode"=> "wal_only").increment(1);
                }
                return stream;
            }
        };

        let (start_offset, live) = match start {
            StartPosition::Latest => (self.hot_log.current_offset(), true),
            StartPosition::Offset(o) => (o, false),
        };

        let wal_checkpoint = self.hot_log.current_wal_checkpoint().await;
        let wal_start_offset = wal_checkpoint.map_or(0, |ckpt| ckpt.start_offset);

        if start_offset >= wal_start_offset {
            info!(
                target = "wal_storage",
                topic = %topic_path,
                start = start_offset,
                wal_start = wal_start_offset,
                "creating reader from WAL only (request is within local retention)"
            );
            let stream = self.hot_log.tail_reader(start_offset, live).await;
            if stream.is_ok() {
                counter!(WAL_READER_CREATE_TOTAL.name, "topic"=> topic_name.to_string(), "mode"=> "wal_only").increment(1);
            }
            stream
        } else {
            let handoff_offset = wal_start_offset;
            info!(
                target = "wal_storage",
                topic = %topic_path,
                start = start_offset,
                handoff = handoff_offset,
                "creating reader with Cloud->WAL chaining"
            );

            let reader = CloudReader::new(durable_store, metadata, topic_path.clone());
            let cloud_stream = reader
                .read_range(start_offset, Some(handoff_offset - 1))
                .await?;

            let wal_stream = self.hot_log.tail_reader(handoff_offset, false).await?;

            let chained = cloud_stream.chain(wal_stream);
            counter!(WAL_READER_CREATE_TOTAL.name, "topic"=> topic_name.to_string(), "mode"=> "cloud_then_wal").increment(1);
            counter!(CLOUD_HANDOFF_TO_WAL_TOTAL.name, "topic"=> topic_name.to_string())
                .increment(1);
            Ok(Box::pin(chained))
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
