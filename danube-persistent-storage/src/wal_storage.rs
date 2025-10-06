use async_trait::async_trait;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorage, PersistentStorageError, StartPosition, TopicStream};
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::cloud_reader::CloudReader;
use crate::cloud_store::CloudStore;
use crate::etcd_metadata::EtcdMetadata;
use crate::wal::Wal;

#[derive(Debug, Default, Clone)]
pub struct WalStorage {
    wal: Wal,
    cloud: Option<CloudStore>,
    etcd: Option<EtcdMetadata>,
    topic_path: Option<String>,
}

impl WalStorage {
    /// Construct from a pre-configured WAL
    pub fn from_wal(wal: Wal) -> Self {
        Self {
            wal,
            cloud: None,
            etcd: None,
            topic_path: None,
        }
    }

    /// Enable cloud historical reads by wiring CloudStore + EtcdMetadata and logical topic path.
    pub fn with_cloud(mut self, cloud: CloudStore, etcd: EtcdMetadata, topic_path: String) -> Self {
        self.cloud = Some(cloud);
        self.etcd = Some(etcd);
        self.topic_path = Some(topic_path);
        if let Some(tp) = &self.topic_path {
            info!(target = "wal_storage", topic = %tp, "cloud handoff enabled for topic");
        }
        self
    }

    /// Convenience: append a message directly to the underlying WAL.
    ///
    /// Note: Integration tests use `storage.append(&msg)`; this helper forwards to `Wal::append`.
    #[allow(dead_code)]
    pub(crate) async fn append(&self, msg: &StreamMessage) -> Result<u64, PersistentStorageError> {
        self.wal.append(msg).await
    }
}

#[async_trait]
impl PersistentStorage for WalStorage {
    async fn append_message(
        &self,
        _topic_name: &str,
        msg: StreamMessage,
    ) -> Result<u64, PersistentStorageError> {
        self.wal.append(&msg).await
    }

    async fn create_reader(
        &self,
        _topic_name: &str,
        start: StartPosition,
    ) -> Result<TopicStream, PersistentStorageError> {
        // This function requires cloud and etcd to be configured for the tiered reading logic.
        let (cloud, etcd, topic_path) = match (
            self.cloud.as_ref(),
            self.etcd.as_ref(),
            self.topic_path.as_ref(),
        ) {
            (Some(c), Some(e), Some(tp)) => (c.clone(), e.clone(), tp.clone()),
            _ => {
                // Fallback to WAL-only behavior if cloud integration is not configured.
                let (from, live) = match start {
                    StartPosition::Latest => (self.wal.current_offset(), true),
                    StartPosition::Offset(o) => (o, false),
                };
                warn!(
                    target = "wal_storage",
                    start = from,
                    "cloud is not configured; creating reader from WAL only"
                );
                return self.wal.tail_reader(from, live).await;
            }
        };

        // 1. Determine the concrete start offset and whether we are in live mode.
        let (start_offset, live) = match start {
            StartPosition::Latest => (self.wal.current_offset(), true),
            StartPosition::Offset(o) => (o, false),
        };

        // 2. Get the WAL's start offset from its checkpoint. This tells us the oldest
        // offset available in the local WAL files.
        let wal_checkpoint = self.wal.current_wal_checkpoint().await;
        let wal_start_offset = wal_checkpoint.map_or(0, |ckpt| ckpt.start_offset);

        // 3. Implement the tiered reading logic.
        if start_offset >= wal_start_offset {
            // CASE 1: The entire read can be served from the local WAL.
            // This is the most efficient path for active consumers.
            info!(
                target = "wal_storage",
                topic = %topic_path,
                start = start_offset,
                wal_start = wal_start_offset,
                "creating reader from WAL only (request is within local retention)"
            );
            self.wal.tail_reader(start_offset, live).await
        } else {
            // CASE 2: The read must start from the cloud and then hand off to the WAL.
            let handoff_offset = wal_start_offset;
            info!(
                target = "wal_storage",
                topic = %topic_path,
                start = start_offset,
                handoff = handoff_offset,
                "creating reader with Cloud->WAL chaining"
            );

            // Create a stream for the historical data from the cloud.
            // This will read from start_offset up to (but not including) handoff_offset.
            let reader = CloudReader::new(cloud, etcd, topic_path.clone());
            let cloud_stream = reader
                .read_range(start_offset, Some(handoff_offset - 1))
                .await?;

            // Create a stream for the recent data from the WAL, starting at the handoff point.
            let wal_stream = self.wal.tail_reader(handoff_offset, false).await?;

            // Chain them together to provide a single, seamless stream to the consumer.
            let chained = cloud_stream.chain(wal_stream);
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
        Ok(())
    }
}
