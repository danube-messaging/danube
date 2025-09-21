use async_trait::async_trait;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorage, PersistentStorageError, StartPosition, TopicStream};
use tokio_stream::StreamExt;

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
    pub fn new() -> Self {
        Self {
            wal: Wal::new(),
            cloud: None,
            etcd: None,
            topic_path: None,
        }
    }

    /// Construct from a pre-configured WAL (e.g., created with WalConfig::with_config())
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
        self
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
        // If cloud and etcd are configured, compute Cloud->WAL handoff watermark and chain streams.
        if let (Some(cloud), Some(etcd), Some(topic_path)) = (
            self.cloud.clone(),
            self.etcd.clone(),
            self.topic_path.clone(),
        ) {
            let start_off = match start {
                StartPosition::Latest => self.wal.current_offset().saturating_sub(1),
                StartPosition::Offset(o) => o,
            };
            // Determine the last completed object that intersects [start_off, ..]
            let reader = CloudReader::new(cloud, etcd, topic_path);
            // Fetch descriptors and compute Oend
            let from_padded = format!("{:020}", start_off);
            let descs = reader
                .etcd()
                .get_object_descriptors_range(reader.topic_path(), &from_padded, None)
                .await?;
            let mut oend: Option<u64> = None;
            for d in descs.iter() {
                if d.end_offset >= start_off {
                    oend = Some(oend.map(|x| x.max(d.end_offset)).unwrap_or(d.end_offset));
                }
            }
            // WAL min offset is unknown yet (no pruning), so assume 0 for now
            let w0 = 0u64;
            let h = match oend {
                Some(end) => std::cmp::max(w0, end.saturating_add(1)),
                None => start_off, // no cloud needed for this start
            };

            if oend.is_some() && h > start_off {
                // Cloud path needed for [start_off, h-1], then switch to WAL at h
                let cloud_stream = reader.read_range(start_off, Some(h - 1)).await?;
                let wal_stream = self.wal.tail_reader(h).await?;
                let chained = cloud_stream.chain(wal_stream);
                return Ok(Box::pin(chained));
            }
            // else fall through to WAL tail only
        }

        let from = match start {
            StartPosition::Latest => {
                // Start tailing at the current tip (inclusive): ensure the next appended
                // message (with offset == current_offset) is delivered by setting from = tip-1
                self.wal.current_offset().saturating_sub(1)
            }
            StartPosition::Offset(o) => o,
        };
        self.wal.tail_reader(from).await
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
