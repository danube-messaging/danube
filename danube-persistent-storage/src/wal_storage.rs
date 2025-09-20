use async_trait::async_trait;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorage, PersistentStorageError, StartPosition, TopicStream};

use crate::wal::Wal;

#[derive(Debug, Default, Clone)]
pub struct WalStorage {
    wal: Wal,
}

impl WalStorage {
    pub fn new() -> Self {
        Self { wal: Wal::new() }
    }

    /// Construct from a pre-configured WAL (e.g., created with WalConfig::with_config())
    pub fn from_wal(wal: Wal) -> Self {
        Self { wal }
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
