use crate::wal::{Wal, WalCheckpoint, WalConfig};
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};

#[derive(Debug, Clone, Default)]
pub struct HotLog {
    inner: Wal,
}

impl HotLog {
    pub fn new(inner: Wal) -> Self {
        Self { inner }
    }

    pub fn config_type() -> &'static str {
        std::any::type_name::<WalConfig>()
    }

    pub fn current_offset(&self) -> u64 {
        self.inner.current_offset()
    }

    pub fn last_committed_offset(&self) -> u64 {
        self.current_offset().saturating_sub(1)
    }

    pub async fn append(&self, msg: &StreamMessage) -> Result<u64, PersistentStorageError> {
        self.inner.append(msg).await
    }

    pub async fn flush(&self) -> Result<(), PersistentStorageError> {
        self.inner.flush().await
    }

    pub async fn current_wal_checkpoint(&self) -> Option<WalCheckpoint> {
        self.inner.current_wal_checkpoint().await
    }

    pub async fn set_topic_for_metrics(&self, topic_name: String) {
        self.inner.set_topic_for_metrics(topic_name).await
    }

    pub async fn tail_reader(
        &self,
        from: u64,
        live: bool,
    ) -> Result<TopicStream, PersistentStorageError> {
        self.inner.tail_reader(from, live).await
    }

    pub fn wal(&self) -> &Wal {
        &self.inner
    }

    pub fn into_inner(self) -> Wal {
        self.inner
    }
}

impl From<Wal> for HotLog {
    fn from(value: Wal) -> Self {
        Self::new(value)
    }
}
