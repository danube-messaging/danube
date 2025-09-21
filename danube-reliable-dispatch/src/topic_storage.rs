use crate::errors::{ReliableDispatchError, Result};
use danube_core::{
    dispatch_strategy::ReliableOptions,
    message::StreamMessage,
    storage::{PersistentStorage, StartPosition, TopicStream},
};
use danube_persistent_storage::WalStorage;

// TopicStore is used only for reliable messaging (WAL-only)
#[derive(Debug, Clone)]
pub(crate) struct TopicStore {
    topic_name: String,
    // WAL-first persistent storage (with optional CloudReader handoff inside)
    persistent: WalStorage,
}

impl TopicStore {
    pub(crate) fn new(
        topic_name: &str,
        _reliable_options: ReliableOptions,
        wal_storage: WalStorage,
    ) -> Self {
        Self {
            topic_name: topic_name.to_string(),
            persistent: wal_storage,
        }
    }

    pub(crate) async fn store_message(&self, message: StreamMessage) -> Result<()> {
        self.persistent
            .append_message(&self.topic_name, message)
            .await
            .map_err(|e| ReliableDispatchError::StorageError(e.to_string()))?;
        Ok(())
    }

    /// Enable WAL-first persistent storage for this topic store.
    pub(crate) fn use_persistent_storage(&mut self, storage: WalStorage) {
        self.persistent = storage;
    }

    /// Create a streaming reader starting from the given position.
    pub(crate) async fn create_reader(
        &self,
        start: StartPosition,
    ) -> std::result::Result<TopicStream, ReliableDispatchError> {
        let stream = self
            .persistent
            .create_reader(&self.topic_name, start)
            .await
            .map_err(|e| ReliableDispatchError::StorageError(e.to_string()))?;
        Ok(stream)
    }
}
