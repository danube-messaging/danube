mod topic_storage;
mod topic_storage_test;
use topic_storage::TopicStore;
mod errors;
pub use errors::ReliableDispatchError;
use errors::Result;
mod dispatch;
mod dispatch_test;
pub use dispatch::SubscriptionDispatch;

use danube_core::{
    dispatch_strategy::ReliableOptions,
    message::StreamMessage,
    storage::{StartPosition, TopicStream},
};
use danube_persistent_storage::WalStorage;
use dashmap::DashMap;
use std::sync::{atomic::AtomicUsize, Arc};

/// ReliableDispatch is Topic bounded message queue for reliable delivery
#[derive(Debug)]
pub struct ReliableDispatch {
    // Topic store is used to store messages in a queue for reliable delivery
    pub(crate) topic_store: TopicStore,
    // Map of subscription name to last acknowledged segment id
    pub(crate) subscriptions: Arc<DashMap<String, Arc<AtomicUsize>>>,
    // Channel to send shutdown signal to the lifecycle management task
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
}

impl ReliableDispatch {
    /// Construct a ReliableDispatch using a provided WalStorage (WAL + Cloud path).
    pub fn new(
        topic_name: &str,
        reliable_options: ReliableOptions,
        wal_storage: WalStorage,
    ) -> Self {
        let subscriptions: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(DashMap::new());
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let subscriptions_cloned = Arc::clone(&subscriptions);

        let topic_store = TopicStore::new(&topic_name, reliable_options, wal_storage);
        // Lifecycle management task removed in WAL-only path
        Self {
            topic_store,
            subscriptions,
            shutdown_tx,
        }
    }

    /// Backward-compat alias to construct with an explicitly provided WalStorage
    pub fn new_with_persistent(
        topic_name: &str,
        reliable_options: ReliableOptions,
        wal_storage: WalStorage,
    ) -> Self {
        Self::new(topic_name, reliable_options, wal_storage)
    }

    pub async fn new_subscription_dispatch(
        &self,
        subscription_name: &str,
    ) -> Result<SubscriptionDispatch> {
        // For WAL-only streaming, we no longer need the last_acked_segment here.
        // SubscriptionDispatch maintains its own pending ack state while streaming.
        let subscription_dispatch = SubscriptionDispatch::new(self.topic_store.clone());

        //self.subscription_dispatch.insert(subscription_name.to_string(), subscription_name.to_string());

        Ok(subscription_dispatch)
    }

    pub async fn store_message(&self, message: StreamMessage) -> Result<()> {
        self.topic_store.store_message(message).await?;
        Ok(())
    }

    pub async fn add_subscription(&self, subscription_name: &str) -> Result<()> {
        self.subscriptions
            .insert(subscription_name.to_string(), Arc::new(AtomicUsize::new(0)));
        Ok(())
    }

    pub async fn get_last_acknowledged_segment(
        &self,
        subscription_name: &str,
    ) -> Result<Arc<AtomicUsize>> {
        match self.subscriptions.get(subscription_name) {
            Some(subscription) => Ok(Arc::clone(subscription.value())),
            None => Err(ReliableDispatchError::SubscriptionError(
                "Subscription not found".to_string(),
            )),
        }
    }

    /// Create a TopicStream for this topic starting from the Latest position.
    /// This is backed by the new WAL path when enabled; otherwise it will be empty.
    pub async fn create_stream_latest(&self) -> Result<TopicStream> {
        self.topic_store
            .create_reader(StartPosition::Latest)
            .await
            .map_err(|e| ReliableDispatchError::StorageError(e.to_string()))
    }
}

impl Drop for ReliableDispatch {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.try_send(());
    }
}
