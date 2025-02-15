mod topic_storage;
mod topic_storage_test;
pub use storage_backend::create_message_storage;
use topic_storage::TopicStore;
mod errors;
pub use errors::ReliableDispatchError;
use errors::Result;
mod dispatch;
mod dispatch_test;
pub use dispatch::SubscriptionDispatch;
mod storage_backend;
mod topic_cache;
pub use topic_cache::TopicCache;

use danube_core::{dispatch_strategy::ReliableOptions, message::StreamMessage};
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
    pub fn new(
        topic_name: &str,
        reliable_options: ReliableOptions,
        topic_cache: TopicCache,
    ) -> Self {
        let subscriptions: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(DashMap::new());
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let subscriptions_cloned = Arc::clone(&subscriptions);

        let retention_policy = reliable_options.retention_policy.clone();
        let topic_store = TopicStore::new(&topic_name, topic_cache, reliable_options);
        // Start the lifecycle management task
        topic_store.start_lifecycle_management_task(
            shutdown_rx,
            subscriptions_cloned,
            retention_policy,
        );

        Self {
            topic_store,
            subscriptions,
            shutdown_tx,
        }
    }

    pub async fn new_subscription_dispatch(
        &self,
        subscription_name: &str,
    ) -> Result<SubscriptionDispatch> {
        let sub_last_acked_segment = self
            .get_last_acknowledged_segment(subscription_name)
            .await?;

        let subscription_dispatch =
            SubscriptionDispatch::new(self.topic_store.clone(), sub_last_acked_segment);

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
}

impl Drop for ReliableDispatch {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.try_send(());
    }
}
