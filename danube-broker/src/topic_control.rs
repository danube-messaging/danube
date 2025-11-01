use std::sync::Arc;

use anyhow::{anyhow, Result};
use dashmap::DashMap;
use metrics::gauge;
use tracing::{error, warn};

use crate::broker_metrics::{TOPIC_CONSUMERS, TOPIC_PRODUCERS};
use crate::resources::BASE_TOPICS_PATH;
use crate::utils::join_path;
use crate::{
    broker_metrics::BROKER_TOPICS,
    resources::Resources,
    schema::SchemaType,
    subscription::{ConsumerInfo, SubscriptionOptions},
    topic::Topic,
    topic_worker::TopicWorkerPool,
};
use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_core::message::StreamMessage;
use danube_persistent_storage::WalStorageFactory;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

/// Manages topics and their associated producers and consumers.
#[derive(Debug, Clone)]
pub(crate) struct TopicManager {
    /// Broker identifier for metrics and logging.
    pub(crate) broker_id: u64,
    /// Worker pool handling per-topic execution and routing.
    pub(crate) topic_worker_pool: Arc<TopicWorkerPool>,
    /// Factory for building per-topic WAL storage (reliable mode).
    pub(crate) wal_factory: WalStorageFactory,
    /// Shared access to Local Cache-backed resources.
    pub(crate) resources: Arc<Mutex<Resources>>,
    /// Index of producer_id -> topic_name.
    pub(crate) producers: ProducerRegistry,
    /// Index of consumer_id -> (topic_name, subscription_name).
    pub(crate) consumers: ConsumerRegistry,
}

impl TopicManager {
    /// Constructs a new TopicManager bound to this broker's worker pool and resources.
    pub(crate) fn new(
        broker_id: u64,
        topic_worker_pool: Arc<TopicWorkerPool>,
        wal_factory: WalStorageFactory,
        resources: Arc<Mutex<Resources>>,
        producers: ProducerRegistry,
        consumers: ConsumerRegistry,
    ) -> Self {
        Self {
            broker_id,
            topic_worker_pool,
            wal_factory,
            resources,
            producers,
            consumers,
        }
    }

    /// Ensures a topic exists locally by materializing it from Local Cache metadata.
    /// Returns the configured dispatch strategy and schema type.
    pub(crate) async fn ensure_local(
        &self,
        topic_name: &str,
    ) -> Result<(ConfigDispatchStrategy, SchemaType)> {
        //get retention strategy from local_cache
        let dispatch_strategy = {
            let resources = self.resources.lock().await;
            resources.topic.get_dispatch_strategy(topic_name)
        };
        if dispatch_strategy.is_none() {
            return Err(anyhow!(
                "Unable to create topic without a valid dispatch strategy"
            ));
        }

        let dispatch_strategy = dispatch_strategy.unwrap();

        // Build per-topic WalStorage with Cloud handoff via the factory
        let wal_storage = if dispatch_strategy == ConfigDispatchStrategy::Reliable {
            Some(self.wal_factory.for_topic(topic_name).await?)
        } else {
            None
        };

        let mut new_topic = Topic::new(
            topic_name,
            dispatch_strategy.clone(),
            wal_storage,
            {
                let resources = self.resources.lock().await;
                resources.topic.clone()
            },
        );

        let schema = {
            let resources = self.resources.lock().await;
            resources.topic.get_schema(topic_name)
        };
        if schema.is_none() {
            return Err(anyhow!("Unable to create topic without a valid schema"));
        }
        let schema = schema.unwrap();
        let _ = new_topic.add_schema(schema.clone());

        // get policies from local_cache
        let policies = {
            let resources = self.resources.lock().await;
            resources.topic.get_policies(topic_name)
        };

        if let Some(with_policies) = policies {
            let _ = new_topic.policies_update(with_policies);
        } else {
            // get namespace policies
            let parts: Vec<_> = topic_name.split('/').collect();
            let ns_name = format!("/{}", parts[1]);

            let ns_policies = {
                let resources = self.resources.lock().await;
                resources.namespace.get_policies(&ns_name)
            };
            if let Ok(ns_policies) = ns_policies {
                let _ = new_topic.policies_update(ns_policies);
            }
        }

        // Wrap topic in Arc for concurrent access
        let new_topic_arc = Arc::new(new_topic);

        // Add topic to worker pool (single source of truth)
        self.topic_worker_pool
            .add_topic_to_worker(topic_name.to_string(), new_topic_arc);

        gauge!(BROKER_TOPICS.name, "broker" => self.broker_id.to_string()).increment(1);

        Ok((dispatch_strategy, schema.type_schema))
    }

    /// Flush and seal persistent storage (WAL/uploader/deleter).
    pub(crate) async fn flush_and_seal(&self, topic_name: &str) -> Result<()> {
        self.wal_factory
            .flush_and_seal(topic_name, self.broker_id)
            .await
            .map(|_| ())
            .map_err(|e| anyhow!("flush_and_seal failed: {}", e))
    }

    /// Delete ETCD storage metadata for a reliable topic.
    pub(crate) async fn delete_storage_metadata(&self, topic_name: &str) -> Result<()> {
        self.wal_factory
            .delete_storage_metadata(topic_name)
            .await
            .map_err(|e| anyhow!("delete_storage_metadata failed: {}", e))
    }

    /// Removes a topic from this broker, disconnects producers/consumers, and updates indices.
    pub(crate) async fn delete_local(&self, topic_name: &str) -> Result<Arc<Topic>> {
        // First check if topic exists
        if !self.topic_worker_pool.contains_topic(topic_name) {
            return Err(anyhow!(
                "The topic {} does not exist on the broker {}",
                topic_name,
                self.broker_id
            ));
        }

        // Mark topic as unavailable to reject new publishes during deletion
        if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
            topic.unavailable_topic().await;
        }

        // If reliable, ensure persistent storage is sealed and storage metadata removed
        let is_reliable = {
            let resources = self.resources.lock().await;
            resources
                .topic
                .get_dispatch_strategy(topic_name)
                .map(|ds| matches!(ds, ConfigDispatchStrategy::Reliable))
                .unwrap_or(false)
        };
        if is_reliable {
            if let Err(e) = self.flush_and_seal(topic_name).await {
                error!(
                    "flush_and_seal failed during delete for {}: {}",
                    topic_name, e
                );
            }
            if let Err(e) = self.delete_storage_metadata(topic_name).await {
                error!(
                    "delete_storage_metadata failed during delete for {}: {}",
                    topic_name, e
                );
            }

            // Remove subscription cursors under /topics/<ns>/<topic>/subscriptions/*/cursor
            let subs = {
                let resources = self.resources.lock().await;
                resources.topic.get_subscription_for_topic(topic_name).await
            };
            let mut resources = self.resources.lock().await;
            // Build correct path: /topics/<ns>/<topic>/subscriptions/<sub>/cursor
            let trimmed = topic_name.trim_start_matches('/');
            let mut parts = trimmed.split('/');
            let ns = parts.next().unwrap_or("");
            let topic = parts.next().unwrap_or("");
            for sub in subs {
                let cursor_path = join_path(&[
                    BASE_TOPICS_PATH,
                    ns,
                    topic,
                    "subscriptions",
                    &sub,
                    "cursor",
                ]);
                if let Err(e) = resources.topic.delete(&cursor_path).await {
                    warn!(
                        "Failed to delete reliable cursor for {}/{} subscription {} at {}: {}",
                        ns,
                        topic,
                        sub,
                        cursor_path,
                        e
                    );
                }
            }
        }

        // Remove from worker pool (single source of truth) to prevent new operations
        let topic = self
            .topic_worker_pool
            .remove_topic_from_worker(topic_name)
            .ok_or_else(|| anyhow!("Failed to remove topic from worker pool"))?;

        // Disconnect all attached producers/consumers and clean indices
        let (producers, consumers) = topic.close().await?;

        for producer_id in producers {
            self.producers.remove(&producer_id);
        }
        for consumer_id in consumers {
            self.consumers.remove(&consumer_id);
        }

        gauge!(BROKER_TOPICS.name, "broker" => self.broker_id.to_string()).decrement(1);

        Ok(topic)
    }

    /// Unload a topic from this broker, preserving required metadata so it can be reassigned.
    /// - Non-reliable: keep delivery, schema, namespace; delete producer metadata (optionally subscriptions)
    /// - Reliable: flush cursors, seal storage, delete producer metadata; keep subscriptions/cursors/storage
    pub(crate) async fn unload_topic(&self, topic_name: &str) -> Result<()> {
        if !self.topic_worker_pool.contains_topic(topic_name) {
            return Err(anyhow!(
                "The topic {} does not exist on the broker {}",
                topic_name, self.broker_id
            ));
        }

        let is_reliable = {
            let resources = self.resources.lock().await;
            resources
                .topic
                .get_dispatch_strategy(topic_name)
                .map(|ds| matches!(ds, ConfigDispatchStrategy::Reliable))
                .unwrap_or(false)
        };

        if is_reliable {
            self.unload_reliable_topic(topic_name).await
        } else {
            self.unload_non_reliable_topic(topic_name).await
        }
    }

    async fn unload_non_reliable_topic(&self, topic_name: &str) -> Result<()> {
        if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
            topic.unavailable_topic().await;
        }

        let topic = self
            .topic_worker_pool
            .remove_topic_from_worker(topic_name)
            .ok_or_else(|| anyhow!("Failed to remove topic from worker pool"))?;
        let (producers, consumers) = topic.close().await?;

        for producer_id in producers {
            self.producers.remove(&producer_id);
        }
        for consumer_id in consumers {
            self.consumers.remove(&consumer_id);
        }

        // Delete producer metadata; keep delivery/schema/namespace; optionally cleanup subscriptions
        {
            let mut resources = self.resources.lock().await;
            let _ = resources.topic.delete_all_producers(topic_name).await;
            let _ = resources.topic.delete_all_subscriptions(topic_name).await;
        }

        gauge!(BROKER_TOPICS.name, "broker" => self.broker_id.to_string()).decrement(1);
        Ok(())
    }

    async fn unload_reliable_topic(&self, topic_name: &str) -> Result<()> {
        if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
            topic.unavailable_topic().await;
        }

        let _ = self.flush_subscription_cursors(topic_name).await;
        self.flush_and_seal(topic_name).await?;

        let topic = self
            .topic_worker_pool
            .remove_topic_from_worker(topic_name)
            .ok_or_else(|| anyhow!("Failed to remove topic from worker pool"))?;
        let (producers, consumers) = topic.close().await?;

        for producer_id in producers {
            self.producers.remove(&producer_id);
        }
        for consumer_id in consumers {
            self.consumers.remove(&consumer_id);
        }

        // Delete only producer metadata
        {
            let mut resources = self.resources.lock().await;
            let _ = resources.topic.delete_all_producers(topic_name).await;
        }

        gauge!(BROKER_TOPICS.name, "broker" => self.broker_id.to_string()).decrement(1);
        Ok(())
    }

    /// Best-effort flush of all subscription cursors for a topic (reliable only).
    async fn flush_subscription_cursors(&self, topic_name: &str) -> Result<()> {
        if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
            let subscriptions = topic.subscriptions.lock().await;
            for (_sub_name, subscription) in subscriptions.iter() {
                if let Some(dispatcher) = &subscription.dispatcher {
                    let _ = dispatcher.flush_progress_now().await;
                }
            }
        }
        Ok(())
    }

    //======================== Producer Ops ========================
    /// Creates and registers a producer on the given topic and updates metadata/metrics.
    pub(crate) async fn create_producer(
        &self,
        producer_name: &str,
        producer_id: u64,
        producer_access_mode: i32,
        topic_name: &str,
    ) -> Result<u64> {
        if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
            let producer_config = topic
                .create_producer(producer_id, producer_name, producer_access_mode)
                .await?;

            self.producers.insert(producer_id, topic_name.to_string());

            gauge!(TOPIC_PRODUCERS.name, "topic" => topic_name.to_string()).increment(1);

            {
                let mut resources = self.resources.lock().await;
                resources
                    .topic
                    .create_producer(producer_id, topic_name, producer_config)
                    .await?;
            }
        } else {
            return Err(anyhow!("Unable to find the topic: {}", topic_name));
        }

        Ok(producer_id)
    }

    /// Locates the topic managed by `producer_id`, if available.
    pub(crate) fn find_topic_by_producer(&self, producer_id: u64) -> Option<Arc<Topic>> {
        if let Some(topic_name) = self.producers.get_topic(producer_id) {
            self.topic_worker_pool.get_topic(&topic_name)
        } else {
            None
        }
    }

    /// Returns true if the producer is healthy according to its topic state.
    pub(crate) async fn health_producer(&self, producer_id: u64) -> bool {
        if let Some(topic) = self.find_topic_by_producer(producer_id) {
            return topic.get_producer_status(producer_id).await;
        }
        false
    }

    //======================== Consumer Ops ========================
    /// Subscribes to a topic, registers the consumer, persists subscription and updates metrics.
    pub(crate) async fn subscribe(
        &self,
        topic_name: String,
        subscription_options: SubscriptionOptions,
    ) -> Result<u64> {
        let consumer_id = self
            .topic_worker_pool
            .subscribe_async(topic_name.clone(), subscription_options.clone())
            .await?;

        let sub_name_clone = subscription_options.subscription_name.clone();
        self.consumers
            .insert(consumer_id, topic_name.clone(), sub_name_clone);

        gauge!(TOPIC_CONSUMERS.name, "topic" => topic_name.clone()).increment(1);

        let sub_options = serde_json::to_value(&subscription_options)?;
        {
            let mut resources = self.resources.lock().await;
            resources
                .topic
                .create_subscription(
                    &subscription_options.subscription_name,
                    &topic_name,
                    sub_options,
                )
                .await?;
        }

        Ok(consumer_id)
    }

    /// Finds consumer info by id via the consumer index and topic subscriptions.
    pub(crate) async fn find_consumer_by_id(&self, consumer_id: u64) -> Option<ConsumerInfo> {
        if let Some((topic_name, subscription_name)) = self.consumers.get(consumer_id) {
            if let Some(topic) = self.topic_worker_pool.get_topic(&topic_name) {
                if let Some(subscription) = topic.subscriptions.lock().await.get(&subscription_name)
                {
                    return subscription.get_consumer_info(consumer_id);
                }
            }
        }
        None
    }

    /// Finds consumer info and its receiver channel by id.
    pub(crate) async fn find_consumer_and_rx(
        &self,
        consumer_id: u64,
    ) -> Option<(ConsumerInfo, Arc<Mutex<mpsc::Receiver<StreamMessage>>>)> {
        if let Some((topic_name, subscription_name)) = self.consumers.get(consumer_id) {
            if let Some(topic) = self.topic_worker_pool.get_topic(&topic_name) {
                if let Some(subscription) = topic.subscriptions.lock().await.get(&subscription_name)
                {
                    let info = subscription.get_consumer_info(consumer_id)?;
                    let rx = subscription.get_consumer_rx(consumer_id)?;
                    return Some((info, rx));
                }
            }
        }
        None
    }

    /// Returns true if the consumer is healthy according to its subscription state.
    pub(crate) async fn health_consumer(&self, consumer_id: u64) -> bool {
        if let Some(consumer) = self.find_consumer_by_id(consumer_id).await {
            return consumer.get_status().await;
        }
        false
    }

    /// Returns the consumer_id if a consumer with the given name exists on the subscription.
    pub(crate) async fn check_if_consumer_exist(
        &self,
        consumer_name: &str,
        subscription_name: &str,
        topic_name: &str,
    ) -> Option<u64> {
        let topic = self.topic_worker_pool.get_topic(topic_name)?;
        topic
            .validate_consumer(subscription_name, consumer_name)
            .await
    }

    /// Signals subscription dispatch to resume when a consumer reconnects (reliable mode).
    pub(crate) async fn trigger_dispatcher_on_reconnect(&self, consumer_id: u64) {
        if let Some((topic_name, subscription_name)) = self.consumers.get(consumer_id) {
            if let Some(topic) = self.topic_worker_pool.get_topic(&topic_name) {
                let subscriptions = topic.subscriptions.lock().await;
                if let Some(subscription) = subscriptions.get(&subscription_name) {
                    if let Some(dispatcher) = &subscription.dispatcher {
                        if let Err(e) = dispatcher.reset_pending().await {
                            tracing::warn!("Failed to reset pending state: {}", e);
                        }
                    }
                }
                drop(subscriptions);

                let notifier_guard = topic.notifiers.lock().await;
                for notifier in notifier_guard.iter() {
                    notifier.notify_one();
                }
            } else {
                tracing::warn!(
                    "Topic not found when trying to trigger dispatcher for consumer {}",
                    consumer_id
                );
            }
        } else {
            tracing::warn!(
                "Consumer {} not found in consumer registry when trying to trigger dispatcher",
                consumer_id
            );
        }
    }

    /// Unsubscribes from a topic if there are no active consumers and removes the subscription from metadata.
    pub(crate) async fn unsubscribe(
        &self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<()> {
        if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
            if let Some(value) = topic.check_subscription(subscription_name).await {
                if value == false {
                    topic.unsubscribe(subscription_name).await;
                    let _ = {
                        let mut resources = self.resources.lock().await;
                        resources
                            .topic
                            .delete_subscription(subscription_name, topic_name)
                            .await
                    };
                    return Ok(());
                }
            }
        }

        Err(anyhow!(
            "Unable to unsubscribe as the subscription {} has active consumers",
            subscription_name
        ))
    }
}

#[derive(Debug, Clone)]
/// In-memory registry for producers: producer_id -> topic_name.
pub(crate) struct ProducerRegistry(Arc<DashMap<u64, String>>);

impl ProducerRegistry {
    /// Creates an empty producer registry.
    pub(crate) fn new() -> Self {
        Self(Arc::new(DashMap::new()))
    }

    /// Inserts/updates the mapping for `producer_id`.
    pub(crate) fn insert(&self, producer_id: u64, topic_name: String) {
        self.0.insert(producer_id, topic_name);
    }

    /// Removes a producer mapping if present.
    pub(crate) fn remove(&self, producer_id: &u64) {
        self.0.remove(producer_id);
    }

    /// Returns true if a `producer_id` is registered.
    pub(crate) fn contains(&self, producer_id: u64) -> bool {
        self.0.contains_key(&producer_id)
    }

    /// Returns the topic name for `producer_id`, if registered.
    pub(crate) fn get_topic(&self, producer_id: u64) -> Option<String> {
        self.0.get(&producer_id).map(|r| r.value().clone())
    }
}

#[derive(Debug, Clone)]
/// In-memory registry for consumers: consumer_id -> (topic_name, subscription_name).
pub(crate) struct ConsumerRegistry(Arc<DashMap<u64, (String, String)>>);

impl ConsumerRegistry {
    /// Creates an empty consumer registry.
    pub(crate) fn new() -> Self {
        Self(Arc::new(DashMap::new()))
    }

    /// Inserts/updates the mapping for `consumer_id`.
    pub(crate) fn insert(&self, consumer_id: u64, topic_name: String, subscription_name: String) {
        self.0.insert(consumer_id, (topic_name, subscription_name));
    }

    /// Removes a consumer mapping if present.
    pub(crate) fn remove(&self, consumer_id: &u64) {
        self.0.remove(consumer_id);
    }

    /// Returns the (topic, subscription) pair for `consumer_id`, if registered.
    pub(crate) fn get(&self, consumer_id: u64) -> Option<(String, String)> {
        self.0.get(&consumer_id).map(|e| e.value().clone())
    }
}
