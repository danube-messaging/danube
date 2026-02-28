use std::sync::Arc;

use anyhow::{anyhow, Result};
use dashmap::DashMap;
use metrics::gauge;
use tonic::Status;
use tracing::{debug, error, info, warn};

use crate::broker_metrics::{TOPIC_ACTIVE_CONSUMERS, TOPIC_ACTIVE_PRODUCERS};
use crate::danube_service::metrics_collector::MetricsCollector;
use crate::resources::BASE_TOPICS_PATH;
use crate::schema::ValidationPolicy;
use crate::utils::join_path;
use crate::{
    broker_metrics::BROKER_TOPICS_OWNED,
    consumer::Consumer,
    resources::Resources,
    subscription::{SubscriptionOptions, SUBSCRIPTION_IDLE_GRACE},
    topic::Topic,
    topic_worker::TopicWorkerPool,
};
use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_persistent_storage::WalStorageFactory;
/// Manages topics and their associated producers and consumers.
#[derive(Debug, Clone)]
pub(crate) struct TopicManager {
    /// Broker identifier for metrics and logging.
    pub(crate) broker_id: u64,
    /// Worker pool handling per-topic execution and routing.
    pub(crate) topic_worker_pool: Arc<TopicWorkerPool>,
    /// Factory for building per-topic WAL storage (reliable mode).
    pub(crate) wal_factory: WalStorageFactory,
    /// Shared access to metadata resources (Raft state machine).
    pub(crate) resources: Arc<Resources>,
    /// Index of producer_id -> topic_name.
    pub(crate) producers: ProducerRegistry,
    /// Index of consumer_id -> (topic_name, subscription_name).
    pub(crate) consumers: ConsumerRegistry,
    /// Metrics collector for dual-tracking
    pub(crate) metrics_collector: Arc<MetricsCollector>,
}

impl TopicManager {
    /// Constructs a new TopicManager bound to this broker's worker pool and resources.
    pub(crate) fn new(
        broker_id: u64,
        topic_worker_pool: Arc<TopicWorkerPool>,
        wal_factory: WalStorageFactory,
        resources: Arc<Resources>,
        producers: ProducerRegistry,
        consumers: ConsumerRegistry,
        metrics_collector: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            broker_id,
            topic_worker_pool,
            wal_factory,
            resources,
            producers,
            consumers,
            metrics_collector,
        }
    }

    /// Spawns a background task that periodically removes idle non-reliable subscriptions.
    /// The check interval is half the grace period (worst-case delay = 1.5× grace).
    /// Also cleans up ConsumerRegistry entries for removed consumers.
    pub(crate) fn start_subscription_removal(&self) {
        let topic_worker_pool = Arc::clone(&self.topic_worker_pool);
        let consumer_registry = self.consumers.clone();
        let interval = SUBSCRIPTION_IDLE_GRACE / 2;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let topics = topic_worker_pool.all_non_reliable_topics();
                for topic in topics {
                    let removed = topic
                        .remove_idle_subscriptions(SUBSCRIPTION_IDLE_GRACE)
                        .await;
                    for cid in removed {
                        debug!(consumer_id = %cid, topic = %topic.topic_name, "reaper: removed stale consumer from registry");
                        consumer_registry.remove(&cid);
                    }
                }
            }
        });

        info!(
            grace_secs = SUBSCRIPTION_IDLE_GRACE.as_secs(),
            interval_secs = interval.as_secs(),
            "subscription idle removal service started"
        );
    }

    /// Ensures a topic exists locally by materializing it from metadata store.
    /// Returns the configured dispatch strategy and optional schema subject.
    pub(crate) async fn ensure_local(
        &self,
        topic_name: &str,
    ) -> Result<(ConfigDispatchStrategy, Option<String>)> {
        //get retention strategy from store
        let dispatch_strategy = self.resources.topic.get_dispatch_strategy(topic_name).await;
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
            self.resources.topic.clone(),
            self.resources.schema.clone(),
            self.metrics_collector.clone(),
        );

        // get policies from store
        let policies = self.resources.topic.get_policies(topic_name).await;

        if let Some(with_policies) = policies {
            let _ = new_topic.policies_update(with_policies);
        } else {
            // get namespace policies
            let parts: Vec<_> = topic_name.split('/').collect();
            let ns_name = format!("/{}", parts[1]);

            let ns_policies = self.resources.namespace.get_policies(&ns_name).await;
            if let Ok(ns_policies) = ns_policies {
                let _ = new_topic.policies_update(ns_policies);
            }
        }

        // Load schema configuration if it exists
        let schema_config = self
            .resources
            .topic
            .get_schema_config(topic_name)
            .await
            .ok()
            .flatten();

        if let Some(config) = schema_config {
            info!(
                topic = %topic_name,
                subject = %config.subject,
                policy = ?config.validation_policy,
                "loading persisted schema config for topic"
            );

            // Apply schema configuration to the topic
            if let Err(e) = new_topic
                .set_schema_ref(danube_core::proto::SchemaReference {
                    subject: config.subject.clone(),
                    version_ref: None,
                })
                .await
            {
                warn!(
                    topic = %topic_name,
                    subject = %config.subject,
                    error = %e,
                    "Failed to apply persisted schema subject to topic"
                );
            }

            new_topic
                .configure_schema_validation(
                    config.validation_policy,
                    config.enable_payload_validation,
                )
                .await;
        }

        // Wrap topic in Arc for concurrent access
        let new_topic_arc = Arc::new(new_topic);

        // Add topic to worker pool (single source of truth)
        self.topic_worker_pool
            .add_topic_to_worker(topic_name.to_string(), new_topic_arc);

        gauge!(BROKER_TOPICS_OWNED.name).increment(1);

        // Get schema subject from metadata if topic has one
        let schema_subject = self.resources.topic.get_schema_subject(topic_name).await;

        Ok((dispatch_strategy, schema_subject))
    }

    /// Get schema information for logging (from Raft state machine)
    pub(crate) async fn get_schema_info(&self, topic_name: &str) -> Option<(String, u64, String)> {
        // Get schema subject from topic metadata
        let schema_subject = self.resources.topic.get_schema_subject(topic_name).await?;

        // Get schema details from registry
        let schema_details = self
            .resources
            .schema
            .get_schema_by_subject(&schema_subject)
            .await?;

        Some((
            schema_subject,
            schema_details.schema_id,
            schema_details.schema_type,
        ))
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
        let is_reliable = self
            .resources
            .topic
            .get_dispatch_strategy(topic_name)
            .await
            .map(|ds| matches!(ds, ConfigDispatchStrategy::Reliable))
            .unwrap_or(false);
        if is_reliable {
            if let Err(e) = self.flush_and_seal(topic_name).await {
                error!(
                    topic = %topic_name,
                    error = %e,
                    "flush_and_seal failed during delete"
                );
            }
            if let Err(e) = self.delete_storage_metadata(topic_name).await {
                error!(
                    topic = %topic_name,
                    error = %e,
                    "delete_storage_metadata failed during delete"
                );
            }

            // Remove subscription cursors under /topics/<ns>/<topic>/subscriptions/*/cursor
            let subs = self
                .resources
                .topic
                .get_subscription_for_topic(topic_name)
                .await;
            // Build correct path: /topics/<ns>/<topic>/subscriptions/<sub>/cursor
            let trimmed = topic_name.trim_start_matches('/');
            let mut parts = trimmed.split('/');
            let ns = parts.next().unwrap_or("");
            let topic = parts.next().unwrap_or("");
            for sub in subs {
                let cursor_path =
                    join_path(&[BASE_TOPICS_PATH, ns, topic, "subscriptions", &sub, "cursor"]);
                if let Err(e) = self.resources.topic.delete(&cursor_path).await {
                    warn!(
                        namespace = %ns,
                        topic = %topic,
                        subscription = %sub,
                        path = %cursor_path,
                        error = %e,
                        "failed to delete reliable cursor"
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

        gauge!(BROKER_TOPICS_OWNED.name).decrement(1);

        Ok(topic)
    }

    /// Unload a topic from this broker, preserving required metadata so it can be reassigned.
    /// - Non-reliable: keep delivery, schema, namespace; delete producer metadata (optionally subscriptions)
    /// - Reliable: flush cursors, seal storage, delete producer metadata; keep subscriptions/cursors/storage
    pub(crate) async fn unload_topic(&self, topic_name: &str) -> Result<()> {
        if !self.topic_worker_pool.contains_topic(topic_name) {
            return Err(anyhow!(
                "The topic {} does not exist on the broker {}",
                topic_name,
                self.broker_id
            ));
        }

        let is_reliable = self
            .resources
            .topic
            .get_dispatch_strategy(topic_name)
            .await
            .map(|ds| matches!(ds, ConfigDispatchStrategy::Reliable))
            .unwrap_or(false);

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
        let _ = self.resources.topic.delete_all_producers(topic_name).await;
        let _ = self
            .resources
            .topic
            .delete_all_subscriptions(topic_name)
            .await;

        gauge!(BROKER_TOPICS_OWNED.name).decrement(1);
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
        let _ = self.resources.topic.delete_all_producers(topic_name).await;

        gauge!(BROKER_TOPICS_OWNED.name).decrement(1);
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

            gauge!(TOPIC_ACTIVE_PRODUCERS.name, "topic" => topic_name.to_string()).increment(1);

            // Dual-track producer count
            let producer_count = topic.get_producer_count().await;
            self.metrics_collector
                .set_producer_count(topic_name, producer_count)
                .await;

            self.resources
                .topic
                .create_producer(producer_id, topic_name, producer_config)
                .await?;
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

        gauge!(TOPIC_ACTIVE_CONSUMERS.name, "topic" => topic_name.clone()).increment(1);

        // Dual-track consumer count
        if let Some(topic) = self.topic_worker_pool.get_topic(&topic_name) {
            let consumer_count = topic.get_consumer_count().await;
            self.metrics_collector
                .set_consumer_count(&topic_name, consumer_count)
                .await;
        }

        let sub_options = serde_json::to_value(&subscription_options)?;
        self.resources
            .topic
            .create_subscription(
                &subscription_options.subscription_name,
                &topic_name,
                sub_options,
            )
            .await?;

        Ok(consumer_id)
    }

    /// Finds consumer by id via the consumer index and topic subscriptions.
    pub(crate) async fn find_consumer_by_id(&self, consumer_id: u64) -> Option<Consumer> {
        if let Some((topic_name, subscription_name)) = self.consumers.get(consumer_id) {
            if let Some(topic) = self.topic_worker_pool.get_topic(&topic_name) {
                if let Some(subscription) = topic.subscriptions.lock().await.get(&subscription_name)
                {
                    return subscription.get_consumer(consumer_id);
                }
            }
        }
        None
    }

    /// Finds consumer by id for streaming operations.
    /// The consumer contains session (with rx_cons) needed for message streaming.
    /// This is an alias for find_consumer_by_id but makes the streaming use-case explicit.
    pub(crate) async fn find_consumer_for_streaming(&self, consumer_id: u64) -> Option<Consumer> {
        self.find_consumer_by_id(consumer_id).await
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
                            tracing::warn!(consumer_id = %consumer_id, error = %e, "failed to reset pending state");
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
                    consumer_id = %consumer_id,
                    "topic not found when trying to trigger dispatcher"
                );
            }
        } else {
            tracing::warn!(
                consumer_id = %consumer_id,
                "consumer not found in consumer registry when trying to trigger dispatcher"
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
                    let _ = self
                        .resources
                        .topic
                        .delete_subscription(subscription_name, topic_name)
                        .await;
                    return Ok(());
                }
            }
        }

        Err(anyhow!(
            "Unable to unsubscribe as the subscription {} has active consumers",
            subscription_name
        ))
    }

    // ===== Schema Configuration Operations (Admin) =====

    /// Configure schema settings for a topic (admin-only).
    ///
    /// This allows administrators to assign or change a topic's schema subject
    /// and validation settings, overriding first producer's assignment.
    pub(crate) async fn configure_topic_schema(
        &self,
        topic_name: &str,
        schema_subject: String,
        validation_policy: ValidationPolicy,
        enable_payload_validation: bool,
    ) -> Result<String, Status> {
        info!(
            topic = %topic_name,
            subject = %schema_subject,
            policy = ?validation_policy,
            "configuring schema for topic"
        );

        // Get the topic from worker pool
        let topic = self
            .topic_worker_pool
            .get_topic(topic_name)
            .ok_or_else(|| {
                Status::not_found(format!("Topic '{}' not found on this broker", topic_name))
            })?;

        // Set schema subject for the topic
        topic
            .set_schema_ref(danube_core::proto::SchemaReference {
                subject: schema_subject.clone(),
                version_ref: None,
            })
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to set schema subject '{}': {}",
                    schema_subject, e
                ))
            })?;

        // Configure validation settings
        topic
            .configure_schema_validation(validation_policy, enable_payload_validation)
            .await;

        // Persist configuration to store
        let config = crate::resources::TopicSchemaConfig {
            subject: schema_subject.clone(),
            validation_policy,
            enable_payload_validation,
        };
        self.resources
            .topic
            .store_schema_config(topic_name, &config)
            .await
            .map_err(|e| Status::internal(format!("Failed to persist schema config: {}", e)))?;

        info!(
            topic = %topic_name,
            subject = %schema_subject,
            "successfully configured topic with schema (persisted to ETCD)"
        );

        Ok(format!(
            "Topic '{}' configured with schema '{}', validation policy '{:?}', payload validation {}",
            topic_name,
            schema_subject,
            validation_policy,
            if enable_payload_validation { "enabled" } else { "disabled" }
        ))
    }

    /// Update validation policy for a topic (admin-only).
    ///
    /// This allows administrators to adjust validation strictness without
    /// changing the schema subject.
    pub(crate) async fn update_topic_validation_policy(
        &self,
        topic_name: &str,
        validation_policy: ValidationPolicy,
        enable_payload_validation: bool,
    ) -> Result<String, Status> {
        info!(
            topic = %topic_name,
            policy = ?validation_policy,
            "updating validation policy for topic"
        );

        // Get the topic from worker pool
        let topic = self
            .topic_worker_pool
            .get_topic(topic_name)
            .ok_or_else(|| {
                Status::not_found(format!("Topic '{}' not found on this broker", topic_name))
            })?;

        // Update validation settings
        topic
            .configure_schema_validation(validation_policy, enable_payload_validation)
            .await;

        // Get current schema subject (required for ETCD storage)
        let schema_subject = topic.get_schema_subject().await.ok_or_else(|| {
            Status::failed_precondition(format!(
                "Topic '{}' has no schema subject configured. Use ConfigureTopicSchema first.",
                topic_name
            ))
        })?;

        // Persist updated configuration to store
        let config = crate::resources::TopicSchemaConfig {
            subject: schema_subject,
            validation_policy,
            enable_payload_validation,
        };
        self.resources
            .topic
            .store_schema_config(topic_name, &config)
            .await
            .map_err(|e| Status::internal(format!("Failed to persist validation policy: {}", e)))?;

        info!(
            topic = %topic_name,
            "successfully updated validation policy (persisted to ETCD)"
        );

        Ok(format!(
            "Validation policy updated to '{:?}', payload validation {}",
            validation_policy,
            if enable_payload_validation {
                "enabled"
            } else {
                "disabled"
            }
        ))
    }

    /// Get current schema configuration for a topic.
    ///
    /// Returns the schema subject, validation policy, payload validation setting,
    /// and subject's schema_id (base ID for the subject, not version-specific).
    /// Note: Topics can have messages with multiple schema versions from the same subject.
    pub(crate) async fn get_topic_schema_config(
        &self,
        topic_name: &str,
    ) -> Result<(String, ValidationPolicy, bool, u64), Status> {
        info!(topic = %topic_name, "getting schema config for topic");

        // Get the topic from worker pool
        let topic = self
            .topic_worker_pool
            .get_topic(topic_name)
            .ok_or_else(|| {
                Status::not_found(format!("Topic '{}' not found on this broker", topic_name))
            })?;

        // Get schema subject
        let schema_subject = topic
            .get_schema_subject()
            .await
            .unwrap_or_else(|| String::from(""));

        // Get validation policy
        let validation_policy = topic.get_validation_policy().await;

        // Get payload validation setting
        let enable_payload_validation = topic.get_payload_validation_enabled().await;

        // Get subject's schema_id (base ID for the subject, not version-specific)
        let schema_id = topic.get_subject_schema_id().await.unwrap_or(0);

        info!(
            topic = %topic_name,
            subject = %schema_subject,
            policy = ?validation_policy,
            subject_schema_id = %schema_id,
            "topic schema config"
        );

        Ok((
            schema_subject,
            validation_policy,
            enable_payload_validation,
            schema_id,
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
