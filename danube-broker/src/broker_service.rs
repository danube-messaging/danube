use crate::broker_metrics::CLIENT_REDIRECTS_TOTAL;
use anyhow::{anyhow, Result};
use danube_core::message::StreamMessage;
use danube_persistent_storage::WalStorageFactory;
use metrics::counter;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Code, Status};
use tracing::info;

use danube_core::proto::{DispatchStrategy as ProtoDispatchStrategy, ErrorType, SchemaReference};

use crate::{
    consumer::Consumer,
    error_message::create_error_status,
    message::AckMessage,
    resources::Resources,
    subscription::SubscriptionOptions,
    topic_cluster::TopicCluster,
    topic_control::{ConsumerRegistry, ProducerRegistry, TopicManager},
    topic_worker::TopicWorkerPool,
    utils::get_random_id,
};

/// BrokerService is the broker API surface.
///
/// It acts as a thin orchestrator:
/// - Delegates local topic lifecycle and producer/consumer ops to `TopicManager`.
/// - Delegates cluster-level operations (metadata) to `TopicCluster`.
/// - Delegates message flow to `TopicWorkerPool`.
///
/// This keeps the entrypoint clean while heavy lifting is done in specialized modules.
#[derive(Debug)]
pub(crate) struct BrokerService {
    /// Unique identifier for this broker instance.
    pub(crate) broker_id: u64,
    /// Worker pool that owns and executes per-topic operations.
    pub(crate) topic_worker_pool: Arc<TopicWorkerPool>,
    /// Manager for local topic lifecycle and producer/consumer management.
    pub(crate) topic_manager: TopicManager,
    /// Cluster/metadata accessor for topic lifecycle at cluster scope.
    pub(crate) topic_cluster: TopicCluster,

    /// Shared resources (Local Cache views over cluster/namespace/topic metadata).
    pub(crate) resources: Arc<Mutex<Resources>>,
    /// Whether producers are allowed to auto-create topics when missing.
    pub(crate) auto_create_topics: bool,
}

impl BrokerService {
    /// Constructs a new BrokerService and wires its delegates (TopicManager, WorkerPool, Resources).
    pub(crate) fn new(
        resources: Resources,
        wal_factory: WalStorageFactory,
        auto_create_topics: bool,
    ) -> Self {
        let broker_id = get_random_id();

        let producers = ProducerRegistry::new();
        let consumers = ConsumerRegistry::new();
        let topic_worker_pool = Arc::new(TopicWorkerPool::new(None));
        let resources_arc = Arc::new(Mutex::new(resources));

        let topic_manager = TopicManager::new(
            broker_id,
            topic_worker_pool.clone(),
            wal_factory.clone(),
            resources_arc.clone(),
            producers.clone(),
            consumers.clone(),
        );

        let topic_cluster = TopicCluster::new(resources_arc.clone());

        BrokerService {
            broker_id,
            topic_worker_pool,
            topic_manager,
            topic_cluster,
            resources: resources_arc,
            auto_create_topics,
        }
    }

    // =====================================================================
    // Client lookup and topic access
    // =====================================================================

    // The broker checks if it is the owner of the topic. If it is not, but the topic exist in the cluster,
    // the broker instruct the client to redo the lookup request.
    //
    // If the topic doesn't exist in the cluster, and the auto-topic creation is enabled,
    // the broker creates new topic to the metadata store.
    //
    // The Leader Broker will be informed about the new topic creation and assign the topic to a broker.
    // The selected Broker will be informed through watch mechanism and will host the topic.
    /// Validates topic format, checks local presence and dispatch strategy, and
    /// optionally triggers cluster auto-create. Returns true if local; otherwise
    /// returns an error with appropriate retry/redirection semantics.
    pub(crate) async fn get_topic(
        &self,
        topic_name: &str,
        dispatch_strategy: Option<ProtoDispatchStrategy>,
        schema_ref: Option<SchemaReference>,
    ) -> Result<bool, Status> {
        // Validate topic format
        if !validate_topic_format(topic_name) {
            let error_string = format!(
                "The topic: {} has an invalid format, should be: /namespace_name/topic_name",
                topic_name
            );
            return Err(create_error_status(
                Code::InvalidArgument,
                ErrorType::InvalidTopicName,
                &error_string,
                None,
            ));
        }

        // Fast path: topic is local
        if self.topic_worker_pool.contains_topic(topic_name) {
            if let Some(req_ds) = dispatch_strategy {
                if let Some(false) = self.topic_worker_pool.strategies_match(topic_name, req_ds) {
                    return Err(create_error_status(
                        Code::FailedPrecondition,
                        ErrorType::UnknownError,
                        "Producer requested dispatch strategy does not match topic strategy",
                        None,
                    ));
                }
            }

            // Schema reference validation: if producer explicitly sets schema, validate it
            if let Some(schema_ref) = schema_ref {
                if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
                    let subject = schema_ref.subject.clone();

                    // Check if topic already has a schema subject assigned
                    let existing_subject = topic.get_schema_subject().await;

                    match existing_subject {
                        Some(existing) => {
                            // Topic has schema - producer's subject must match
                            if existing != subject {
                                return Err(create_error_status(
                                    Code::FailedPrecondition,
                                    ErrorType::UnknownError,
                                    &format!(
                                        "Topic '{}' uses schema subject '{}', cannot use '{}'. Only admin can change topic schema.",
                                        topic_name, existing, subject
                                    ),
                                    None,
                                ));
                            }
                            // Subject matches - already set, no action needed
                        }
                        None => {
                            // Topic has no schema yet - first producer assigns it
                            topic.set_schema_ref(schema_ref).await.map_err(|e| {
                                create_error_status(
                                    Code::FailedPrecondition,
                                    ErrorType::UnknownError,
                                    &format!(
                                        "Failed to assign schema '{}' to topic '{}': {}. Ensure schema is registered first.",
                                        subject, topic_name, e
                                    ),
                                    None,
                                )
                            })?;
                        }
                    }
                }
            }

            return Ok(true);
        }

        // If topic exists in namespace but is not local, ask client to redo lookup
        let cluster = TopicCluster::new(self.resources.clone());
        if cluster.exists_topic_in_namespace(topic_name).await {
            // Count redirect when topic exists but is not local
            let ns = get_nsname_from_topic(topic_name).to_string();
            counter!(
                CLIENT_REDIRECTS_TOTAL.name,
                "reason" => "not_local",
                "topic_ns" => ns
            )
            .increment(1);
            return Err(create_error_status(
                Code::Unavailable,
                ErrorType::ServiceNotReady,
                "The topic exists but is served by another broker, redo the Lookup request",
                None,
            ));
        }

        // Auto-create path if enabled
        if !self.auto_create_topics {
            let error_string = &format!("Unable to find the topic: {}", topic_name);
            return Err(create_error_status(
                Code::InvalidArgument,
                ErrorType::TopicNotFound,
                error_string,
                None,
            ));
        }

        match self
            .create_topic_cluster(topic_name, dispatch_strategy, schema_ref)
            .await
        {
            Ok(()) => {
                // Count redirect after auto-create path
                let ns = get_nsname_from_topic(topic_name).to_string();
                counter!(
                    CLIENT_REDIRECTS_TOTAL.name,
                    "reason" => "post_create",
                    "topic_ns" => ns
                )
                .increment(1);
                Err(create_error_status(
                Code::Unavailable,
                ErrorType::ServiceNotReady,
                "The topic metadata was created, need to redo the lookup to find the correct broker",
                None,
                ))
            }
            Err(err) => Err(err),
        }
    }

    /// Returns the list of topic names currently served by this broker.
    pub(crate) fn get_topics(&self) -> Vec<String> {
        self.topic_worker_pool.get_all_topics()
    }

    // =====================================================================
    // Cluster operations
    // =====================================================================

    /// Creates a topic in the cluster metadata. Load Manager will assign it to a broker.
    pub(crate) async fn create_topic_cluster(
        &self,
        topic_name: &str,
        dispatch_strategy: Option<ProtoDispatchStrategy>,
        schema_ref: Option<SchemaReference>,
    ) -> Result<(), Status> {
        self.topic_cluster
            .create_on_cluster(topic_name, dispatch_strategy, schema_ref, None)
            .await
    }

    // (removed) post_new_topic moved to TopicCluster

    /// Schedules deletion of a topic in the cluster metadata (triggers host broker cleanup).
    /// Owning broker performs local cleanup via watch_events_for_broker upon receiving delete.
    pub(crate) async fn post_delete_topic(&self, topic_name: &str) -> Result<()> {
        self.topic_cluster.post_delete_topic(topic_name).await
    }

    // =====================================================================
    // Lookups and partitions
    // =====================================================================
    /// Looks up the broker serving `topic_name`. Returns (is_local, addr).
    pub(crate) async fn lookup_topic(&self, topic_name: &str) -> Option<(bool, String)> {
        // Served locally
        if self.topic_worker_pool.contains_topic(topic_name) {
            return Some((true, String::new()));
        }

        // Ask cluster for serving broker address
        self.topic_cluster
            .find_serving_broker(topic_name)
            .await
            .map(|addr| (false, addr))
    }

    /// Returns the broker_id that currently serves the topic, if any.
    pub(crate) async fn get_topic_broker_id(&self, topic_name: &str) -> Option<String> {
        let resources = self.resources.lock().await;
        resources.cluster.get_broker_for_topic(topic_name).await
    }

    /// Retrieves partition names if `topic_name` is partitioned, or returns the single topic.
    pub(crate) async fn topic_partitions(&self, topic_name: &str) -> Vec<String> {
        let mut topics = Vec::new();
        let ns_name = get_nsname_from_topic(topic_name);

        // check if the topic exist in the Metadata Store
        // if true, means that it is not a partitioned topic
        match {
            let resources = self.resources.lock().await;
            resources
                .namespace
                .check_if_topic_exist(ns_name, topic_name)
        } {
            true => {
                topics.push(topic_name.to_owned());
                return topics;
            }
            false => {}
        };

        // if not, we should look for any topic starting with topic_name

        topics = {
            let resources = self.resources.lock().await;
            resources
                .namespace
                .get_topic_partitions(ns_name, topic_name)
                .await
        };

        topics
    }

    // =====================================================================
    // Producer operations (delegated to TopicManager)
    // =====================================================================

    /// Returns an existing producer id if a producer with `producer_name` is already attached.
    pub(crate) async fn check_if_producer_exist(
        &self,
        topic_name: &str,
        producer_name: &str,
    ) -> Option<u64> {
        let topic = self.topic_worker_pool.get_topic(topic_name)?;
        let producers = topic.producers.lock().await;
        for (_id, producer) in producers.iter() {
            if producer.producer_name == producer_name {
                return Some(producer.producer_id);
            }
        }
        None
    }

    /// Returns true if the producer with `producer_id` is currently healthy.
    pub(crate) async fn health_producer(&self, producer_id: u64) -> bool {
        self.topic_manager.health_producer(producer_id).await
    }

    /// Creates and attaches a new producer to `topic_name`.
    pub(crate) async fn create_new_producer(
        &self,
        producer_name: &str,
        producer_id: u64,
        producer_access_mode: i32,
        topic_name: &str,
    ) -> Result<u64> {
        self.topic_manager
            .create_producer(producer_name, producer_id, producer_access_mode, topic_name)
            .await
    }

    // finding a Topic by Producer ID (moved to TopicManager; keep wrapper if needed)

    // =====================================================================
    // Consumer operations (delegated to TopicManager)
    // =====================================================================

    /// Returns consumer for `consumer_id` if present.
    pub(crate) async fn find_consumer_by_id(&self, consumer_id: u64) -> Option<Consumer> {
        self.topic_manager.find_consumer_by_id(consumer_id).await
    }

    /// Finds consumer for streaming operations.
    /// Returns consumer which contains session (with rx_cons) for `consumer_id` if present.
    pub(crate) async fn find_consumer_for_streaming(&self, consumer_id: u64) -> Option<Consumer> {
        self.topic_manager
            .find_consumer_for_streaming(consumer_id)
            .await
    }

    /// Returns true if the consumer with `consumer_id` is currently healthy.
    pub(crate) async fn health_consumer(&self, consumer_id: u64) -> bool {
        self.topic_manager.health_consumer(consumer_id).await
    }

    /// Triggers the dispatcher to resume polling when a consumer reconnects (reliable mode).
    pub(crate) async fn trigger_dispatcher_on_reconnect(&self, consumer_id: u64) {
        self.topic_manager
            .trigger_dispatcher_on_reconnect(consumer_id)
            .await
    }

    /// Returns an existing consumer id if allowed and already registered for the subscription.
    pub(crate) async fn check_if_consumer_exist(
        &self,
        consumer_name: &str,
        subscription_name: &str,
        topic_name: &str,
    ) -> Option<u64> {
        self.topic_manager
            .check_if_consumer_exist(consumer_name, subscription_name, topic_name)
            .await
    }

    /// Validates whether policies allow creating a new subscription for `topic_name`.
    pub(crate) async fn allow_subscription_creation(&self, topic_name: impl Into<String>) -> bool {
        let topic_name = topic_name.into();
        if let Some(topic) = self.topic_worker_pool.get_topic(&topic_name) {
            let limit = topic
                .topic_policies
                .as_ref()
                .map(|p| p.get_max_subscriptions_per_topic())
                .unwrap_or(0);
            if limit == 0 {
                return true;
            }
            let current = topic.subscription_count().await as u32;
            return current < limit;
        }
        // If topic not found locally, conservatively allow (caller ensured locality via get_topic)
        true
    }

    // =====================================================================
    // Messaging (delegated to TopicWorkerPool)
    // =====================================================================

    /// Publishes a message asynchronously via the worker pool.
    pub async fn publish_message_async(
        &self,
        topic_name: String,
        message: StreamMessage,
    ) -> Result<()> {
        // Route message through topic worker pool for async processing
        self.topic_worker_pool
            .publish_message_async(topic_name, message)
            .await
    }

    // Async version of subscription using topic worker pool
    pub(crate) async fn subscribe_async(
        &self,
        topic_name: String,
        subscription_options: SubscriptionOptions,
    ) -> Result<u64> {
        self.topic_manager
            .subscribe(topic_name, subscription_options)
            .await
    }

    /// Acknowledges a message asynchronously via the worker pool.
    pub(crate) async fn ack_message_async(&self, ack_msg: AckMessage) -> Result<()> {
        self.topic_worker_pool.ack_message_async(ack_msg).await
    }

    // unsubscribe subscription from topic
    // only if subscription is empty, so no consumers attached
    /// Unsubscribes a subscription from a topic if safe (no active consumers).
    pub(crate) async fn unsubscribe(
        &self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<()> {
        self.topic_manager
            .unsubscribe(subscription_name, topic_name)
            .await
    }

    // =====================================================================
    // Namespace management (admin helpers)
    // =====================================================================

    /// Creates a namespace if it doesn't already exist.
    pub(crate) async fn create_namespace_if_absent(&self, namespace_name: &str) -> Result<()> {
        match {
            let mut resources = self.resources.lock().await;
            resources.namespace.namespace_exist(namespace_name).await
        } {
            Ok(true) => {
                return Err(anyhow!("Namespace {} already exists.", namespace_name));
            }
            Ok(false) => {
                let mut resources = self.resources.lock().await;
                resources
                    .namespace
                    .create_namespace(namespace_name, None)
                    .await?;
            }
            Err(err) => {
                return Err(anyhow!("Unable to perform operation {}", err));
            }
        }

        Ok(())
    }

    // deletes only empty namespaces (with no topics)
    /// Deletes a namespace (only if empty based on backend rules).
    pub(crate) async fn delete_namespace(&self, ns_name: &str) -> Result<()> {
        let mut resources = self.resources.lock().await;
        resources.namespace.delete_namespace(ns_name).await?;

        Ok(())
    }

    // Shutdown the broker service and all worker threads
    #[allow(dead_code)]
    /// Shuts down the worker pool gracefully.
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        let topic_count = self.topic_worker_pool.get_all_topics().len();
        info!(
            broker_id = %self.broker_id,
            topic_count = %topic_count,
            "shutting down broker service"
        );

        // Shutdown the topic worker pool
        Arc::get_mut(&mut self.topic_worker_pool)
            .ok_or_else(|| anyhow!("Failed to get mutable reference to topic worker pool"))?
            .shutdown()
            .await;

        info!(
            broker_id = %self.broker_id,
            "broker service shutdown completed"
        );
        Ok(())
    }
}

// Topics string representation:  /{namespace}/{topic-name}
// =====================================================================
// Utilities
// =====================================================================
/// Validates that the topic string has the required format `/namespace/topic-name`.
pub(crate) fn validate_topic_format(input: &str) -> bool {
    let parts: Vec<&str> = input.split('/').collect();

    if parts.len() != 3 {
        return false;
    }

    for part in parts.iter() {
        if !part
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return false;
        }
    }

    true
}

// extract the namespace from a topic
// example from topic /ns_name/topic_name returns the  ns_name
/// Extracts the namespace component from a validated topic string.
fn get_nsname_from_topic(topic_name: &str) -> &str {
    // assuming that the topic name has already been validated.
    let parts: Vec<&str> = topic_name.split('/').collect();
    let ns_name = parts.get(1).unwrap();

    ns_name
}
