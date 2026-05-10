use crate::args_parse::BrokerMode;
use crate::broker_metrics::CLIENT_REDIRECTS_TOTAL;
use crate::danube_service::metrics_collector::MetricsCollector;
use anyhow::{anyhow, Result};
use danube_core::message::StreamMessage;
use danube_edge::replicator::replicator::EdgeReplicator;
use danube_persistent_storage::StorageFactory;
use metrics::counter;
use std::sync::Arc;

use tonic::Status;
use tracing::info;

use danube_core::proto::{DispatchStrategy as ProtoDispatchStrategy, SchemaReference};

use crate::{
    consumer::Consumer,
    message::{AckMessage, NackMessage},
    replicator::Replicator,
    resources::Resources,
    subscription::SubscriptionOptions,
    topic_cluster::TopicCluster,
    topic_control::{ConsumerRegistry, ProducerRegistry, TopicManager},
    topic_registry::TopicRegistry,
};

/// Result of a mode-aware topic creation.
pub(crate) enum TopicCreated {
    /// Topic is loaded locally and ready (standalone/edge).
    Loaded,
    /// Topic metadata was created; client must redo lookup to find the assigned broker (cluster).
    Pending,
}

/// BrokerService is the broker API surface.
///
/// It acts as a thin orchestrator:
/// - Delegates local topic lifecycle and producer/consumer ops to `TopicManager`.
/// - Delegates cluster-level operations (metadata) to `TopicCluster`.
/// - Delegates local topic lookup and message flow to `TopicRegistry`.
///
/// This keeps the entrypoint clean while heavy lifting is done in specialized modules.
#[derive(Debug)]
pub(crate) struct BrokerService {
    /// Unique identifier for this broker instance.
    pub(crate) broker_id: u64,
    /// Deployment mode: Cluster, Standalone, or Edge.
    pub(crate) mode: BrokerMode,
    /// Registry of local topics hosted by this broker.
    pub(crate) topic_registry: Arc<TopicRegistry>,
    /// Manager for local topic lifecycle and producer/consumer management.
    pub(crate) topic_manager: TopicManager,
    /// Cluster/metadata accessor for topic lifecycle at cluster scope.
    pub(crate) topic_cluster: TopicCluster,

    /// Shared resources (cluster/namespace/topic metadata via Raft state machine).
    pub(crate) resources: Arc<Resources>,
    /// Whether producers are allowed to auto-create topics when missing.
    pub(crate) auto_create_topics: bool,

    /// Internal metrics collector for LoadReport generation
    pub(crate) metrics_collector: Arc<MetricsCollector>,
    /// Shared broker-level Replicator
    pub(crate) replicator: Arc<Replicator>,
    /// Edge replicator (only set in Edge mode).
    /// Used by `create_topic` and `delete_topic` to coordinate with the cloud.
    edge_replicator: Option<Arc<EdgeReplicator>>,
}

impl BrokerService {
    /// Constructs a new BrokerService and wires its delegates (TopicManager, WorkerPool, Resources).
    ///
    /// `broker_id` is the stable Raft node ID (auto-generated on first boot and
    /// persisted in `{data_dir}/node_id`), so the broker identity survives restarts.
    ///
    /// `edge_replicator` is `Some` only in Edge mode — it's created beforehand in
    /// `main.rs` and injected here so BrokerService can coordinate topic
    /// creation/deletion with the cloud cluster.
    pub(crate) fn new(
        broker_id: u64,
        mode: BrokerMode,
        resources: Resources,
        storage_factory: StorageFactory,
        auto_create_topics: bool,
        edge_replicator: Option<Arc<EdgeReplicator>>,
    ) -> Self {
        let producers = ProducerRegistry::new();
        let consumers = ConsumerRegistry::new();
        let topic_registry = Arc::new(TopicRegistry::new(None));
        let resources_arc = Arc::new(resources);
        let metrics_collector = Arc::new(MetricsCollector::new());
        let replicator = Arc::new(Replicator::new(broker_id));

        let topic_manager = TopicManager::new(
            broker_id,
            topic_registry.clone(),
            storage_factory.clone(),
            resources_arc.clone(),
            producers.clone(),
            consumers.clone(),
            metrics_collector.clone(),
            replicator.clone(),
        );

        let topic_cluster = TopicCluster::new(resources_arc.clone());

        // Start background loop that removes idle non-reliable subscriptions
        topic_manager.start_subscription_removal();

        BrokerService {
            broker_id,
            mode,
            topic_registry,
            topic_manager,
            topic_cluster,
            resources: resources_arc,
            auto_create_topics,
            metrics_collector,
            replicator,
            edge_replicator,
        }
    }

    /// Get a reference to the metrics collector for dual-tracking
    pub(crate) fn metrics_collector(&self) -> &Arc<MetricsCollector> {
        &self.metrics_collector
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
    /// optionally triggers cluster auto-create (producer path only).
    ///
    /// `allow_auto_create` controls whether this call may create a new topic in
    /// the cluster when the topic doesn't exist. Only the **producer** path sets
    /// this to `true`; the consumer path must always pass `false` so it can never
    /// accidentally create topics.
    ///
    /// Returns `Ok(true)` when the topic is served locally.
    /// Returns an error with retry/redirection semantics otherwise.
    pub(crate) async fn get_topic(
        &self,
        topic_name: &str,
        dispatch_strategy: Option<ProtoDispatchStrategy>,
        schema_ref: Option<SchemaReference>,
        allow_auto_create: bool,
    ) -> Result<bool, Status> {
        // Validate topic format
        if !validate_topic_format(topic_name) {
            return Err(Status::invalid_argument(format!(
                "The topic: {} has an invalid format, should be: /namespace_name/topic_name",
                topic_name
            )));
        }

        // Fast path: topic is local — validate and return
        if self.topic_registry.contains_topic(topic_name) {
            return self
                .validate_local_topic(topic_name, dispatch_strategy, schema_ref)
                .await;
        }

        // If topic exists in namespace but is not local, check if this broker
        // is the assigned owner. If so, the topic may still be loading after a
        // Raft assignment (watch event hasn't fired yet) — load it eagerly.
        let cluster = TopicCluster::new(self.resources.clone());
        let topic_in_namespace = cluster.exists_topic_in_namespace(topic_name).await;

        if topic_in_namespace {
            // Check if this broker is the assigned owner
            let assigned_to_self = self
                .get_topic_broker_id(topic_name)
                .await
                .and_then(|id| id.parse::<u64>().ok())
                .map_or(false, |id| id == self.broker_id);

            if assigned_to_self {
                // Double-check: the watch may have loaded the topic between
                // our first check and now.
                if !self.topic_registry.contains_topic(topic_name) {
                    // Topic is assigned to us but not loaded yet — eagerly load it
                    // from the metadata store rather than waiting for the watch event.
                    match self.topic_manager.ensure_local(topic_name).await {
                        Ok(_) => {
                            tracing::info!(
                                topic = %topic_name,
                                "eagerly loaded topic assigned to this broker"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                topic = %topic_name,
                                error = %e,
                                "failed to eagerly load topic, falling through to redirect"
                            );
                        }
                    }
                }

                // Topic should now be in registry — run the full validation
                // (dispatch strategy + schema) via the fast path.
                if self.topic_registry.contains_topic(topic_name) {
                    return self
                        .validate_local_topic(topic_name, dispatch_strategy, schema_ref)
                        .await;
                }
            }

            // Count redirect when topic exists but is not local
            let ns = get_nsname_from_topic(topic_name).to_string();
            counter!(
                CLIENT_REDIRECTS_TOTAL.name,
                "reason" => "not_local",
                "topic_ns" => ns
            )
            .increment(1);
            return Err(Status::unavailable(
                "The topic exists but is served by another broker, redo the Lookup request",
            ));
        }

        // Topic does not exist anywhere in the cluster.
        // Only the producer path is allowed to auto-create.
        if !allow_auto_create || !self.auto_create_topics {
            return Err(Status::not_found(format!(
                "Unable to find the topic: {}",
                topic_name
            )));
        }

        match self
            .create_topic(topic_name, dispatch_strategy, schema_ref)
            .await
        {
            Ok(TopicCreated::Loaded) => {
                // Standalone/Edge: topic loaded directly, no redirect needed
                Ok(true)
            }
            Ok(TopicCreated::Pending) => {
                // Cluster: topic posted to unassigned, client must redo lookup
                let ns = get_nsname_from_topic(topic_name).to_string();
                counter!(
                    CLIENT_REDIRECTS_TOTAL.name,
                    "reason" => "post_create",
                    "topic_ns" => ns
                )
                .increment(1);
                Err(Status::unavailable(
                    "The topic metadata was created, need to redo the lookup to find the correct broker",
                ))
            }
            Err(err) => Err(err),
        }
    }

    /// Validates a topic that is already in the local registry.
    /// Checks dispatch strategy match and schema subject compatibility.
    /// Called from both the fast path and the eager-load path in `get_topic`.
    async fn validate_local_topic(
        &self,
        topic_name: &str,
        dispatch_strategy: Option<ProtoDispatchStrategy>,
        schema_ref: Option<SchemaReference>,
    ) -> Result<bool, Status> {
        if let Some(req_ds) = dispatch_strategy {
            if let Some(false) = self.topic_registry.strategies_match(topic_name, req_ds) {
                return Err(Status::failed_precondition(
                    "Producer requested dispatch strategy does not match topic strategy",
                ));
            }
        }

        // Schema reference validation: if producer explicitly sets schema, validate it
        if let Some(schema_ref) = schema_ref {
            if let Some(topic) = self.topic_registry.get_topic(topic_name) {
                let subject = schema_ref.subject.clone();

                // Check if topic already has a schema subject assigned
                let existing_subject = topic.get_schema_subject().await;

                match existing_subject {
                    Some(existing) => {
                        // Topic has schema - producer's subject must match
                        if existing != subject {
                            return Err(Status::failed_precondition(format!(
                                "Topic '{}' uses schema subject '{}', cannot use '{}'. Only admin can change topic schema.",
                                topic_name, existing, subject
                            )));
                        }
                        // Subject matches - already set, no action needed
                    }
                    None => {
                        // Topic has no schema yet - first producer assigns it
                        topic.set_schema_ref(schema_ref).await.map_err(|e| {
                            Status::failed_precondition(format!(
                                "Failed to assign schema '{}' to topic '{}': {}. Ensure schema is registered first.",
                                subject, topic_name, e
                            ))
                        })?;
                    }
                }
            }
        }

        Ok(true)
    }

    /// Returns the list of topic names currently served by this broker.
    pub(crate) fn get_topics(&self) -> Vec<String> {
        self.topic_registry.get_all_topics()
    }

    // =====================================================================
    // Mode-aware topic lifecycle
    // =====================================================================

    /// Mode-aware topic creation entry point.
    ///
    /// Used by both producer auto-create and admin CLI.
    /// - **Cluster**: posts to unassigned path for LoadManager assignment (redirect).
    /// - **Standalone**: writes metadata + loads topic directly (no redirect).
    /// - **Edge**: reserved for PR2 (cloud confirmation).
    pub(crate) async fn create_topic(
        &self,
        topic_name: &str,
        dispatch_strategy: Option<ProtoDispatchStrategy>,
        schema_ref: Option<SchemaReference>,
    ) -> Result<TopicCreated, Status> {
        match self.mode {
            BrokerMode::Standalone => {
                // Validate + write metadata directly (skip unassigned path)
                self.topic_cluster
                    .create_on_cluster_standalone(topic_name, dispatch_strategy, schema_ref, None)
                    .await?;

                // Load the topic directly into the local registry
                self.topic_manager
                    .ensure_local(topic_name)
                    .await
                    .map_err(|e| {
                        Status::internal(format!("failed to load topic locally: {}", e))
                    })?;

                Ok(TopicCreated::Loaded)
            }
            BrokerMode::Cluster => {
                // Full cluster pipeline: unassigned → LoadManager → broker_watcher
                self.topic_cluster
                    .create_on_cluster(topic_name, dispatch_strategy, schema_ref, None)
                    .await?;
                Ok(TopicCreated::Pending)
            }
            BrokerMode::Edge => {
                // Edge mode: topics are managed exclusively via RegisterEdge at bootstrap.
                // Runtime topic creation is not supported — topics must be declared
                // in edge.yaml and registered with the cluster at startup.
                //
                // If a producer tries to create a topic that wasn't in the config,
                // we create it locally (for the WAL) but it won't be replicated
                // to the cluster until the edge restarts with the topic in its config.
                self.topic_cluster
                    .create_on_cluster_standalone(topic_name, dispatch_strategy, schema_ref, None)
                    .await?;

                self.topic_manager
                    .ensure_local(topic_name)
                    .await
                    .map_err(|e| {
                        Status::internal(format!("failed to load topic locally: {}", e))
                    })?;

                info!(
                    topic = %topic_name,
                    "edge topic created locally (note: not registered with cluster — \
                     add to edge.yaml and restart to enable replication)"
                );

                Ok(TopicCreated::Loaded)
            }
        }
    }

    /// Schedules deletion of a topic in a mode-aware manner.
    ///
    /// - **Cluster**: triggers the cluster deletion pipeline (broker assignment + watch).
    /// - **Standalone**: removes the topic locally and cleans up metadata directly.
    /// - **Edge**: reserved for PR2.
    pub(crate) async fn delete_topic(&self, topic_name: &str) -> Result<()> {
        match self.mode {
            BrokerMode::Standalone => {
                // Remove from local topic registry
                self.topic_registry.remove_topic(topic_name);
                // Clean up all metadata
                self.topic_cluster.delete_topic_metadata(topic_name).await
            }
            BrokerMode::Cluster => self.topic_cluster.post_delete_topic(topic_name).await,
            BrokerMode::Edge => {
                // Edge mode: local cleanup only. The topic will be removed from
                // the cluster on next RegisterEdge if removed from edge.yaml.
                if let Some(edge_rep) = self.edge_replicator.as_ref() {
                    // Stop WAL tailing for this topic
                    edge_rep.remove_topic(topic_name).await;
                }

                // Clean up locally (registry + Raft metadata)
                self.topic_registry.remove_topic(topic_name);
                self.topic_cluster.delete_topic_metadata(topic_name).await
            }
        }
    }

    // =====================================================================
    // Lookups and partitions
    // =====================================================================
    /// Looks up the broker serving `topic_name`. Returns (is_local, broker_url, connect_url).
    pub(crate) async fn lookup_topic(&self, topic_name: &str) -> Option<(bool, String, String)> {
        // Served locally
        if self.topic_registry.contains_topic(topic_name) {
            return Some((true, String::new(), String::new()));
        }

        // Ask cluster for serving broker address
        self.topic_cluster
            .find_serving_broker(topic_name)
            .await
            .map(|(broker_url, connect_url)| (false, broker_url, connect_url))
    }

    /// Returns the broker_id that currently serves the topic, if any.
    pub(crate) async fn get_topic_broker_id(&self, topic_name: &str) -> Option<String> {
        self.resources
            .cluster
            .get_broker_for_topic(topic_name)
            .await
    }

    /// Retrieves partition names if `topic_name` is partitioned, or returns the single topic.
    pub(crate) async fn topic_partitions(&self, topic_name: &str) -> Vec<String> {
        let mut topics = Vec::new();
        let ns_name = get_nsname_from_topic(topic_name);

        // check if the topic exist in the Metadata Store
        // if true, means that it is not a partitioned topic
        match self
            .resources
            .namespace
            .check_if_topic_exist(ns_name, topic_name)
            .await
        {
            true => {
                topics.push(topic_name.to_owned());
                return topics;
            }
            false => {}
        };

        // if not, we should look for any topic starting with topic_name

        topics = self
            .resources
            .namespace
            .get_topic_partitions(ns_name, topic_name)
            .await;

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
        let topic = self.topic_registry.get_topic(topic_name)?;
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
        if let Some(topic) = self.topic_registry.get_topic(&topic_name) {
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
    // Messaging (delegated to TopicRegistry)
    // =====================================================================

    /// Publishes a message asynchronously via the topic registry.
    pub async fn publish_message_async(
        &self,
        topic_name: String,
        message: StreamMessage,
    ) -> Result<()> {
        // Route message through the topic registry for async processing
        self.topic_registry
            .publish_message_async(topic_name, message)
            .await
    }

    // Async version of subscription using the topic registry
    pub(crate) async fn subscribe_async(
        &self,
        topic_name: String,
        subscription_options: SubscriptionOptions,
    ) -> Result<u64> {
        self.topic_manager
            .subscribe(topic_name, subscription_options)
            .await
    }

    /// Acknowledges a message asynchronously via the topic registry.
    pub(crate) async fn ack_message_async(&self, ack_msg: AckMessage) -> Result<()> {
        self.topic_registry.ack_message_async(ack_msg).await
    }

    pub(crate) async fn nack_message_async(&self, nack_msg: NackMessage) -> Result<()> {
        self.topic_registry.nack_message_async(nack_msg).await
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
        match self
            .resources
            .namespace
            .namespace_exist(namespace_name)
            .await
        {
            Ok(true) => {
                return Err(anyhow!("Namespace {} already exists.", namespace_name));
            }
            Ok(false) => {
                self.resources
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
        self.resources.namespace.delete_namespace(ns_name).await?;
        Ok(())
    }

    // Shutdown the broker service and all topic state
    #[allow(dead_code)]
    /// Shuts down the topic registry gracefully.
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        let topic_count = self.topic_registry.get_all_topics().len();
        info!(
            broker_id = %self.broker_id,
            topic_count = %topic_count,
            "shutting down broker service"
        );

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
