use anyhow::{anyhow, Result};
use danube_core::{
    dispatch_strategy::ConfigDispatchStrategy,
    message::StreamMessage,
    storage::{PersistentStorage, StartPosition, TopicStream},
};
use danube_persistent_storage::WalStorage;
use metrics::{counter, gauge, histogram};
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::time::Duration;
use tracing::{info, warn};

use crate::{
    broker_metrics::{
        SCHEMA_VALIDATION_FAILURES_TOTAL, SCHEMA_VALIDATION_TOTAL, TOPIC_ACTIVE_SUBSCRIPTIONS,
        TOPIC_BYTES_IN_TOTAL, TOPIC_MESSAGES_IN_TOTAL, TOPIC_MESSAGE_SIZE_BYTES,
    },
    dispatcher::DispatchStrategy,
    message::AckMessage,
    policies::Policies,
    producer::Producer,
    rate_limiter::RateLimiter,
    resources::{SchemaResources, TopicResources},
    // Phase 6: Old Schema type removed - using SchemaResources for schema operations
    schema::ValidationPolicy,
    subscription::{Subscription, SubscriptionOptions},
};
use danube_core::proto::SchemaReference;

#[cfg(test)]
#[path = "topic_tests.rs"]
mod topic_tests;

pub(crate) static SYSTEM_TOPIC: &str = "/system/_events_topic";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TopicState {
    Active,
    Draining,
    Closed,
}

// (moved helper/validation methods after struct definition)

// Topic
//
// Manage its own producers and subscriptions. This includes maintaining the state of producers
// and subscriptions and handling message publishing and consumption.
//
// Topics are responsible for accepting messages from producers
// and ensuring they are delivered to the correct subscriptions.
//
// Topics string representation:  /{namespace}/{topic-name}
//
#[derive(Debug)]
pub(crate) struct Topic {
    pub(crate) topic_name: String,
    // Phase 6: Legacy schema field removed - using schema_ref with SchemaRegistry
    // pub(crate) schema: Option<Schema>,
    // New schema registry reference (wrapped in Mutex for Arc access)
    schema_ref: Mutex<Option<SchemaReference>>,
    // Resolved schema ID for fast validation (cached from registry, wrapped in Mutex)
    resolved_schema_id: Mutex<Option<u64>>,
    // Schema validation policy (None/Warn/Enforce for schema ID validation)
    pub(crate) validation_policy: ValidationPolicy,
    // Enable deep payload validation (default: false, for performance)
    enable_payload_validation: bool,
    pub(crate) topic_policies: Option<Policies>,
    // subscription_name -> Subscription
    pub(crate) subscriptions: Mutex<HashMap<String, Subscription>>,
    // the producers currently connected to this topic, producer_id -> Producer
    pub(crate) producers: Mutex<HashMap<u64, Producer>>,
    // the retention strategy for the topic, Reliable vs NonReliable
    pub(crate) dispatch_strategy: DispatchStrategy,
    pub(crate) notifiers: Mutex<Vec<Arc<Notify>>>,
    // handle to metadata topic resources for cleanup operations
    resources_topic: TopicResources,
    // handle to schema resources for schema resolution
    resources_schema: SchemaResources,
    // unified dispatcher TopicStore facade (per-topic WAL/Cloud access)
    topic_store: Option<TopicStore>,
    // topic state for orchestrations like unload
    state: Mutex<TopicState>,
    // Phase 2: optional topic-level publish rate limiter (messages/sec)
    pub(crate) publish_rate_limiter: Option<Arc<RateLimiter>>,
}

impl Topic {
    pub(crate) fn new(
        topic_name: &str,
        dispatch_strategy: ConfigDispatchStrategy,
        wal_storage: Option<WalStorage>,
        resources_topic: TopicResources,
        resources_schema: SchemaResources,
    ) -> Self {
        let topic_store = wal_storage.map(|ws| TopicStore::new(topic_name.to_string(), ws));
        let dispatch_strategy = match dispatch_strategy {
            ConfigDispatchStrategy::NonReliable => DispatchStrategy::NonReliable,
            ConfigDispatchStrategy::Reliable => DispatchStrategy::Reliable,
        };

        Topic {
            topic_name: topic_name.into(),
            // Phase 6: schema field removed, new fields wrapped in Mutex
            schema_ref: Mutex::new(None),
            resolved_schema_id: Mutex::new(None),
            validation_policy: ValidationPolicy::None,
            enable_payload_validation: false, // Disabled by default for performance
            topic_policies: None,
            subscriptions: Mutex::new(HashMap::new()),
            producers: Mutex::new(HashMap::new()),
            dispatch_strategy,
            notifiers: Mutex::new(Vec::new()),
            resources_topic,
            resources_schema,
            topic_store,
            state: Mutex::new(TopicState::Active),
            publish_rate_limiter: None,
        }
    }

    #[allow(unused_assignments)]
    pub(crate) async fn create_producer(
        &self,
        producer_id: u64,
        producer_name: &str,
        producer_access_mode: i32,
    ) -> Result<serde_json::Value> {
        // Policy: max_producers_per_topic
        self.can_add_producer().await?;
        let mut producer_config = serde_json::Value::String(String::new());
        let mut producers = self.producers.lock().await;

        match producers.entry(producer_id) {
            Entry::Vacant(entry) => {
                let new_producer = Producer::new(
                    producer_id,
                    producer_name.into(),
                    self.topic_name.clone(),
                    producer_access_mode,
                );

                producer_config = serde_json::to_value(&new_producer)?;

                entry.insert(new_producer);
            }
            Entry::Occupied(entry) => {
                //let current_producer = entry.get();
                info!("the requested producer: {} already exists", entry.key());
                return Err(anyhow!(" the producer already exist"));
            }
        }
        Ok(producer_config)
    }

    // Close this topic - disconnect all producers and subscriptions associated with this topic
    pub(crate) async fn close(&self) -> Result<(Vec<u64>, Vec<u64>)> {
        let mut disconnected_producers = Vec::new();
        let mut disconnected_consumers = Vec::new();

        // Disconnect all the topic producers
        {
            let mut producers = self.producers.lock().await;
            for (_, producer) in producers.iter_mut() {
                let producer_id = producer.disconnect();
                disconnected_producers.push(producer_id);
            }
        }

        // Disconnect all the topic subscriptions
        let mut subs_guard = self.subscriptions.lock().await;
        for (_, subscription) in subs_guard.iter_mut() {
            let mut consumers = subscription.disconnect().await?;
            disconnected_consumers.append(&mut consumers);
        }
        // Decrement subscriptions gauge for all existing
        let subs_len = subs_guard.len();
        if subs_len > 0 {
            gauge!(TOPIC_ACTIVE_SUBSCRIPTIONS.name, "topic" => self.topic_name.clone())
                .decrement(subs_len as f64);
        }

        Ok((disconnected_producers, disconnected_consumers))
    }

    // Asynchronous version of publish_message for better performance
    pub(crate) async fn publish_message_async(&self, stream_message: StreamMessage) -> Result<()> {
        // Block publishes when draining
        {
            let state = self.state.lock().await;
            if *state == TopicState::Draining || *state == TopicState::Closed {
                return Err(anyhow!(
                    "Topic {} is draining or closed; retry lookup/moved",
                    self.topic_name
                ));
            }
        }
        // Phase 3: publish rate limiting (if configured)
        if let Some(lim) = &self.publish_rate_limiter {
            if !lim.try_acquire(1).await {
                warn!(
                    "Publish rate limit exceeded for topic {} (warn-only)",
                    self.topic_name
                );
            }
        }
        // Record message size distribution early (bytes)
        histogram!(
            TOPIC_MESSAGE_SIZE_BYTES.name,
            "topic" => self.topic_name.clone()
        )
        .record(stream_message.payload.len() as f64);

        // Policy: max_message_size
        self.validate_message_size(stream_message.payload.len())?;

        // Schema validation (if enabled)
        self.validate_message_schema(&stream_message).await?;

        // Validate producer without blocking
        let producer_id = stream_message.msg_id.producer_id;
        {
            let producers = self.producers.lock().await;
            if !producers.contains_key(&producer_id) {
                return Err(anyhow!(
                    "the producer with id {} is not attached to topic name: {}",
                    producer_id,
                    self.topic_name
                ));
            }
        }

        // Update ingress counters (topic only)
        counter!(
            TOPIC_MESSAGES_IN_TOTAL.name,
            "topic"=> self.topic_name.clone()
        )
        .increment(1);
        counter!(
            TOPIC_BYTES_IN_TOTAL.name,
            "topic"=> self.topic_name.clone()
        )
        .increment(stream_message.payload.len() as u64);

        // Process message based on dispatch strategy
        match &self.dispatch_strategy {
            DispatchStrategy::NonReliable => {
                self.dispatch_to_subscriptions_async(stream_message).await
            }
            DispatchStrategy::Reliable => {
                // Reliable: persist first, notify only on success (WAL append)
                if let Some(store) = &self.topic_store {
                    store.store_message(stream_message).await?;
                } else {
                    return Err(anyhow!("WAL is not configured for a reliable topic"));
                }

                let notifier_guard = self.notifiers.lock().await;
                for notifier in notifier_guard.iter() {
                    notifier.notify_one();
                }
                Ok(())
            }
        }
    }

    // Helper method for async subscription dispatch
    async fn dispatch_to_subscriptions_async(&self, stream_message: StreamMessage) -> Result<()> {
        // For now, we'll use a simpler approach without cloning subscriptions
        // TODO: Implement proper concurrent dispatch with Arc<Subscription> or interior mutability
        let subscription_names: Vec<String> = {
            let subscriptions = self.subscriptions.lock().await;
            subscriptions.keys().cloned().collect()
        };

        // For now, dispatch synchronously to avoid Arc<Subscription> issues
        // TODO: Implement proper async concurrent dispatch
        let mut subscriptions_to_remove = Vec::new();

        for subscription_name in &subscription_names {
            let subscriptions = self.subscriptions.lock().await;
            if let Some(subscription) = subscriptions.get(subscription_name) {
                let result = subscription
                    .send_message_to_dispatcher(stream_message.clone())
                    .await;
                if let Err(err) = result {
                    info!(
                        "The subscription {}, has no active consumers, got error: {} ",
                        subscription_name, err
                    );
                    subscriptions_to_remove.push(subscription_name.clone());
                }
            }
        }

        // Clean up failed subscriptions
        for subscription_name in subscriptions_to_remove {
            self.unsubscribe(&subscription_name).await;
            // Best-effort delete from metadata store
            self.delete_subscription_metadata(&subscription_name).await;
        }

        Ok(())
    }

    /// Transition topic to Draining: new publishes will be rejected.
    pub(crate) async fn unavailable_topic(&self) {
        let mut st = self.state.lock().await;
        *st = TopicState::Draining;
    }

    // Note: pausing is handled via dispatcher disconnect on each subscription.

    // Best-effort deletion of subscription from metadata store
    async fn delete_subscription_metadata(&self, subscription_name: &str) {
        let mut topic_res = self.resources_topic.clone();
        let _ = topic_res
            .delete_subscription(subscription_name, &self.topic_name)
            .await;
    }

    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        let mut subscriptions = self.subscriptions.lock().await;
        let subscription = subscriptions
            .get_mut(ack_msg.subscription_name.as_str())
            .ok_or_else(|| anyhow!("Subscription not found"))?;
        subscription.ack_message(ack_msg).await?;
        Ok(())
    }

    pub(crate) async fn get_producer_status(&self, producer_id: u64) -> bool {
        let producers = self.producers.lock().await;
        if let Some(producer) = producers.get(&producer_id) {
            if producer.status == true {
                return true;
            }
        }
        false
    }

    // Subscribe to the topic and create a consumer for receiving messages
    pub(crate) async fn subscribe(
        &self,
        topic_name: &str,
        options: SubscriptionOptions,
    ) -> Result<u64> {
        //Todo! sub_metadata is user-defined information to the subscription,
        //maybe for user internal business, management and montoring
        let sub_metadata = HashMap::new();

        // Check if subscription already exists without holding the lock across awaits
        let is_new = {
            let subs = self.subscriptions.lock().await;
            !subs.contains_key(&options.subscription_name)
        };

        if is_new {
            // Policy: max_subscriptions_per_topic (only when creating a new subscription)
            self.can_add_subscription().await?;

            // Build the subscription and dispatcher without holding the lock
            let mut new_subscription =
                Subscription::new(options.clone(), &self.topic_name, sub_metadata);
            // Phase 2: install per-subscription dispatch limiter if configured
            if let Some(pol) = &self.topic_policies {
                let sub_rate = pol.get_max_subscription_dispatch_rate();
                if sub_rate > 0 {
                    new_subscription
                        .set_dispatch_rate_limiter(Some(Arc::new(RateLimiter::new(sub_rate))));
                }
            }

            if let DispatchStrategy::Reliable = &self.dispatch_strategy {
                let notifier = new_subscription
                    .create_new_dispatcher(
                        options.clone(),
                        &self.dispatch_strategy,
                        self.topic_store.clone(),
                        Some(self.resources_topic.clone()),
                        Some(Duration::from_secs(10)),
                    )
                    .await?;
                if let Some(notifier) = notifier {
                    self.notifiers.lock().await.push(notifier);
                }
            } else {
                let _ = new_subscription
                    .create_new_dispatcher(
                        options.clone(),
                        &self.dispatch_strategy,
                        None,
                        None,
                        None,
                    )
                    .await?;
            }

            // Insert the new subscription
            let mut subs = self.subscriptions.lock().await;
            subs.insert(options.subscription_name.clone(), new_subscription);
            // Gauge: topic active subscriptions ++
            gauge!(TOPIC_ACTIVE_SUBSCRIPTIONS.name, "topic" => self.topic_name.clone())
                .increment(1.0);
        }

        // Policy: consumer limits (per-subscription and per-topic)
        self.can_add_consumer_to_subscription(&options.subscription_name)
            .await?;

        // Retrieve the subscription and proceed
        let mut subs = self.subscriptions.lock().await;
        let subscription = subs
            .get_mut(&options.subscription_name)
            .expect("subscription must exist at this point");

        if subscription.is_exclusive() && subscription.has_consumers() {
            warn!("Not allowed to add the Consumer: {}, the Exclusive subscription can't be shared with other consumers", options.consumer_name);
            return Err(anyhow!("Not allowed to add the Consumer: {}, the Exclusive subscription can't be shared with other consumers", options.consumer_name));
        }

        let consumer_id = subscription.add_consumer(topic_name, options).await?;

        Ok(consumer_id)
    }

    // Unsubscribes the specified subscription from the topic
    // should be called if all consumers are disconnected
    pub(crate) async fn unsubscribe(&self, subscription_name: &str) {
        let removed = self.subscriptions.lock().await.remove(subscription_name);
        if removed.is_some() {
            gauge!(TOPIC_ACTIVE_SUBSCRIPTIONS.name, "topic" => self.topic_name.clone())
                .decrement(1.0);
        }
    }

    pub(crate) async fn validate_consumer(
        &self,
        subscription_name: &str,
        consumer_name: &str,
    ) -> Option<u64> {
        let sub_guard = self.subscriptions.lock().await;
        let subscription = match sub_guard.get(subscription_name) {
            Some(subscr) => subscr,
            None => return None,
        };

        let consumer_id = match subscription.validate_consumer(consumer_name).await {
            Some(id) => id,
            None => return None,
        };

        Some(consumer_id)
    }

    // check_subscription checks if the subscription is activelly used by any consumer
    pub(crate) async fn check_subscription(&self, subscription: &str) -> Option<bool> {
        let sub_guard = self.subscriptions.lock().await;
        let subs = sub_guard.get(subscription)?;

        let consumers = subs.get_consumers();

        for consumer_info in consumers {
            if consumer_info.get_status().await {
                return Some(true);
            }
        }

        Some(false)
    }

    // Update Topic Policies
    pub(crate) fn policies_update(&mut self, policies: Policies) -> Result<()> {
        self.topic_policies = Some(policies);
        // Initialize optional limiters based on policies (>0)
        if let Some(p) = &self.topic_policies {
            let pub_rate = p.get_max_publish_rate();
            self.publish_rate_limiter = if pub_rate > 0 {
                Some(Arc::new(RateLimiter::new(pub_rate)))
            } else {
                None
            };
        }
        Ok(())
    }

    // Phase 6: Old schema methods commented out - use SchemaRegistry instead
    // pub(crate) fn add_schema(&mut self, schema: Schema) -> Result<()> {
    //     self.schema = Some(schema);
    //     Ok(())
    // }
    //
    // #[allow(dead_code)]
    // pub(crate) fn get_schema(&self) -> Option<Schema> {
    //     self.schema.clone()
    // }
    //
    // #[allow(dead_code)]
    // pub(crate) fn delete_schema(&self, _schema: Schema) -> Result<()> {
    //     todo!()
    // }

    // ===== New Schema Registry Methods =====

    /// Set schema reference and resolve to schema ID
    /// This should be called when topic loads or when producer sets schema
    pub(crate) fn set_schema_ref(&self, schema_ref: SchemaReference) -> Result<()> {
        let subject = schema_ref.subject.clone();

        // Resolve schema_ref to schema_id using SchemaResources
        if let Some(schema_id) = self.resources_schema.get_schema_id(&subject) {
            // Update both fields atomically
            if let Ok(mut schema_ref_guard) = self.schema_ref.try_lock() {
                *schema_ref_guard = Some(schema_ref);
            }
            if let Ok(mut resolved_id_guard) = self.resolved_schema_id.try_lock() {
                *resolved_id_guard = Some(schema_id);
            }

            info!(
                "Topic {} resolved schema: subject={}, schema_id={}",
                self.topic_name, subject, schema_id
            );
            Ok(())
        } else {
            // Schema not in cache yet - may need to be registered first
            warn!(
                "Schema not found in cache for subject: {}. Producer should register schema first.",
                subject
            );
            Err(anyhow!(
                "Schema '{}' not found in registry. Please register schema before producing messages.",
                subject
            ))
        }
    }

    /// Set validation policy for this topic
    #[allow(dead_code)]
    pub(crate) fn set_validation_policy(&mut self, policy: ValidationPolicy) {
        self.validation_policy = policy;
    }

    /// Enable or disable deep payload validation (default: false for performance)
    /// When enabled, validates message payload content against schema definition
    #[allow(dead_code)]
    pub(crate) fn set_payload_validation(&mut self, enabled: bool) {
        self.enable_payload_validation = enabled;
    }

    /// Validate a message against topic's schema
    /// Returns Ok(()) if validation passes, Err if fails
    pub(crate) async fn validate_message_schema(&self, message: &StreamMessage) -> Result<()> {
        // Skip validation if policy is None
        if matches!(self.validation_policy, ValidationPolicy::None) {
            return Ok(());
        }

        // Record validation attempt
        counter!(
            SCHEMA_VALIDATION_TOTAL.name,
            "topic" => self.topic_name.clone(),
            "policy" => format!("{:?}", self.validation_policy)
        )
        .increment(1);

        // Check if topic has schema enforcement enabled
        let expected_schema_id = {
            let resolved_id_guard = self
                .resolved_schema_id
                .try_lock()
                .map_err(|_| anyhow!("Failed to acquire schema lock"))?;

            match *resolved_id_guard {
                Some(id) => id,
                None => {
                    // No schema set for topic
                    if matches!(self.validation_policy, ValidationPolicy::Enforce) {
                        return Err(anyhow!(
                            "Topic {} requires schema but none is configured",
                            self.topic_name
                        ));
                    }
                    return Ok(()); // Warn mode: allow messages without schema
                }
            }
        };

        // Check if message has schema_id
        let message_schema_id = match message.schema_id {
            Some(id) => id,
            None => {
                let err = anyhow!(
                    "Message missing schema_id for topic {} (expected schema_id: {})",
                    self.topic_name,
                    expected_schema_id
                );

                // Record failure
                counter!(
                    SCHEMA_VALIDATION_FAILURES_TOTAL.name,
                    "topic" => self.topic_name.clone(),
                    "reason" => "missing_schema_id"
                )
                .increment(1);

                match self.validation_policy {
                    ValidationPolicy::Warn => {
                        warn!("{}", err);
                        return Ok(()); // Warn but allow
                    }
                    ValidationPolicy::Enforce => return Err(err),
                    ValidationPolicy::None => return Ok(()),
                }
            }
        };

        // Validate schema_id matches
        if message_schema_id != expected_schema_id {
            let err = anyhow!(
                "Schema mismatch for topic {}: message has schema_id={}, expected={}",
                self.topic_name,
                message_schema_id,
                expected_schema_id
            );

            // Record failure
            counter!(
                SCHEMA_VALIDATION_FAILURES_TOTAL.name,
                "topic" => self.topic_name.clone(),
                "reason" => "schema_mismatch"
            )
            .increment(1);

            match self.validation_policy {
                ValidationPolicy::Warn => {
                    warn!("{}", err);
                    Ok(()) // Warn but allow
                }
                ValidationPolicy::Enforce => Err(err),
                ValidationPolicy::None => Ok(()),
            }
        } else {
            // Schema ID validation passed

            // Phase 6: Deep payload validation (optional, disabled by default)
            if self.enable_payload_validation {
                self.validate_payload_content(message).await?;
            }

            Ok(())
        }
    }

    /// Validate message payload content against schema definition (deep validation)
    /// This is CPU-intensive and disabled by default
    async fn validate_payload_content(&self, message: &StreamMessage) -> Result<()> {
        use crate::schema::validator::ValidatorFactory;

        // Get schema definition from resources
        let schema_id_guard = self
            .resolved_schema_id
            .try_lock()
            .map_err(|_| anyhow!("Failed to acquire schema lock"))?;

        let _schema_id = match *schema_id_guard {
            Some(id) => id,
            None => return Ok(()), // No schema configured, skip validation
        };
        drop(schema_id_guard);

        // Get the subject from schema_ref
        let schema_ref_guard = self
            .schema_ref
            .try_lock()
            .map_err(|_| anyhow!("Failed to acquire schema ref lock"))?;

        let subject = match &*schema_ref_guard {
            Some(ref_val) => ref_val.subject.clone(),
            None => return Ok(()), // No subject, skip
        };
        drop(schema_ref_guard);

        // Fetch schema version from resources
        let version = message.schema_version.unwrap_or(1);
        let schema_version = self
            .resources_schema
            .get_version(&subject, version)
            .await
            .map_err(|e| anyhow!("Schema version not found: {}/{} - {}", subject, version, e))?;

        // Create validator and validate payload
        let validator = ValidatorFactory::create(&schema_version.schema_def)
            .map_err(|e| anyhow!("Failed to create validator: {}", e))?;

        validator.validate(&message.payload).map_err(|e| {
            // Record payload validation failure
            counter!(
                SCHEMA_VALIDATION_FAILURES_TOTAL.name,
                "topic" => self.topic_name.clone(),
                "reason" => "payload_invalid"
            )
            .increment(1);

            anyhow!("Payload validation failed: {}", e)
        })?;

        Ok(())
    }
}

impl Topic {
    // ===== Helper counters =====
    pub(crate) async fn producer_count(&self) -> usize {
        let producers = self.producers.lock().await;
        producers.len()
    }

    pub(crate) async fn subscription_count(&self) -> usize {
        let subscriptions = self.subscriptions.lock().await;
        subscriptions.len()
    }

    pub(crate) async fn total_consumer_count(&self) -> usize {
        let subscriptions = self.subscriptions.lock().await;
        let mut total = 0usize;
        for (_name, sub) in subscriptions.iter() {
            total += sub.consumer_count();
        }
        total
    }

    // ===== Policy validations =====
    pub(crate) async fn can_add_producer(&self) -> Result<()> {
        let limit = self
            .topic_policies
            .as_ref()
            .map(|p| p.get_max_producers_per_topic())
            .unwrap_or(0);
        if limit == 0 {
            return Ok(());
        }
        let current = self.producer_count().await as u32;
        if current >= limit {
            return Err(anyhow!(
                "Producer limit reached for topic {}. Current: {}, Limit: {}",
                self.topic_name,
                current,
                limit
            ));
        }
        Ok(())
    }

    pub(crate) async fn can_add_subscription(&self) -> Result<()> {
        let limit = self
            .topic_policies
            .as_ref()
            .map(|p| p.get_max_subscriptions_per_topic())
            .unwrap_or(0);
        if limit == 0 {
            return Ok(());
        }
        let current = self.subscription_count().await as u32;
        if current >= limit {
            return Err(anyhow!(
                "Subscription limit reached for topic {}. Current: {}, Limit: {}",
                self.topic_name,
                current,
                limit
            ));
        }
        Ok(())
    }

    pub(crate) async fn can_add_consumer_to_subscription(&self, sub_name: &str) -> Result<()> {
        // Per-subscription limit
        let per_sub_limit = self
            .topic_policies
            .as_ref()
            .map(|p| p.get_max_consumers_per_subscription())
            .unwrap_or(0);
        if per_sub_limit > 0 {
            let subscriptions = self.subscriptions.lock().await;
            if let Some(sub) = subscriptions.get(sub_name) {
                let current = sub.consumer_count() as u32;
                if current >= per_sub_limit {
                    return Err(anyhow!(
                        "Consumer limit per subscription reached on topic {} subscription {}. Current: {}, Limit: {}",
                        self.topic_name, sub_name, current, per_sub_limit
                    ));
                }
            }
        }
        let topic_limit = self
            .topic_policies
            .as_ref()
            .map(|p| p.get_max_consumers_per_topic())
            .unwrap_or(0);
        if topic_limit > 0 {
            let current_total = self.total_consumer_count().await as u32;
            if current_total >= topic_limit {
                return Err(anyhow!(
                    "Consumer limit per topic reached for {}. Current: {}, Limit: {}",
                    self.topic_name,
                    current_total,
                    topic_limit
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_message_size(&self, size: usize) -> Result<()> {
        let max = self
            .topic_policies
            .as_ref()
            .map(|p| p.get_max_message_size())
            .unwrap_or(0);
        if max == 0 {
            return Ok(());
        }
        if (size as u32) > max {
            return Err(anyhow!(
                "Message size {} exceeds maximum allowed {} for topic {}",
                size,
                max,
                self.topic_name
            ));
        }
        Ok(())
    }
}

// TopicStore is a thin facade over PersistentStorage (WalStorage) scoped to a single topic.
// It provides a simple API for appending messages and creating readers starting at a given position.
#[derive(Debug, Clone)]
pub(crate) struct TopicStore {
    topic_name: String,
    storage: WalStorage,
}

impl TopicStore {
    pub(crate) fn new(topic_name: String, storage: WalStorage) -> Self {
        Self {
            topic_name,
            storage,
        }
    }

    /// Append a message to the WAL and return its offset.
    pub(crate) async fn store_message(&self, message: StreamMessage) -> anyhow::Result<u64> {
        let off = self
            .storage
            .append_message(&self.topic_name, message)
            .await?;
        Ok(off)
    }

    /// Create a stream reader starting at `start` using WAL tail or CloudReader handoff.
    pub(crate) async fn create_reader(&self, start: StartPosition) -> anyhow::Result<TopicStream> {
        let stream = self.storage.create_reader(&self.topic_name, start).await?;
        Ok(stream)
    }

    /// Returns the last committed offset in the WAL for this topic.
    /// This represents the "head" of the topic - the highest offset written.
    /// The next message will be assigned offset = head.
    ///
    /// This is used by the lag detection mechanism to determine if a subscription
    /// is behind the current state of the topic.
    pub(crate) fn get_last_committed_offset(&self) -> u64 {
        self.storage.current_offset()
    }
}
