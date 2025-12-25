use anyhow::{anyhow, Ok, Result};
use danube_core::message::StreamMessage;
use metrics::gauge;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::{trace, warn};

use crate::{
    broker_metrics::{SUBSCRIPTION_ACTIVE_CONSUMERS, TOPIC_ACTIVE_CONSUMERS},
    consumer::{Consumer, ConsumerSession},
    dispatch_strategy::DispatchStrategy,
    dispatcher::subscription_engine::SubscriptionEngine,
    dispatcher::{
        unified_multiple::UnifiedMultipleDispatcher, unified_single::UnifiedSingleDispatcher,
        Dispatcher,
    },
    message::AckMessage,
    rate_limiter::RateLimiter,
    resources::TopicResources,
    topic::TopicStore,
    utils::get_random_id,
};

// Subscriptions manage the consumers that are subscribed to them.
// They also handle dispatchers that manage the distribution of messages to these consumers.
#[derive(Debug)]
pub(crate) struct Subscription {
    pub(crate) subscription_name: String,
    pub(crate) subscription_type: i32,
    #[allow(dead_code)]
    pub(crate) topic_name: String,
    pub(crate) dispatcher: Option<Dispatcher>,
    pub(crate) consumers: HashMap<u64, Consumer>,
    // Phase 2/3: optional per-subscription dispatch limiter (messages/sec)
    pub(crate) dispatch_rate_limiter: Option<Arc<RateLimiter>>,
}

// ConsumerInfo removed - Consumer now contains all necessary state via ConsumerSession

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SubscriptionOptions {
    pub(crate) subscription_name: String,
    pub(crate) subscription_type: i32, // should be moved to SubscriptionType
    pub(crate) consumer_id: Option<u64>,
    pub(crate) consumer_name: String,
}

impl Subscription {
    // create new subscription
    pub(crate) fn new(
        sub_options: SubscriptionOptions,
        topic_name: &str,
        _meta_properties: HashMap<String, String>,
    ) -> Self {
        Subscription {
            subscription_name: sub_options.subscription_name,
            subscription_type: sub_options.subscription_type,
            topic_name: topic_name.into(),
            dispatcher: None,
            consumers: HashMap::new(),
            dispatch_rate_limiter: None,
        }
    }
    // Phase 2: setter to install a limiter created by Topic based on policies
    pub(crate) fn set_dispatch_rate_limiter(&mut self, limiter: Option<Arc<RateLimiter>>) {
        self.dispatch_rate_limiter = limiter;
    }
    // Adds a consumer to the subscription
    pub(crate) async fn add_consumer(
        &mut self,
        topic_name: &str,
        options: SubscriptionOptions,
    ) -> Result<u64> {
        //for communication with client consumer
        let (tx_cons, rx_cons) = mpsc::channel(4);

        let consumer_id = get_random_id();
        let session = Arc::new(Mutex::new(ConsumerSession::new(rx_cons)));
        let consumer = Consumer::new(
            consumer_id,
            &options.consumer_name,
            options.subscription_type,
            topic_name,
            &self.subscription_name,
            tx_cons,
            session.clone(),
        );

        let dispatcher = self.dispatcher.as_mut().unwrap();
        // Add the consumer to the dispatcher
        dispatcher.add_consumer(consumer.clone()).await?;

        // Insert the consumer into the subscription's consumer list
        self.consumers.insert(consumer_id, consumer);

        // Gauge: active consumers per subscription ++
        gauge!(
            SUBSCRIPTION_ACTIVE_CONSUMERS.name,
            "topic" => self.topic_name.to_string(),
            "subscription" => self.subscription_name.clone()
        )
        .increment(1.0);

        trace!(
            "A dispatcher {:?} has been added on subscription {}",
            &dispatcher,
            self.subscription_name
        );

        Ok(consumer_id)
    }

    pub(crate) async fn create_new_dispatcher(
        &mut self,
        options: SubscriptionOptions,
        dispatch_strategy: &DispatchStrategy,
        topic_store: Option<TopicStore>,
        topic_resources: Option<TopicResources>,
        sub_progress_flush_interval: Option<Duration>,
    ) -> Result<Option<Arc<Notify>>> {
        let (new_dispatcher, notifier) = match dispatch_strategy {
            DispatchStrategy::NonReliable => match options.subscription_type {
                // Exclusive
                0 => (
                    Dispatcher::UnifiedOneConsumer(UnifiedSingleDispatcher::new_non_reliable()),
                    None,
                ),

                // Shared
                1 => (
                    Dispatcher::UnifiedMultipleConsumers(
                        UnifiedMultipleDispatcher::new_non_reliable(),
                    ),
                    None,
                ),

                // Failover
                2 => (
                    Dispatcher::UnifiedOneConsumer(UnifiedSingleDispatcher::new_non_reliable()),
                    None,
                ),

                _ => {
                    return Err(anyhow!("Should not get here"));
                }
            },
            DispatchStrategy::Reliable => {
                // Use unified reliable dispatchers with ack-gating via SubscriptionEngine over TopicStore
                let ts = topic_store
                    .ok_or_else(|| anyhow!("TopicStore not provided for reliable dispatcher"))?;
                match options.subscription_type {
                    // Exclusive
                    0 => {
                        let tr = topic_resources
                            .clone()
                            .expect("progress resources must be provided for reliable dispatcher");
                        let engine = SubscriptionEngine::new_with_progress(
                            options.subscription_name.clone(),
                            self.topic_name.clone(),
                            Arc::new(ts.clone()),
                            tr,
                            sub_progress_flush_interval.unwrap_or(Duration::from_secs(5)),
                            self.dispatch_rate_limiter.clone(),
                        );
                        let new_dispatcher = UnifiedSingleDispatcher::new_reliable(engine);
                        // Ensure reliable dispatcher is initialized before exposing notifier
                        new_dispatcher.ready().await;
                        let notifier = new_dispatcher.get_notifier();
                        (
                            Dispatcher::UnifiedOneConsumer(new_dispatcher),
                            Some(notifier),
                        )
                    }

                    // Shared
                    1 => {
                        let tr = topic_resources
                            .clone()
                            .expect("progress resources must be provided for reliable dispatcher");
                        let engine = SubscriptionEngine::new_with_progress(
                            options.subscription_name.clone(),
                            self.topic_name.clone(),
                            Arc::new(ts.clone()),
                            tr,
                            sub_progress_flush_interval.unwrap_or(Duration::from_secs(5)),
                            self.dispatch_rate_limiter.clone(),
                        );
                        let new_dispatcher = UnifiedMultipleDispatcher::new_reliable(engine);
                        // Ensure reliable dispatcher is initialized before exposing notifier
                        new_dispatcher.ready().await;
                        let notifier = new_dispatcher.get_notifier();
                        (
                            Dispatcher::UnifiedMultipleConsumers(new_dispatcher),
                            Some(notifier),
                        )
                    }

                    // Failover (treat as single active consumer)
                    2 => {
                        let tr = topic_resources
                            .clone()
                            .expect("progress resources must be provided for reliable dispatcher");
                        let engine = SubscriptionEngine::new_with_progress(
                            options.subscription_name.clone(),
                            self.topic_name.clone(),
                            Arc::new(ts.clone()),
                            tr,
                            sub_progress_flush_interval.unwrap_or(Duration::from_secs(5)),
                            self.dispatch_rate_limiter.clone(),
                        );
                        let new_dispatcher = UnifiedSingleDispatcher::new_reliable(engine);
                        let notifier = new_dispatcher.get_notifier();
                        (
                            Dispatcher::UnifiedOneConsumer(new_dispatcher),
                            Some(notifier),
                        )
                    }

                    _ => {
                        return Err(anyhow!("Should not get here"));
                    }
                }
            }
        };

        self.dispatcher = Some(new_dispatcher);

        Ok(notifier)
    }

    pub(crate) async fn send_message_to_dispatcher(&self, message: StreamMessage) -> Result<()> {
        // Non-reliable path uses this method. If a per-subscription limiter exists and denies,
        // warn-only and proceed (do not drop) per request.
        if let Some(lim) = &self.dispatch_rate_limiter {
            if !lim.try_acquire(1).await {
                warn!(
                    "Dispatch rate limit exceeded for subscription {} on topic {} (warn-only)",
                    self.subscription_name, self.topic_name
                );
            }
        }
        // Try to send the message
        if let Some(dispatcher) = self.dispatcher.as_ref() {
            dispatcher.dispatch_message(message).await?;
        } else {
            return Err(anyhow!("Dispatcher not initialized"));
        }
        Ok(())
    }

    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        if let Some(dispatcher) = self.dispatcher.as_ref() {
            dispatcher.ack_message(ack_msg).await?;
        } else {
            return Err(anyhow!("Dispatcher not initialized"));
        }
        Ok(())
    }

    pub(crate) fn get_consumer(&self, consumer_id: u64) -> Option<Consumer> {
        self.consumers.get(&consumer_id).cloned()
    }

    /// Returns the number of consumers in this subscription.
    /// Efficient: O(1) operation, no cloning.
    pub(crate) fn consumer_count(&self) -> usize {
        self.consumers.len()
    }

    /// Returns true if there are any consumers in this subscription.
    /// Efficient: O(1) operation, no cloning.
    pub(crate) fn has_consumers(&self) -> bool {
        !self.consumers.is_empty()
    }

    /// Get all consumers (clones all consumer instances).
    /// Use sparingly - prefer consumer_count() or has_consumers() when possible.
    pub(crate) fn get_consumers(&self) -> Vec<Consumer> {
        self.consumers.values().cloned().collect::<Vec<_>>()
    }

    // handles the disconnection of consumers associated with the subscription.
    pub(crate) async fn disconnect(&mut self) -> Result<Vec<u64>> {
        let mut consumers_id = Vec::new();

        for (consumer_id, consumer) in self.consumers.iter() {
            if consumer.get_status().await {
                // if consumer exist and its status is true, then set the status to false
                consumer.set_status_inactive().await;
                consumers_id.push(*consumer_id);
            }
            gauge!(TOPIC_ACTIVE_CONSUMERS.name, "topic" => self.topic_name.to_string())
                .decrement(1);
            gauge!(SUBSCRIPTION_ACTIVE_CONSUMERS.name, "topic" => self.topic_name.to_string(), "subscription" => self.subscription_name.clone()).decrement(1);
        }

        // Disconnect all consumers
        if let Some(dispatcher) = self.dispatcher.as_mut() {
            dispatcher.disconnect_all_consumers().await?;
        }

        Ok(consumers_id)
    }

    // Validate Consumer - returns consumer ID
    pub(crate) async fn validate_consumer(&self, consumer_name: &str) -> Option<u64> {
        for consumer in self.consumers.values() {
            if consumer.consumer_name == consumer_name {
                // if consumer exist and its status is false, then the consumer has disconnected
                // the consumer client may try to reconnect
                // then set the status to true and use the consumer
                if !consumer.get_status().await {
                    consumer.set_status_active().await;
                }
                return Some(consumer.consumer_id);
            }
        }
        None
    }

    pub(crate) fn is_exclusive(&self) -> bool {
        if self.subscription_type == 0 {
            return true;
        }
        return false;
    }
}
