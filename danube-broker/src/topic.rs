use anyhow::{anyhow, Result};
use danube_core::{dispatch_strategy::ConfigDispatchStrategy, message::StreamMessage};
use danube_persistent_storage::WalStorage;
use danube_reliable_dispatch::ReliableDispatch;
use metrics::counter;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tracing::{info, warn};

use crate::{
    broker_metrics::{TOPIC_BYTES_IN_COUNTER, TOPIC_MSG_IN_COUNTER},
    dispatch_strategy::DispatchStrategy,
    message::AckMessage,
    policies::Policies,
    producer::Producer,
    resources::TopicResources,
    schema::Schema,
    subscription::{Subscription, SubscriptionOptions},
};

pub(crate) static SYSTEM_TOPIC: &str = "/system/_events_topic";

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
    pub(crate) schema: Option<Schema>,
    pub(crate) topic_policies: Option<Policies>,
    // subscription_name -> Subscription
    pub(crate) subscriptions: Mutex<HashMap<String, Subscription>>,
    // the producers currently connected to this topic, producer_id -> Producer
    pub(crate) producers: Mutex<HashMap<u64, Producer>>,
    // the retention strategy for the topic, Reliable vs NonReliable
    pub(crate) dispatch_strategy: DispatchStrategy,
    notifiers: Mutex<Vec<Arc<Notify>>>,
    // handle to metadata topic resources for cleanup operations
    resources_topic: Option<TopicResources>,
}

impl Topic {
    pub(crate) fn new(
        topic_name: &str,
        dispatch_strategy: ConfigDispatchStrategy,
        wal_storage: WalStorage,
        resources_topic: Option<TopicResources>,
    ) -> Self {
        let dispatch_strategy = match dispatch_strategy {
            ConfigDispatchStrategy::NonReliable => DispatchStrategy::NonReliable,
            ConfigDispatchStrategy::Reliable(reliable_options) => DispatchStrategy::Reliable(
                ReliableDispatch::new(topic_name, reliable_options, wal_storage),
            ),
        };

        Topic {
            topic_name: topic_name.into(),
            schema: None,
            topic_policies: None,
            subscriptions: Mutex::new(HashMap::new()),
            producers: Mutex::new(HashMap::new()),
            dispatch_strategy,
            notifiers: Mutex::new(Vec::new()),
            resources_topic,
        }
    }

    #[allow(unused_assignments)]
    pub(crate) async fn create_producer(
        &self,
        producer_id: u64,
        producer_name: &str,
        producer_access_mode: i32,
    ) -> Result<serde_json::Value> {
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
        for (_, subscription) in self.subscriptions.lock().await.iter_mut() {
            let mut consumers = subscription.disconnect().await?;
            disconnected_consumers.append(&mut consumers);
        }

        Ok((disconnected_producers, disconnected_consumers))
    }

    // Asynchronous version of publish_message for better performance
    pub(crate) async fn publish_message_async(&self, stream_message: StreamMessage) -> Result<()> {
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

        // Update metrics
        counter!(TOPIC_MSG_IN_COUNTER.name, "topic"=> self.topic_name.clone() , "producer" => producer_id.to_string()).increment(1);
        counter!(TOPIC_BYTES_IN_COUNTER.name, "topic"=> self.topic_name.clone() , "producer" => producer_id.to_string()).increment(stream_message.payload.len() as u64);

        // Process message based on dispatch strategy
        match &self.dispatch_strategy {
            DispatchStrategy::NonReliable => {
                self.dispatch_to_subscriptions_async(stream_message).await
            }
            DispatchStrategy::Reliable(reliable_dispatch) => {
                // Reliable: persist first, notify only on success
                reliable_dispatch.store_message(stream_message).await?;

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

    // Best-effort deletion of subscription from metadata store
    async fn delete_subscription_metadata(&self, subscription_name: &str) {
        if let Some(mut topic_res) = self.resources_topic.clone() {
            let _ = topic_res
                .delete_subscription(subscription_name, &self.topic_name)
                .await;
        }
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

        let mut subscriptions_lock = self.subscriptions.lock().await;

        let subscription = if let std::collections::hash_map::Entry::Vacant(entry) =
            subscriptions_lock.entry(options.subscription_name.clone())
        {
            let mut new_subscription =
                Subscription::new(options.clone(), &self.topic_name, sub_metadata);

            // Handle additional logic for reliable storage
            if let DispatchStrategy::Reliable(reliable_dispatch) = &self.dispatch_strategy {
                reliable_dispatch
                    .add_subscription(&new_subscription.subscription_name)
                    .await?;

                let notifier = new_subscription
                    .create_new_dispatcher(options.clone(), &self.dispatch_strategy)
                    .await?;

                if let Some(notifier) = notifier {
                    self.notifiers.lock().await.push(notifier);
                }
            } else {
                let _ = new_subscription
                    .create_new_dispatcher(options.clone(), &self.dispatch_strategy)
                    .await?;
            }

            entry.insert(new_subscription)
        } else {
            subscriptions_lock
                .get_mut(&options.subscription_name)
                .unwrap()
        };

        if subscription.is_exclusive() && !subscription.get_consumers_info().is_empty() {
            warn!("Not allowed to add the Consumer: {}, the Exclusive subscription can't be shared with other consumers", options.consumer_name);
            return Err(anyhow!("Not allowed to add the Consumer: {}, the Exclusive subscription can't be shared with other consumers", options.consumer_name));
        }

        //Todo! Check the topic policies with max_consumers per topic

        let consumer_id = subscription.add_consumer(topic_name, options).await?;

        Ok(consumer_id)
    }

    // Unsubscribes the specified subscription from the topic
    // should be called if all consumers are disconnected
    pub(crate) async fn unsubscribe(&self, subscription_name: &str) {
        let _ = self.subscriptions.lock().await.remove(subscription_name);
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

        let consumers = subs.get_consumers_info();

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

        Ok(())
    }

    // Add a schema to the topic.
    pub(crate) fn add_schema(&mut self, schema: Schema) -> Result<()> {
        self.schema = Some(schema);
        Ok(())
    }

    // get topic's schema
    #[allow(dead_code)]
    pub(crate) fn get_schema(&self) -> Option<Schema> {
        self.schema.clone()
    }

    // Add a schema to the topic.
    #[allow(dead_code)]
    pub(crate) fn delete_schema(&self, _schema: Schema) -> Result<()> {
        todo!()
    }
}
