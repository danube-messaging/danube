use anyhow::{anyhow, Ok, Result};
use danube_core::message::StreamMessage;
use metrics::gauge;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::trace;

use crate::{
    broker_metrics::TOPIC_CONSUMERS,
    consumer::Consumer,
    dispatch_strategy::DispatchStrategy,
    dispatcher::{
        dispatcher_multiple_consumers::DispatcherMultipleConsumers,
        dispatcher_reliable_multiple_consumers::DispatcherReliableMultipleConsumers,
        dispatcher_reliable_single_consumer::DispatcherReliableSingleConsumer,
        dispatcher_single_consumer::DispatcherSingleConsumer, Dispatcher,
    },
    message::AckMessage,
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
    pub(crate) consumers: HashMap<u64, ConsumerInfo>,
}

#[derive(Debug, Clone)]
pub(crate) struct ConsumerInfo {
    pub(crate) consumer_id: u64,
    pub(crate) sub_options: SubscriptionOptions,
    pub(crate) status: Arc<Mutex<bool>>,
    pub(crate) rx_cons: Arc<Mutex<mpsc::Receiver<StreamMessage>>>,
}

impl ConsumerInfo {
    pub(crate) async fn get_status(&self) -> bool {
        *self.status.lock().await
    }
    #[allow(dead_code)]
    pub(crate) async fn set_status_false(&self) -> () {
        *self.status.lock().await = false
    }
    pub(crate) async fn set_status_true(&self) -> () {
        *self.status.lock().await = true
    }
}

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
        }
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
        let consumer_status = Arc::new(Mutex::new(true));
        let consumer = Consumer::new(
            consumer_id,
            &options.consumer_name,
            options.subscription_type,
            topic_name,
            tx_cons,
            consumer_status.clone(),
        );

        let dispatcher = self.dispatcher.as_mut().unwrap();
        // Add the consumer to the dispatcher
        dispatcher.add_consumer(consumer).await?;

        let consumer_info = ConsumerInfo {
            consumer_id,
            sub_options: options,
            status: consumer_status,
            rx_cons: Arc::new(Mutex::new(rx_cons)),
        };

        // Insert the consumer into the subscription's consumer list
        self.consumers.insert(consumer_id, consumer_info.clone());

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
    ) -> Result<Option<Arc<Notify>>> {
        let (new_dispatcher, notifier) = match dispatch_strategy {
            DispatchStrategy::NonReliable => match options.subscription_type {
                // Exclusive
                0 => (
                    Dispatcher::OneConsumer(DispatcherSingleConsumer::new()),
                    None,
                ),

                // Shared
                1 => (
                    Dispatcher::MultipleConsumers(DispatcherMultipleConsumers::new()),
                    None,
                ),

                // Failover
                2 => (
                    Dispatcher::OneConsumer(DispatcherSingleConsumer::new()),
                    None,
                ),

                _ => {
                    return Err(anyhow!("Should not get here"));
                }
            },
            DispatchStrategy::Reliable(reliable_dispatcher) => {
                let subscription_dispatch = reliable_dispatcher
                    .new_subscription_dispatch(&options.subscription_name)
                    .await?;

                match options.subscription_type {
                    // Exclusive
                    0 => {
                        let new_dispatcher =
                            DispatcherReliableSingleConsumer::new(subscription_dispatch);
                        let notifier = new_dispatcher.get_notifier();
                        (
                            Dispatcher::ReliableOneConsumer(new_dispatcher),
                            Some(notifier),
                        )
                    }

                    // Shared
                    1 => {
                        let new_dispatcher =
                            DispatcherReliableMultipleConsumers::new(subscription_dispatch);
                        let notifier = new_dispatcher.get_notifier();
                        (
                            Dispatcher::ReliableMultipleConsumers(new_dispatcher),
                            Some(notifier),
                        )
                    }

                    // Failover
                    2 => {
                        let new_dispatcher =
                            DispatcherReliableSingleConsumer::new(subscription_dispatch);
                        let notifier = new_dispatcher.get_notifier();
                        (
                            Dispatcher::ReliableOneConsumer(new_dispatcher),
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

    pub(crate) fn get_consumer_rx(
        &self,
        consumer_id: u64,
    ) -> Option<Arc<Mutex<mpsc::Receiver<StreamMessage>>>> {
        if let Some(consumer_info) = self.consumers.get(&consumer_id) {
            return Some(consumer_info.rx_cons.clone());
        }
        None
    }

    pub(crate) fn get_consumer_info(&self, consumer_id: u64) -> Option<ConsumerInfo> {
        if let Some(consumer) = self.consumers.get(&consumer_id) {
            return Some(consumer.clone());
        }
        None
    }

    // Get Consumers
    pub(crate) fn get_consumers_info(&self) -> Vec<ConsumerInfo> {
        let consumers = self.consumers.values().cloned().collect::<Vec<_>>();
        consumers
    }

    // handles the disconnection of consumers associated with the subscription.
    #[allow(dead_code)]
    pub(crate) async fn disconnect(&mut self) -> Result<Vec<u64>> {
        let mut consumers_id = Vec::new();

        for (consumer_id, consumer_info) in self.consumers.iter_mut() {
            if consumer_info.get_status().await {
                // if consumer exist and its status is true, then set the status to false
                consumer_info.set_status_false().await;
                consumers_id.push(*consumer_id);
            }
            gauge!(TOPIC_CONSUMERS.name, "topic" => self.topic_name.to_string()).decrement(1);
        }

        // Disconnect all consumers
        if let Some(dispatcher) = self.dispatcher.as_mut() {
            dispatcher.disconnect_all_consumers().await?;
        }

        Ok(consumers_id)
    }

    // Validate Consumer - returns consumer ID
    pub(crate) async fn validate_consumer(&self, consumer_name: &str) -> Option<u64> {
        for consumer_info in self.consumers.values() {
            if consumer_info.sub_options.consumer_name == consumer_name {
                // if consumer exist and its status is false, then the consumer has disconnected
                // the consumer client may try to reconnect
                // then set the status to true and use the consumer
                if !consumer_info.get_status().await {
                    consumer_info.set_status_true().await;
                }
                return Some(consumer_info.consumer_id);
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
