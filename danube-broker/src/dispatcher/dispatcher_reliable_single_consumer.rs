use anyhow::{anyhow, Result};
use danube_reliable_dispatch::SubscriptionDispatch;
use tokio::sync::mpsc;
use tracing::{trace, warn};

use crate::{consumer::Consumer, dispatcher::DispatcherCommand, message::AckMessage};

/// Reliable dispatcher for single consumer, it sends ordered messages to a single consumer
#[derive(Debug)]
pub(crate) struct DispatcherReliableSingleConsumer {
    control_tx: mpsc::Sender<DispatcherCommand>,
}

impl DispatcherReliableSingleConsumer {
    pub(crate) fn new(mut subscription_dispatch: SubscriptionDispatch) -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);

        // Spawn dispatcher task
        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            let mut active_consumer: Option<Consumer> = None;

            loop {
                tokio::select! {
                    Some(command) = control_rx.recv() => {
                        match command {
                            DispatcherCommand::AddConsumer(consumer) => {
                                if let Err(e) = Self::handle_add_consumer(
                                    &mut consumers,
                                    &mut active_consumer,
                                    consumer,
                                ).await {
                                    warn!("Failed to add consumer: {}", e);
                                }
                            }
                            DispatcherCommand::RemoveConsumer(consumer_id) => {
                                Self::handle_remove_consumer(
                                    &mut consumers,
                                    &mut active_consumer,
                                    consumer_id,
                                ).await;
                            }
                            DispatcherCommand::DisconnectAllConsumers => {
                                Self::handle_disconnect_all(
                                    &mut consumers,
                                    &mut active_consumer,
                                ).await;
                            }
                            DispatcherCommand::DispatchMessage(_) => {
                                unreachable!("Reliable Dispatcher should not receive messages, just segments");
                            }
                            DispatcherCommand::MessageAcked(request_id, msg_id) => {
                                dbg!("received acked message");
                                if let Ok(Some(next_message)) = subscription_dispatch
                                    .handle_message_acked(request_id, msg_id)
                                    .await {
                                        if let Some(consumer) = Self::get_active_consumer(&mut active_consumer).await {
                                            if let Err(e) = consumer.send_message(next_message).await {
                                                warn!("Failed to dispatch message: {}", e);
                                            }
                                        }
                                    }
                            }
                        }
                    }
                    Some(message) = subscription_dispatch.next_message() => {
                        if let Some(consumer) = Self::get_active_consumer(&mut active_consumer).await {
                            if let Err(e) = consumer.send_message(message).await {
                                warn!("Failed to dispatch message: {}", e);
                            }
                        }
                    }
                }
            }
        });

        DispatcherReliableSingleConsumer { control_tx }
    }

    /// Acknowledge a message, which means that the message has been successfully processed by the consumer
    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::MessageAcked(
                ack_msg.request_id,
                ack_msg.msg_id,
            ))
            .await
            .map_err(|_| anyhow!("Failed to send message acked command"))
    }

    /// Add a consumer
    pub(crate) async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::AddConsumer(consumer))
            .await
            .map_err(|_| anyhow!("Failed to send add consumer command"))
    }

    /// Remove a consumer
    #[allow(dead_code)]
    pub(crate) async fn remove_consumer(&self, consumer_id: u64) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::RemoveConsumer(consumer_id))
            .await
            .map_err(|_| anyhow!("Failed to send remove consumer command"))
    }

    /// Disconnect all consumers
    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::DisconnectAllConsumers)
            .await
            .map_err(|_| anyhow!("Failed to send disconnect all consumers command"))
    }

    /// Handle adding a consumer
    async fn handle_add_consumer(
        consumers: &mut Vec<Consumer>,
        active_consumer: &mut Option<Consumer>,
        consumer: Consumer,
    ) -> Result<()> {
        if consumer.subscription_type == 1 {
            return Err(anyhow!(
                "Shared subscription should use a multi-consumer dispatcher"
            ));
        }

        if consumer.subscription_type == 0 && !consumers.is_empty() {
            warn!(
                "Exclusive subscription cannot be shared: consumer_id {}",
                consumer.consumer_id
            );
            return Err(anyhow!(
                "Exclusive subscription cannot be shared with other consumers"
            ));
        }

        consumers.push(consumer.clone());

        if active_consumer.is_none() {
            *active_consumer = Some(consumer.clone());
        }

        trace!(
            "Consumer {} added to single-consumer dispatcher",
            consumer.consumer_name
        );

        Ok(())
    }

    /// Handle removing a consumer
    async fn handle_remove_consumer(
        consumers: &mut Vec<Consumer>,
        active_consumer: &mut Option<Consumer>,
        consumer_id: u64,
    ) {
        consumers.retain(|c| c.consumer_id != consumer_id);

        if let Some(ref active) = active_consumer {
            if active.consumer_id == consumer_id {
                *active_consumer = None;
            }
        }

        trace!("Consumer {} removed from dispatcher", consumer_id);

        // Re-pick an active consumer if needed
        if active_consumer.is_none() && !consumers.is_empty() {
            for consumer in consumers {
                if consumer.get_status().await {
                    *active_consumer = Some(consumer.clone());
                    break;
                }
            }
        }
    }

    /// Handle disconnecting all consumers
    async fn handle_disconnect_all(
        consumers: &mut Vec<Consumer>,
        active_consumer: &mut Option<Consumer>,
    ) {
        consumers.clear();
        *active_consumer = None;
        trace!("All consumers disconnected from dispatcher");
    }

    /// Get the active consumer
    async fn get_active_consumer(active_consumer: &mut Option<Consumer>) -> Option<&mut Consumer> {
        match active_consumer {
            Some(consumer) if consumer.get_status().await => Some(consumer),
            _ => None,
        }
    }
}
