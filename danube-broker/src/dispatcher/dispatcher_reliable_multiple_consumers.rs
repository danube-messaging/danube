use anyhow::{anyhow, Result};
use danube_reliable_dispatch::SubscriptionDispatch;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tracing::{trace, warn};

use crate::{consumer::Consumer, dispatcher::DispatcherCommand, message::AckMessage};

/// Reliable dispatcher for multiple consumers, it sends ordered messages to multiple consumers
#[derive(Debug)]
pub(crate) struct DispatcherReliableMultipleConsumers {
    control_tx: mpsc::Sender<DispatcherCommand>,
}

impl DispatcherReliableMultipleConsumers {
    pub(crate) fn new(mut subscription_dispatch: SubscriptionDispatch) -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);

        // Spawn dispatcher task
        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            let index_consumer = AtomicUsize::new(0);

            loop {
                tokio::select! {
                    Some(command) = control_rx.recv() => {
                        match command {
                            DispatcherCommand::AddConsumer(consumer) => {
                                consumers.push(consumer);
                                trace!("Consumer added. Total consumers: {}", consumers.len());
                            }
                            DispatcherCommand::RemoveConsumer(consumer_id) => {
                                consumers.retain(|c| c.consumer_id != consumer_id);
                                trace!("Consumer removed. Total consumers: {}", consumers.len());
                            }
                            DispatcherCommand::DisconnectAllConsumers => {
                                consumers.clear();
                                trace!("All consumers disconnected.");
                            }
                            DispatcherCommand::DispatchMessage(_) => {
                                unreachable!("Reliable Dispatcher should not receive messages, just segments");
                            }
                            DispatcherCommand::MessageAcked(request_id, msg_id) => {
                                if let Ok(Some(next_message)) = subscription_dispatch
                                    .handle_message_acked(request_id, msg_id)
                                    .await {
                                        if let Some(active_idx) = Self::get_next_active_consumer(&consumers, &index_consumer).await {
                                            if let Err(e) = consumers[active_idx].send_message(next_message).await {
                                                warn!("Failed to dispatch message: {}", e);
                                            }
                                        }
                                    }
                            }
                        }
                    }
                    Some(message) = subscription_dispatch.next_message() => {
                        if let Some(active_idx) = Self::get_next_active_consumer(&consumers, &index_consumer).await {
                            if let Err(e) = consumers[active_idx].send_message(message).await {
                                warn!("Failed to dispatch message: {}", e);
                            }
                        }
                    }
                }
            }
        });

        DispatcherReliableMultipleConsumers { control_tx }
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

    /// Add a new consumer to the dispatcher
    pub(crate) async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::AddConsumer(consumer))
            .await
            .map_err(|_| anyhow!("Failed to send add consumer command"))
    }

    /// Remove a consumer by its ID
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

    async fn get_next_active_consumer(
        consumers: &[Consumer],
        index_consumer: &AtomicUsize,
    ) -> Option<usize> {
        if consumers.is_empty() {
            return None;
        }

        let num_consumers = consumers.len();
        let start_index = index_consumer.load(Ordering::SeqCst) % num_consumers;

        // Try each consumer starting from current index
        for i in 0..num_consumers {
            let check_index = (start_index + i) % num_consumers;
            if consumers[check_index].get_status().await {
                index_consumer.store(check_index + 1, Ordering::SeqCst);
                return Some(check_index);
            }
        }
        None
    }
}
