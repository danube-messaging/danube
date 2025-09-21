use anyhow::{anyhow, Result};
use danube_reliable_dispatch::{ReliableDispatchError, SubscriptionDispatch};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};
use tokio::time::{interval, Duration};
use tracing::{trace, warn};

use crate::{consumer::Consumer, dispatcher::DispatcherCommand, message::AckMessage};

/// Reliable dispatcher for multiple consumers, it sends ordered messages to multiple consumers
#[derive(Debug)]
pub(crate) struct DispatcherReliableMultipleConsumers {
    control_tx: mpsc::Sender<DispatcherCommand>,
    notify_dispatch: Arc<Notify>,
}

impl DispatcherReliableMultipleConsumers {
    fn start_periodic_wakeup(notify: Arc<Notify>) {
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_millis(50));
            loop {
                tick.tick().await;
                notify.notify_one();
            }
        });
    }

    pub(crate) fn new(mut subscription_dispatch: SubscriptionDispatch) -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);
        let notify_dispatch = Arc::new(Notify::new());
        let notify_dispatch_clone = notify_dispatch.clone();

        // Start periodic wakeups to avoid stalling when no acks/control arrive
        Self::start_periodic_wakeup(notify_dispatch.clone());

        // Spawn dispatcher task
        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            let index_consumer = AtomicUsize::new(0);

            loop {
                // Wait for a notification or a control command
                notify_dispatch_clone.notified().await;

                // Process control commands first
                while let Ok(command) = control_rx.try_recv() {
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
                            unreachable!(
                                "Reliable Dispatcher should not receive messages, just segments"
                            );
                        }
                        DispatcherCommand::MessageAcked(request_id, msg_id) => {
                            // First check if we have an active consumer
                            if let Some(active_idx) =
                                Self::get_next_active_consumer(&consumers, &index_consumer).await
                            {
                                if let Ok(Some(next_message)) = subscription_dispatch
                                    .handle_message_acked(request_id, msg_id)
                                    .await
                                {
                                    if let Err(e) =
                                        consumers[active_idx].send_message(next_message).await
                                    {
                                        warn!("Failed to dispatch message: {}", e);
                                    }
                                }
                            }
                        }
                    }
                }

                // A notification has been received, so we can attempt to send the next message
                // Send ordered messages from the TopicStore to the consumers
                // Only process segments if we have an active consumer that's healthy
                if let Some(active_idx) =
                    Self::get_next_active_consumer(&consumers, &index_consumer).await
                {
                    match subscription_dispatch.poll_next().await {
                        Ok(msg) => {
                            if let Err(e) = consumers[active_idx].send_message(msg).await {
                                warn!("Failed to dispatch message: {}", e);
                            }
                        }
                        Err(e) => match e {
                            ReliableDispatchError::NoMessagesAvailable => continue,
                            err => warn!("Error polling next message from stream: {}", err),
                        },
                    };
                }
            }
        });

        DispatcherReliableMultipleConsumers {
            control_tx,
            notify_dispatch,
        }
    }

    /// Notify the dispatcher to process messages
    fn wake_dispatcher(&self) {
        self.notify_dispatch.notify_one();
    }

    pub(crate) fn get_notifier(&self) -> Arc<Notify> {
        self.notify_dispatch.clone()
    }

    /// Acknowledge a message, which means that the message has been successfully processed by the consumer
    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::MessageAcked(
                ack_msg.request_id,
                ack_msg.msg_id,
            ))
            .await
            .map_err(|_| anyhow!("Failed to send message acked command"))?;

        // Notify the dispatcher
        self.wake_dispatcher();
        Ok(())
    }

    /// Add a new consumer to the dispatcher
    pub(crate) async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::AddConsumer(consumer))
            .await
            .map_err(|_| anyhow!("Failed to send add consumer command"))?;

        // Notify the dispatcher
        self.wake_dispatcher();
        Ok(())
    }

    /// Remove a consumer by its ID
    pub(crate) async fn remove_consumer(&self, consumer_id: u64) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::RemoveConsumer(consumer_id))
            .await
            .map_err(|_| anyhow!("Failed to send remove consumer command"))?;

        // Notify the dispatcher
        self.wake_dispatcher();
        Ok(())
    }

    /// Disconnect all consumers
    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::DisconnectAllConsumers)
            .await
            .map_err(|_| anyhow!("Failed to send disconnect all consumers command"))?;

        // Notify the dispatcher
        self.wake_dispatcher();
        Ok(())
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
