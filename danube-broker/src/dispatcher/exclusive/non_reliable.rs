//! Non-reliable exclusive dispatcher
//!
//! # Overview
//!
//! Provides fast message fan-out to a **single active consumer** without acknowledgment tracking.
//! Used for non-durable subscriptions where message loss is acceptable and low latency is prioritized.
//!
//! # Dispatch Behavior
//!
//! - **Single Active Consumer**: Only one consumer is active at a time (the first consumer added)
//! - **No Acknowledgments**: Messages are sent immediately without waiting for consumer confirmation
//! - **Fire-and-Forget**: Once sent, the message is considered delivered (no retries)
//! - **Failover**: If the active consumer disconnects, the next available consumer becomes active
//!
//! # State Management
//!
//! - Maintains a list of consumers with one designated as "active"
//! - The active consumer is selected from the consumer list (first available)
//! - Health checks are performed via `Consumer::get_status()` before sending
//!
//! # Message Flow
//!
//! 1. `dispatch_message(msg)` is called (typically from Topic)
//! 2. Check if active consumer exists and is healthy
//! 3. Send message immediately to active consumer
//! 4. Return success/failure (no waiting for ack)
//!
//! # Use Cases
//!
//! - Real-time data streams where occasional loss is acceptable
//! - Monitoring/telemetry feeds
//! - Exclusive subscriptions with low-latency requirements
//! - Non-durable subscriptions (data not persisted)

use anyhow::{anyhow, Result};
use danube_core::message::StreamMessage;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{trace, warn};

use crate::consumer::Consumer;

use super::super::commands::DispatcherCommand;
use super::super::exclusive::ExclusiveConsumerState;

#[derive(Debug)]
pub(crate) struct NonReliableExclusiveDispatcher {
    control_tx: mpsc::Sender<DispatcherCommand>,
    ready_rx: watch::Receiver<bool>,
}

impl NonReliableExclusiveDispatcher {
    pub fn new() -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);
        let (_ready_tx, ready_rx) = watch::channel(true); // Ready immediately

        tokio::spawn(async move {
            let mut state = ExclusiveConsumerState::new();

            while let Some(cmd) = control_rx.recv().await {
                match cmd {
                    DispatcherCommand::AddConsumer(c) => {
                        state.add_consumer(c);
                    }
                    DispatcherCommand::RemoveConsumer(consumer_id) => {
                        state.remove_consumer(consumer_id);
                    }
                    DispatcherCommand::DisconnectAllConsumers => {
                        state.disconnect_all();
                    }
                    DispatcherCommand::DispatchMessage(msg, response_tx) => {
                        let result =
                            Self::dispatch_to_active_consumer(state.active_consumer_mut(), msg)
                                .await;
                        let _ = response_tx.send(result);
                    }
                    // Ignore reliable-only commands
                    DispatcherCommand::MessageAcked(_) => {}
                    DispatcherCommand::PollAndDispatch => {}
                    DispatcherCommand::ResetPending => {}
                    DispatcherCommand::FlushProgressNow => {}
                }
            }
            trace!("Non-reliable exclusive dispatcher task exiting gracefully");
        });

        Self {
            control_tx,
            ready_rx,
        }
    }

    async fn dispatch_to_active_consumer(
        consumer: Option<&mut Consumer>,
        msg: StreamMessage,
    ) -> Result<()> {
        if let Some(cons) = consumer {
            if !cons.get_status().await {
                Err(anyhow!("Active consumer not healthy"))
            } else {
                match cons.send_message(msg).await {
                    Ok(()) => {
                        trace!("Message dispatched to active consumer {}", cons.consumer_id);
                        Ok(())
                    }
                    Err(e) => {
                        warn!("Failed to dispatch to active consumer: {}", e);
                        Err(e)
                    }
                }
            }
        } else {
            Err(anyhow!("No active consumer available"))
        }
    }

    // Public API
    pub async fn ready(&self) {
        if *self.ready_rx.borrow() {
            return;
        }
        let mut rx = self.ready_rx.clone();
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                break;
            }
        }
    }

    pub async fn dispatch_message(&self, msg: StreamMessage) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(DispatcherCommand::DispatchMessage(msg, tx))
            .await
            .map_err(|e| anyhow!("Failed to send dispatch command: {}", e))?;
        rx.await
            .map_err(|e| anyhow!("Failed to receive dispatch response: {}", e))?
    }

    pub async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::AddConsumer(consumer))
            .await
            .map_err(|_| anyhow!("Failed to send add consumer command"))
    }

    pub async fn remove_consumer(&self, consumer_id: u64) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::RemoveConsumer(consumer_id))
            .await
            .map_err(|_| anyhow!("Failed to send remove consumer command"))
    }

    pub async fn disconnect_all_consumers(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::DisconnectAllConsumers)
            .await
            .map_err(|_| anyhow!("Failed to send disconnect all command"))
    }
}
