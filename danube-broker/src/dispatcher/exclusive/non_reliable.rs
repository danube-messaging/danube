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
use tokio::sync::mpsc;
use tracing::{trace, warn};

use crate::consumer::Consumer;

use super::super::commands::DispatcherCommand;
use super::super::exclusive::ExclusiveConsumerState;

/// Spawn the non-reliable exclusive dispatcher background task.
pub(super) fn start(mut control_rx: mpsc::Receiver<DispatcherCommand>) {
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
                        dispatch_to_active_consumer(state.active_consumer_mut(), msg).await;
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
                    trace!(consumer_id = %cons.consumer_id, "Message dispatched to active consumer");
                    Ok(())
                }
                Err(e) => {
                    warn!(consumer_id = %cons.consumer_id, error = %e, "Failed to dispatch to active consumer");
                    Err(e)
                }
            }
        }
    } else {
        Err(anyhow!("No active consumer available"))
    }
}
