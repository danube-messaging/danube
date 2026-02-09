//! Non-reliable shared dispatcher
//!
//! # Overview
//!
//! Provides fast **round-robin message distribution** to multiple consumers without acknowledgment tracking.
//! Used for non-durable shared subscriptions where message loss is acceptable and load balancing is needed.
//!
//! # Dispatch Behavior
//!
//! - **Multiple Consumers**: All connected consumers can receive messages
//! - **Round-Robin**: Messages are distributed evenly across consumers using atomic counter
//! - **No Acknowledgments**: Messages are sent immediately without waiting for confirmation
//! - **Fire-and-Forget**: Once sent, the message is considered delivered (no retries)
//! - **Best-Effort Delivery**: If a consumer is unhealthy, tries next consumer in rotation
//!
//! # State Management
//!
//! - `consumers`: List of all connected consumers
//! - `rr_index`: Atomic counter for round-robin position (shared, incremented on each send)
//! - Health status tracked per consumer via `Consumer::get_status()`
//!
//! # Round-Robin Algorithm
//!
//! The dispatcher uses an atomic counter to ensure fair distribution:
//!
//! 1. Calculate target index: `rr_index.fetch_add(1) % num_consumers`
//! 2. Try sending to consumer at that index
//! 3. If unhealthy or send fails, try next consumer
//! 4. Continue until successful or all consumers tried
//! 5. Return error if no healthy consumer found
//!
//! # Message Flow
//!
//! 1. `dispatch_message(msg)` is called (typically from Topic)
//! 2. Calculate next consumer index via round-robin
//! 3. Check consumer health with `get_status()`
//! 4. Send message immediately via `send_message()`
//! 5. Return success/failure (no waiting for ack)
//! 6. If send fails, try next consumer in rotation
//!
//! # Use Cases
//!
//! - High-throughput message queues where occasional loss is acceptable
//! - Load distribution across worker pools
//! - Shared subscriptions with low-latency requirements
//! - Real-time analytics where sampling is sufficient
//! - Non-durable shared subscriptions

use anyhow::Result;
use danube_core::message::StreamMessage;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::trace;

use super::super::commands::DispatcherCommand;
use super::super::shared::SharedConsumerState;

/// Spawn the non-reliable shared dispatcher background task.
pub(super) fn start(mut control_rx: mpsc::Receiver<DispatcherCommand>) {
    let rr_index = Arc::new(AtomicUsize::new(0));

    tokio::spawn(async move {
        let mut state = SharedConsumerState::new(rr_index);

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
                    let result = dispatch_round_robin(&mut state, msg).await;
                    let _ = response_tx.send(result);
                }
                _ => {}
            }
        }
        trace!("Non-reliable shared dispatcher task exiting gracefully");
    });
}

async fn dispatch_round_robin(state: &mut SharedConsumerState, msg: StreamMessage) -> Result<()> {
    let num_consumers = state.len();
    if num_consumers == 0 {
        return Err(anyhow::anyhow!("No consumers available"));
    }

    for _ in 0..num_consumers {
        if let Some(target) = state.next_consumer_mut() {
            if !target.get_status().await {
                continue;
            }
            if target.send_message(msg.clone()).await.is_ok() {
                return Ok(());
            }
        }
    }

    Err(anyhow::anyhow!("Failed to dispatch to any consumer"))
}
