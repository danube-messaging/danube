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
//! - **Best-Effort Delivery**: Uses a non-blocking enqueue to the active consumer
//! - **Slow-Consumer Isolation**: A full consumer channel does not block the caller
//! - **Drop-on-Full**: If the active consumer channel is full, the message is dropped without surfacing an error
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
//! 3. Attempt non-blocking enqueue to the active consumer via `try_send_message()`
//! 4. If the channel is full, drop the message and return success
//! 5. If the channel is closed or no healthy consumer exists, return failure
//!
//! # Use Cases
//!
//! - Real-time data streams where occasional loss is acceptable
//! - Monitoring/telemetry feeds
//! - Exclusive subscriptions with low-latency requirements
//! - Non-durable subscriptions (data not persisted)

use anyhow::{anyhow, Result};
use danube_core::message::StreamMessage;
use tracing::{trace, warn};

use crate::consumer::{Consumer, ConsumerSendStatus};

use super::super::exclusive::ExclusiveConsumerState;

/// Direct non-reliable exclusive dispatch.
///
/// This path does not spawn a background task and does not use dispatcher commands.
/// It performs best-effort delivery inline on the caller's async path.
pub(super) async fn dispatch(state: &mut ExclusiveConsumerState, msg: StreamMessage) -> Result<()> {
    dispatch_to_active_consumer(state.active_consumer_mut(), msg).await
}

async fn dispatch_to_active_consumer(
    consumer: Option<&mut Consumer>,
    msg: StreamMessage,
) -> Result<()> {
    if let Some(cons) = consumer {
        if !cons.get_status().await {
            Err(anyhow!("Active consumer not healthy"))
        } else {
            match cons.try_send_message(msg) {
                ConsumerSendStatus::Sent => {
                    trace!(consumer_id = %cons.consumer_id, "Message dispatched to active consumer");
                    Ok(())
                }
                ConsumerSendStatus::Full => Ok(()),
                ConsumerSendStatus::Closed => {
                    cons.set_status_inactive().await;
                    let error = anyhow!("Active consumer channel closed");
                    warn!(consumer_id = %cons.consumer_id, error = %error, "Failed to dispatch to active consumer");
                    Err(error)
                }
            }
        }
    } else {
        Err(anyhow!("No active consumer available"))
    }
}
