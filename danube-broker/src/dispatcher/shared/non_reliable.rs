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
//! - **Best-Effort Delivery**: Attempts a non-blocking enqueue to one consumer at a time
//! - **Slow-Consumer Isolation**: Full consumer channels are skipped so one slow consumer does not stall fan-out
//! - **Drop-on-Saturation**: If all healthy consumers are currently full, the message is dropped without surfacing an error
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
//! 2. Try a non-blocking send to the consumer at that index
//! 3. If unhealthy, closed, or full, try the next consumer
//! 4. Continue until successful or all consumers have been checked
//! 5. Return error only when no consumer is available/healthy; saturation alone is treated as best-effort drop
//!
//! # Message Flow
//!
//! 1. `dispatch_message(msg)` is called (typically from Topic)
//! 2. Calculate next consumer index via round-robin
//! 3. Check consumer health with `get_status()`
//! 4. Attempt non-blocking enqueue via `try_send_message()`
//! 5. If the channel is full, try the next consumer in rotation
//! 6. If all healthy consumers are full, drop the message and return success
//! 7. If all consumers are unavailable, return failure
//!
//! # Use Cases
//!
//! - High-throughput message queues where occasional loss is acceptable
//! - Load distribution across shared consumers
//! - Shared subscriptions with low-latency requirements
//! - Real-time analytics where sampling is sufficient
//! - Non-durable shared subscriptions

use anyhow::Result;
use danube_core::message::StreamMessage;
use crate::consumer::ConsumerSendStatus;
use super::super::shared::SharedConsumerState;

/// Direct non-reliable shared dispatch.
///
/// This path does not spawn a background task and does not use dispatcher commands.
/// It performs best-effort delivery inline on the caller's async path.
pub(super) async fn dispatch(state: &mut SharedConsumerState, msg: StreamMessage) -> Result<()> {
    dispatch_round_robin(state, msg).await
}

async fn dispatch_round_robin(state: &mut SharedConsumerState, msg: StreamMessage) -> Result<()> {
    let num_consumers = state.len();
    if num_consumers == 0 {
        return Err(anyhow::anyhow!("No consumers available"));
    }

    let mut saw_healthy = false;
    let mut saw_full = false;

    for _ in 0..num_consumers {
        if let Some(target) = state.next_consumer_mut() {
            if !target.get_status().await {
                continue;
            }

            saw_healthy = true;

            match target.try_send_message(msg.clone()) {
                ConsumerSendStatus::Sent => return Ok(()),
                ConsumerSendStatus::Full => {
                    saw_full = true;
                }
                ConsumerSendStatus::Closed => {
                    target.set_status_inactive().await;
                }
            }
        }
    }

    if saw_full {
        Ok(())
    } else if saw_healthy {
        Err(anyhow::anyhow!("Failed to dispatch to any healthy consumer"))
    } else {
        Err(anyhow::anyhow!("No consumers available"))
    }
}
