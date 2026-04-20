//! Non-reliable Key-Shared dispatcher
//!
//! # Overview
//!
//! Provides fast **key-based message distribution** to multiple consumers without acknowledgment
//! tracking. Used for non-durable Key-Shared subscriptions where message loss is acceptable
//! but per-key routing is still desired.
//!
//! # Dispatch Behavior
//!
//! - **Key-Based Routing**: Messages are routed to a consumer based on their routing key
//!   using consistent hashing (virtual-node ring)
//! - **No Acknowledgments**: Messages are sent immediately without waiting for confirmation
//! - **Best-Effort Delivery**: Attempts a non-blocking enqueue to the target consumer
//! - **Key Filtering**: Consumers with glob key filters only receive matching keys
//! - **Drop-on-Saturation**: If the target consumer's channel is full, the message is dropped
//!
//! # Differences from Reliable Key-Shared
//!
//! - No `InFlightWindow` — no per-key blocking or ordering enforcement
//! - No ack tracking or cursor persistence
//! - No retry or DLQ handling
//! - Inline dispatch (no background task)

use anyhow::Result;
use danube_core::message::StreamMessage;

use crate::consumer::ConsumerSendStatus;

use super::consumer_state::KeySharedConsumerState;

/// Direct non-reliable key-shared dispatch.
///
/// Routes a message to the consumer selected by consistent hashing on the routing key.
/// If the selected consumer is unhealthy or full, the message is dropped (best-effort).
pub(in crate::dispatcher) async fn dispatch(state: &mut KeySharedConsumerState, msg: StreamMessage) -> Result<()> {
    if state.is_empty() {
        return Err(anyhow::anyhow!("No consumers available"));
    }

    let routing_key = msg.effective_routing_key().to_string();

    let consumer_idx = match state.select_consumer(&routing_key) {
        Some(idx) => idx,
        None => {
            // No consumer accepts this key (all filters reject it) — drop silently
            return Ok(());
        }
    };

    let consumer = state.get_consumer_mut(consumer_idx);

    if !consumer.get_status().await {
        // Target consumer is unhealthy — drop the message (best-effort)
        return Ok(());
    }

    match consumer.try_send_message(msg) {
        ConsumerSendStatus::Sent => Ok(()),
        ConsumerSendStatus::Full => {
            // Consumer channel full — drop (best-effort, no blocking)
            Ok(())
        }
        ConsumerSendStatus::Closed => {
            consumer.set_status_inactive().await;
            Ok(())
        }
    }
}
