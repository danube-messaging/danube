//! # Dispatcher Commands
//!
//! Internal command protocol for async communication between dispatcher public API and background event loop.
//!
//! Commands are sent via `mpsc::channel` from facade methods to the spawned task that manages
//! dispatcher state (consumers, pending messages). This enables single-threaded event loop
//! without complex synchronization.

use anyhow::Result;
use danube_core::message::StreamMessage;
use tokio::sync::oneshot;

use crate::consumer::Consumer;
use crate::message::AckMessage;

/// Commands for internal dispatcher communication.
///
/// Sent from dispatcher public API to background event loop task via `mpsc::channel`.
///
/// ## Command Categories
///
/// **Consumer Management** (all dispatchers):
/// - `AddConsumer`, `RemoveConsumer`, `DisconnectAllConsumers`
///
/// **Non-Reliable Only**:
/// - `DispatchMessage` - Direct dispatch with oneshot response
///
/// **Reliable Only**:
/// - `MessageAcked` - Consumer acknowledgment received
/// - `PollAndDispatch` - Trigger polling from SubscriptionEngine
/// - `ResetPending` - Clear pending state for failover
/// - `FlushProgressNow` - Force immediate cursor persistence
#[derive(Debug)]
pub(super) enum DispatcherCommand {
    /// Register new consumer. Becomes active (exclusive) or joins round-robin (shared).
    AddConsumer(Consumer),

    /// Unregister consumer by ID. Triggers failover if active consumer removed (exclusive).
    /// Pending messages are not lost - will be resent to another consumer.
    RemoveConsumer(u64),

    /// Gracefully disconnect all consumers. Used during subscription shutdown.
    DisconnectAllConsumers,

    /// Direct message dispatch (non-reliable only).
    /// Topic sends message with oneshot channel for response.
    /// Reliable dispatchers return error - they use `PollAndDispatch` instead.
    DispatchMessage(StreamMessage, oneshot::Sender<Result<()>>),

    /// Consumer acknowledgment received (reliable only).
    /// Updates cursor, clears pending state, triggers next poll.
    MessageAcked(AckMessage),

    /// Trigger polling and dispatch (reliable only).
    /// Sent by Topic notifier, heartbeat (on lag), or post-ack.
    /// Polls message from SubscriptionEngine, sends to consumer, waits for ack.
    PollAndDispatch,

    /// Clear pending flag to unblock dispatch (reliable only).
    /// Used during consumer failover - keeps buffered message for retry.
    ResetPending,

    /// Force immediate cursor persistence (reliable only).
    /// Bypasses debounce interval. Used on disconnect/shutdown.
    FlushProgressNow,
}
