//! # Dispatcher Commands
//!
//! Internal command protocol for async communication between dispatcher public API and background event loop.
//!
//! Commands are sent via `mpsc::channel` from facade methods to the spawned task that manages
//! dispatcher state (consumers, pending messages). This enables single-threaded event loop
//! without complex synchronization.

use crate::consumer::Consumer;
use crate::message::{AckMessage, NackMessage};

/// Commands for internal dispatcher communication.
///
/// Sent from dispatcher public API to background event loop task via `mpsc::channel`.
///
/// ## Command Categories
///
/// **Consumer Management** (all dispatchers):
/// - `AddConsumer`, `RemoveConsumer`, `DisconnectAllConsumers`
///
#[derive(Debug)]
pub(super) enum DispatcherCommand {
    /// Register new consumer. Becomes active (exclusive) or joins round-robin (shared).
    AddConsumer(Consumer),

    /// Unregister consumer by ID. Triggers failover if active consumer removed (exclusive).
    /// Pending messages are not lost - will be resent to another consumer.
    RemoveConsumer(u64),

    /// Gracefully disconnect all consumers. Used during subscription shutdown.
    DisconnectAllConsumers,

    MessageAcked(AckMessage),

    MessageNacked(NackMessage),

    PollAndDispatch,

    ResetPending,

    FlushProgressNow,
}
