//! Shared dispatcher - supports Shared subscription type with round-robin
//!
//! Routes messages to multiple consumers using round-robin load balancing.

use anyhow::Result;
use danube_core::message::StreamMessage;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

use crate::consumer::Consumer;
use crate::message::AckMessage;

use super::subscription_engine::SubscriptionEngine;

// Import the specific implementations
mod non_reliable;
mod reliable;

use non_reliable::NonReliableSharedDispatcher;
use reliable::ReliableSharedDispatcher;

/// Shared consumer state with round-robin support
pub(super) struct SharedConsumerState {
    pub(super) consumers: Vec<Consumer>,
    pub(super) rr_index: Arc<AtomicUsize>,
}

impl SharedConsumerState {
    pub fn new(rr_index: Arc<AtomicUsize>) -> Self {
        Self {
            consumers: Vec::new(),
            rr_index,
        }
    }

    pub fn add_consumer(&mut self, consumer: Consumer) {
        self.consumers.push(consumer);
    }

    pub fn remove_consumer(&mut self, consumer_id: u64) {
        self.consumers.retain(|c| c.consumer_id != consumer_id);
    }

    pub fn disconnect_all(&mut self) {
        self.consumers.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }

    pub fn len(&self) -> usize {
        self.consumers.len()
    }

    /// Get the next consumer using round-robin
    pub fn next_consumer_mut(&mut self) -> Option<&mut Consumer> {
        if self.consumers.is_empty() {
            return None;
        }
        let idx = self.rr_index.fetch_add(1, Ordering::Relaxed) % self.consumers.len();
        self.consumers.get_mut(idx)
    }

    /// Get consumer by index (for explicit round-robin with attempts)
    pub fn get_consumer_mut(&mut self, idx: usize) -> Option<&mut Consumer> {
        self.consumers.get_mut(idx)
    }
}

/// Main shared dispatcher enum
#[derive(Debug, Clone)]
pub(crate) enum SharedDispatcher {
    NonReliable(NonReliableSharedDispatcher),
    Reliable(ReliableSharedDispatcher),
}

impl SharedDispatcher {
    /// Create a non-reliable shared dispatcher (fast round-robin, no acks)
    pub(crate) fn new_non_reliable() -> Self {
        Self::NonReliable(NonReliableSharedDispatcher::new())
    }

    /// Create a reliable shared dispatcher (ack-gating round-robin with heartbeat)
    pub(crate) fn new_reliable(engine: SubscriptionEngine) -> Self {
        Self::Reliable(ReliableSharedDispatcher::new(engine))
    }

    /// Get notifier for reliable dispatcher (used by Topic to signal new messages)
    pub(crate) fn get_notifier(&self) -> Arc<Notify> {
        match self {
            Self::Reliable(d) => d.get_notifier(),
            Self::NonReliable(_) => {
                panic!("get_notifier called on non-reliable dispatcher")
            }
        }
    }

    /// Block until the dispatcher is ready
    pub(crate) async fn ready(&self) {
        match self {
            Self::NonReliable(d) => d.ready().await,
            Self::Reliable(d) => d.ready().await,
        }
    }

    /// Dispatch a message to a consumer (round-robin)
    pub(crate) async fn dispatch_message(&self, msg: StreamMessage) -> Result<()> {
        match self {
            Self::NonReliable(d) => d.dispatch_message(msg).await,
            Self::Reliable(d) => d.dispatch_message(msg).await,
        }
    }

    /// Add a consumer to the dispatcher
    pub(crate) async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        match self {
            Self::NonReliable(d) => d.add_consumer(consumer).await,
            Self::Reliable(d) => d.add_consumer(consumer).await,
        }
    }

    /// Remove a consumer from the dispatcher
    pub(crate) async fn remove_consumer(&self, consumer_id: u64) -> Result<()> {
        match self {
            Self::NonReliable(d) => d.remove_consumer(consumer_id).await,
            Self::Reliable(d) => d.remove_consumer(consumer_id).await,
        }
    }

    /// Disconnect all consumers
    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        match self {
            Self::NonReliable(d) => d.disconnect_all_consumers().await,
            Self::Reliable(d) => d.disconnect_all_consumers().await,
        }
    }

    /// Acknowledge a message (reliable only)
    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        match self {
            Self::NonReliable(_) => Ok(()), // Non-reliable ignores acks
            Self::Reliable(d) => d.ack_message(ack_msg).await,
        }
    }

    /// Reset pending state (reliable only)
    pub(crate) async fn reset_pending(&self) -> Result<()> {
        match self {
            Self::NonReliable(_) => Ok(()), // Non-reliable has no pending state
            Self::Reliable(d) => d.reset_pending().await,
        }
    }

    /// Flush subscription progress (reliable only)
    pub async fn flush_progress_now(&self) -> Result<()> {
        match self {
            Self::NonReliable(_) => Ok(()), // Non-reliable: no-op
            Self::Reliable(d) => d.flush_progress_now().await,
        }
    }
}

#[cfg(test)]
#[path = "shared_test.rs"]
mod tests;
