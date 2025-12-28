//! Exclusive dispatcher - supports Exclusive and Failover subscription types
//!
//! Routes messages to a single active consumer. If the active consumer fails,
//! failover to the next available consumer.

use anyhow::Result;
use danube_core::message::StreamMessage;
use std::sync::Arc;
use tokio::sync::Notify;

use crate::consumer::Consumer;
use crate::message::AckMessage;

use super::subscription_engine::SubscriptionEngine;

// Import the specific implementations
mod non_reliable;
mod reliable;

use non_reliable::NonReliableExclusiveDispatcher;
use reliable::ReliableExclusiveDispatcher;

/// Shared consumer state for exclusive dispatchers
/// Manages the list of consumers and tracks which one is active
pub(super) struct ExclusiveConsumerState {
    consumers: Vec<Consumer>,
    active_consumer: Option<Consumer>,
}

impl ExclusiveConsumerState {
    pub fn new() -> Self {
        Self {
            consumers: Vec::new(),
            active_consumer: None,
        }
    }

    pub fn add_consumer(&mut self, consumer: Consumer) {
        if self.consumers.is_empty() {
            self.active_consumer = Some(consumer.clone());
        }
        self.consumers.push(consumer);
    }

    pub fn remove_consumer(&mut self, consumer_id: u64) {
        self.consumers.retain(|c| c.consumer_id != consumer_id);
        if let Some(ref ac) = self.active_consumer {
            if ac.consumer_id == consumer_id {
                self.active_consumer = None;
            }
        }
    }

    pub fn disconnect_all(&mut self) {
        self.consumers.clear();
        self.active_consumer = None;
    }

    pub fn active_consumer_mut(&mut self) -> Option<&mut Consumer> {
        self.active_consumer.as_mut()
    }

    pub fn has_active_consumer(&self) -> bool {
        self.active_consumer.is_some()
    }
}

/// Main exclusive dispatcher enum
#[derive(Debug)]
pub(crate) enum ExclusiveDispatcher {
    NonReliable(NonReliableExclusiveDispatcher),
    Reliable(ReliableExclusiveDispatcher),
}

impl ExclusiveDispatcher {
    /// Create a non-reliable exclusive dispatcher (fast fan-out, no acks)
    pub(crate) fn new_non_reliable() -> Self {
        Self::NonReliable(NonReliableExclusiveDispatcher::new())
    }

    /// Create a reliable exclusive dispatcher (ack-gating with heartbeat)
    pub(crate) fn new_reliable(engine: SubscriptionEngine) -> Self {
        Self::Reliable(ReliableExclusiveDispatcher::new(engine))
    }

    /// Get notifier for reliable dispatcher (used by Topic to signal new messages)
    /// Only reliable dispatchers support notifications
    pub(crate) fn get_notifier(&self) -> Arc<Notify> {
        match self {
            Self::Reliable(d) => d.get_notifier(),
            Self::NonReliable(_) => {
                // This should never be called for non-reliable dispatchers
                // subscription.rs only calls this for reliable mode
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

    /// Dispatch a message to the active consumer (non-reliable only)
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
#[path = "exclusive_test.rs"]
mod tests;
