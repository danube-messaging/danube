//! Exclusive dispatcher - supports Exclusive and Failover subscription types
//!
//! Routes messages to a single active consumer. If the active consumer fails,
//! failover to the next available consumer.

use tokio::sync::{mpsc, watch};

use crate::consumer::Consumer;

use super::commands::DispatcherCommand;
use super::subscription_engine::SubscriptionEngine;

// Import the specific implementations
pub(super) mod non_reliable;
pub(super) mod reliable;

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

/// Thin namespace for spawning exclusive dispatcher background tasks.
/// The actual Dispatcher struct lives in the parent module.
pub(super) struct ExclusiveDispatcher;

impl ExclusiveDispatcher {
    /// Spawn the non-reliable exclusive background task.
    pub(super) fn start_non_reliable(control_rx: mpsc::Receiver<DispatcherCommand>) {
        non_reliable::start(control_rx);
    }

    /// Spawn the reliable exclusive background task.
    pub(super) fn start_reliable(
        engine: SubscriptionEngine,
        control_rx: mpsc::Receiver<DispatcherCommand>,
        control_tx: mpsc::Sender<DispatcherCommand>,
        ready_tx: watch::Sender<bool>,
    ) {
        reliable::start(engine, control_rx, control_tx, ready_tx);
    }
}

#[cfg(test)]
#[path = "exclusive_test.rs"]
mod tests;
