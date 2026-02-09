//! Shared dispatcher - supports Shared subscription type with round-robin
//!
//! Routes messages to multiple consumers using round-robin load balancing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use crate::consumer::Consumer;

use super::commands::DispatcherCommand;
use super::subscription_engine::SubscriptionEngine;

// Import the specific implementations
pub(super) mod non_reliable;
pub(super) mod reliable;

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

/// Thin namespace for spawning shared dispatcher background tasks.
/// The actual Dispatcher struct lives in the parent module.
pub(super) struct SharedDispatcher;

impl SharedDispatcher {
    /// Spawn the non-reliable shared background task.
    pub(super) fn start_non_reliable(control_rx: mpsc::Receiver<DispatcherCommand>) {
        non_reliable::start(control_rx);
    }

    /// Spawn the reliable shared background task.
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
#[path = "shared_test.rs"]
mod tests;
