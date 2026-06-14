//! Shared dispatch window for pipelined message delivery.
//!
//! `DispatchWindow` tracks multiple in-flight messages, handles out-of-order ACKs,
//! and advances a safe cursor for metadata persistence. Used by the Exclusive and
//! Shared reliable dispatchers to replace the single-slot `Option<PendingDelivery>`.
//!
//! # Safe Cursor Invariant
//!
//! The `safe_cursor` is the highest WAL offset where ALL offsets <= this value are
//! confirmed acked (or skipped). This ensures `SubscriptionEngine` never persists
//! a cursor that jumps past unacked messages — preventing message loss on restart.
//!
//! # Example
//!
//! ```text
//!   Dispatched: [1, 2, 3, 4, 5]
//!   Acked:      {1, 3, 5}
//!   safe_cursor: Some(1)   — only 1 is contiguously acked from start
//!
//!   Then ack(2):
//!   Acked:      {1, 2, 3, 5}
//!   safe_cursor: Some(3)   — 1,2,3 are contiguous
//! ```

use std::collections::{BTreeMap, BTreeSet};

use tokio::time::Instant;

use super::pending_delivery::PendingDelivery;

/// A pipelined in-flight message tracking window.
///
/// Tracks multiple dispatched messages, handles out-of-order ACKs,
/// and advances a safe cursor for metadata persistence.
pub(crate) struct DispatchWindow {
    /// Messages dispatched and awaiting ACK, keyed by WAL offset.
    in_flight: BTreeMap<u64, PendingDelivery>,

    /// Set of offsets that have been acked (for contiguous cursor tracking).
    acked_offsets: BTreeSet<u64>,

    /// Highest WAL offset where ALL offsets <= this value are confirmed acked.
    safe_cursor: Option<u64>,

    /// The first offset we've seen (for contiguous cursor initialization).
    first_offset: Option<u64>,

    /// Maximum number of in-flight messages (backpressure).
    max_unacked: usize,
}

impl DispatchWindow {
    /// Create a new dispatch window with the given capacity.
    ///
    /// `max_unacked` controls the maximum number of messages that can be in-flight
    /// simultaneously. Setting this to 1 reproduces the old single-slot behavior.
    pub(crate) fn new(max_unacked: usize) -> Self {
        Self {
            in_flight: BTreeMap::new(),
            acked_offsets: BTreeSet::new(),
            safe_cursor: None,
            first_offset: None,
            max_unacked,
        }
    }

    /// Can we poll and send more messages?
    pub(crate) fn has_capacity(&self) -> bool {
        self.in_flight.len() < self.max_unacked
    }

    /// Record a dispatched message.
    pub(super) fn mark_dispatched(&mut self, offset: u64, delivery: PendingDelivery) {
        self.in_flight.insert(offset, delivery);
        if self.first_offset.is_none() {
            self.first_offset = Some(offset);
        }
    }

    /// Process an ACK. Returns the new safe_cursor if it advanced.
    ///
    /// Removes the entry from in-flight, records the acked offset, and
    /// attempts to advance the contiguous safe cursor.
    pub(crate) fn on_ack(&mut self, offset: u64) -> Option<u64> {
        if self.in_flight.remove(&offset).is_some() {
            self.acked_offsets.insert(offset);
            self.advance_safe_cursor()
        } else {
            None
        }
    }

    /// Process a skipped message (poison-resolved or filtered).
    /// Treated like an immediate ack for cursor purposes.
    pub(crate) fn on_skipped(&mut self, offset: u64) -> Option<u64> {
        self.in_flight.remove(&offset);
        if self.first_offset.is_none() {
            self.first_offset = Some(offset);
        }
        self.acked_offsets.insert(offset);
        self.advance_safe_cursor()
    }

    /// Get a mutable reference to an in-flight entry by offset.
    /// Used for NACK handling and retry scheduling.
    pub(super) fn get_mut(&mut self, offset: u64) -> Option<&mut PendingDelivery> {
        self.in_flight.get_mut(&offset)
    }

    /// Get the safe cursor for metadata persistence.
    pub(crate) fn safe_cursor(&self) -> Option<u64> {
        self.safe_cursor
    }

    /// Number of in-flight messages.
    pub(crate) fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Get the highest offset dispatched into the window.
    /// Returns `None` if the window is empty (and has no acked offsets).
    pub(crate) fn highest_dispatched_offset(&self) -> Option<u64> {
        // The highest offset is either the last in-flight key or the safe_cursor
        // (acked offsets have been removed from in_flight).
        let last_in_flight = self.in_flight.keys().next_back().copied();
        match (last_in_flight, self.safe_cursor) {
            (Some(inf), Some(sc)) => Some(inf.max(sc)),
            (Some(inf), None) => Some(inf),
            (None, Some(sc)) => Some(sc),
            (None, None) => None,
        }
    }

    /// Check if the window is empty (no in-flight messages).
    pub(crate) fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }

    /// Find the first entry that has timed out on ACK.
    /// Returns the offset if found.
    pub(crate) fn find_ack_timed_out(&self, now: Instant) -> Option<u64> {
        self.in_flight
            .iter()
            .find(|(_, d)| d.ack_timed_out(now))
            .map(|(off, _)| *off)
    }

    /// Collect all offsets with expired ACK deadlines.
    pub(crate) fn collect_ack_timed_out(&self, now: Instant) -> Vec<u64> {
        self.in_flight
            .iter()
            .filter(|(_, d)| d.ack_timed_out(now))
            .map(|(off, _)| *off)
            .collect()
    }

    /// Collect all offsets ready for retry.
    pub(crate) fn collect_retry_ready(&self, now: Instant) -> Vec<u64> {
        self.in_flight
            .iter()
            .filter(|(_, d)| d.is_retry_ready(now))
            .map(|(off, _)| *off)
            .collect()
    }

    /// Find the first entry that is retry-exhausted.
    pub(crate) fn find_retry_exhausted(&self) -> Option<u64> {
        self.in_flight
            .iter()
            .find(|(_, d)| d.is_retry_exhausted())
            .map(|(off, _)| *off)
    }

    /// Collect all retry-exhausted offsets.
    pub(crate) fn collect_retry_exhausted(&self) -> Vec<u64> {
        self.in_flight
            .iter()
            .filter(|(_, d)| d.is_retry_exhausted())
            .map(|(off, _)| *off)
            .collect()
    }

    /// Count entries in the WaitingToRetry state (for metrics).
    pub(crate) fn count_waiting_to_retry(&self) -> usize {
        self.in_flight
            .values()
            .filter(|d| d.is_retry_ready(Instant::now()) || d.is_waiting_to_retry())
            .count()
    }

    /// Count entries in the RetryExhausted state (for metrics).
    pub(crate) fn count_retry_exhausted(&self) -> usize {
        self.in_flight
            .values()
            .filter(|d| d.is_retry_exhausted())
            .count()
    }

    /// Check if any entry is retry-ready.
    pub(crate) fn has_any_retry_ready(&self, now: Instant) -> bool {
        self.in_flight.values().any(|d| d.is_retry_ready(now))
    }

    /// Force ALL retryable in-flight entries to immediate retry.
    ///
    /// Unlike `collect_retry_ready`, this targets entries regardless of their
    /// current status (AwaitingAck, WaitingToRetry). Used on consumer reconnect
    /// to ensure unacked messages are redelivered to the new consumer without
    /// waiting for the ack timeout.
    ///
    /// Does NOT force RetryExhausted entries — those have been resolved by
    /// the poison policy.
    pub(super) fn force_retry_all(&mut self, reason: Option<String>) {
        for pending in self.in_flight.values_mut() {
            if !pending.is_retry_exhausted() {
                pending.schedule_retry_now(reason.clone());
            }
        }
    }

    /// Force retry of entries assigned to a specific consumer.
    ///
    /// Used by the shared dispatcher when a consumer disconnects — only that
    /// consumer's in-flight entries should be forcefully retried, while entries
    /// belonging to other healthy consumers remain `AwaitingAck`.
    pub(super) fn force_retry_for_consumer(&mut self, consumer_id: u64, reason: Option<String>) {
        for pending in self.in_flight.values_mut() {
            if pending.target_consumer_id == Some(consumer_id) && !pending.is_retry_exhausted() {
                pending.schedule_retry_now(reason.clone());
            }
        }
    }

    /// Drain all entries (for disconnect cleanup).
    /// Returns the list of all PendingDelivery entries.
    #[allow(dead_code)]
    pub(super) fn drain(&mut self) -> Vec<PendingDelivery> {
        let entries: Vec<PendingDelivery> = self
            .in_flight
            .values()
            .cloned()
            .collect();
        self.in_flight.clear();
        entries
    }

    /// Advance the safe cursor past contiguously-acked offsets.
    ///
    /// Example:
    ///   safe_cursor = Some(5)
    ///   acked_offsets = {6, 7, 9}  (8 is still in-flight)
    ///   → safe_cursor advances to 7, returns Some(7)
    ///   → acked_offsets = {9}
    fn advance_safe_cursor(&mut self) -> Option<u64> {
        let start = match self.safe_cursor {
            Some(c) => c + 1,
            None => match self.first_offset {
                Some(f) => f,
                None => return None,
            },
        };
        let mut cursor = start;
        let mut advanced = false;

        while self.acked_offsets.remove(&cursor) {
            self.safe_cursor = Some(cursor);
            cursor += 1;
            advanced = true;
        }

        if advanced {
            self.safe_cursor
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use danube_core::message::{MessageID, StreamMessage};
    use std::collections::HashMap;

    fn make_msg(offset: u64) -> StreamMessage {
        StreamMessage {
            request_id: 0,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test-topic".to_string(),
                broker_addr: "localhost:6650".to_string(),
                topic_offset: offset,
            },
            payload: Vec::new().into(),
            publish_time: 0,
            producer_name: "test-producer".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
            schema_id: None,
            schema_version: None,
            routing_key: None,
        }
    }

    #[test]
    fn capacity_limits() {
        let mut window = DispatchWindow::new(2);

        let d1 = PendingDelivery::new(make_msg(1));
        window.mark_dispatched(1, d1);
        assert!(window.has_capacity()); // 1 in-flight, limit 2

        let d2 = PendingDelivery::new(make_msg(2));
        window.mark_dispatched(2, d2);
        assert!(!window.has_capacity()); // 2 in-flight, limit 2

        // Ack one frees capacity
        window.on_ack(1);
        assert!(window.has_capacity());
    }

    #[test]
    fn safe_cursor_contiguous_in_order() {
        let mut window = DispatchWindow::new(100);

        for i in 1..=3 {
            window.mark_dispatched(i, PendingDelivery::new(make_msg(i)));
        }

        let cursor = window.on_ack(1);
        assert_eq!(cursor, Some(1));

        let cursor = window.on_ack(2);
        assert_eq!(cursor, Some(2));

        let cursor = window.on_ack(3);
        assert_eq!(cursor, Some(3));
    }

    #[test]
    fn safe_cursor_gap() {
        let mut window = DispatchWindow::new(100);

        for i in 1..=3 {
            window.mark_dispatched(i, PendingDelivery::new(make_msg(i)));
        }

        // Ack out of order: 3, then 1
        let cursor = window.on_ack(3);
        assert_eq!(cursor, None); // 1 and 2 still in-flight

        let cursor = window.on_ack(1);
        assert_eq!(cursor, Some(1)); // only 1 is contiguous

        let cursor = window.on_ack(2);
        assert_eq!(cursor, Some(3)); // now 1,2,3 all acked → jumps to 3
    }

    #[test]
    fn on_skipped_advances_cursor() {
        let mut window = DispatchWindow::new(100);

        let cursor = window.on_skipped(1);
        assert_eq!(cursor, Some(1));

        let cursor = window.on_skipped(2);
        assert_eq!(cursor, Some(2));

        // Skip 4 (gap at 3)
        let cursor = window.on_skipped(4);
        assert_eq!(cursor, None);

        // Skip 3 (fills gap)
        let cursor = window.on_skipped(3);
        assert_eq!(cursor, Some(4));
    }

    #[test]
    fn get_mut_returns_entry() {
        let mut window = DispatchWindow::new(100);
        window.mark_dispatched(5, PendingDelivery::new(make_msg(5)));

        assert!(window.get_mut(5).is_some());
        assert!(window.get_mut(999).is_none());
    }

    #[test]
    fn in_flight_count_and_empty() {
        let mut window = DispatchWindow::new(100);
        assert!(window.is_empty());
        assert_eq!(window.in_flight_count(), 0);

        window.mark_dispatched(1, PendingDelivery::new(make_msg(1)));
        assert!(!window.is_empty());
        assert_eq!(window.in_flight_count(), 1);
    }

    #[test]
    fn drain_returns_all_entries() {
        let mut window = DispatchWindow::new(100);
        for i in 1..=3 {
            window.mark_dispatched(i, PendingDelivery::new(make_msg(i)));
        }

        let drained = window.drain();
        assert_eq!(drained.len(), 3);
        assert!(window.is_empty());
    }

    #[test]
    fn safe_cursor_persists_across_mixed_operations() {
        let mut window = DispatchWindow::new(100);

        // Dispatch 1-5
        for i in 1..=5 {
            window.mark_dispatched(i, PendingDelivery::new(make_msg(i)));
        }

        // Ack 1,2 → cursor = 2
        window.on_ack(1);
        let cursor = window.on_ack(2);
        assert_eq!(cursor, Some(2));

        // Skip 3 (poison) → cursor = 3
        let cursor = window.on_skipped(3);
        assert_eq!(cursor, Some(3));

        // Ack 5 (gap at 4) → no advance
        let cursor = window.on_ack(5);
        assert_eq!(cursor, None);

        // Ack 4 → cursor = 5
        let cursor = window.on_ack(4);
        assert_eq!(cursor, Some(5));

        assert_eq!(window.safe_cursor(), Some(5));
    }

    #[test]
    fn single_slot_behavior_with_max_unacked_1() {
        let mut window = DispatchWindow::new(1);

        window.mark_dispatched(1, PendingDelivery::new(make_msg(1)));
        assert!(!window.has_capacity());

        let cursor = window.on_ack(1);
        assert_eq!(cursor, Some(1));
        assert!(window.has_capacity());

        window.mark_dispatched(2, PendingDelivery::new(make_msg(2)));
        assert!(!window.has_capacity());
    }
}
