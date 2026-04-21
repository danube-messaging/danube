//! In-flight window for Key-Shared dispatch.
//!
//! Tracks multiple in-flight messages with per-key blocking and contiguous cursor advancement.
//!
//! Core invariant: at most ONE message per routing key is in-flight at any time.
//! Additional messages for the same key are queued in `blocked_queue`.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use danube_core::message::StreamMessage;

use super::super::pending_delivery::PendingDelivery;

/// Tracks multiple in-flight messages with per-key blocking.
///
/// The `safe_cursor` only advances past contiguously-acked offsets,
/// ensuring the SubscriptionEngine never persists a cursor that skips unacked messages.
#[derive(Debug)]
pub(crate) struct InFlightWindow {
    /// Messages dispatched and awaiting ACK, keyed by WAL offset.
    pub(crate) in_flight: BTreeMap<u64, InFlightEntry>,

    /// Which routing keys currently have an in-flight message.
    /// Value: the WAL offset holding the lock.
    active_keys: HashMap<String, u64>,

    /// Messages polled from WAL but blocked because their key is active.
    pub(crate) blocked_queue: VecDeque<StreamMessage>,

    /// Set of offsets that have been acked (used for contiguous cursor tracking).
    acked_offsets: BTreeSet<u64>,

    /// The highest WAL offset where ALL offsets <= this value are confirmed acked.
    /// This is what gets persisted to SubscriptionEngine.
    safe_cursor: Option<u64>,

    /// The first offset we've seen (for contiguous cursor initialization).
    first_offset: Option<u64>,

    /// Maximum number of in-flight + blocked messages.
    max_window_size: usize,

    /// Per-consumer in-flight message count for backpressure.
    per_consumer_in_flight: HashMap<u64, usize>,

    /// Maximum in-flight messages per consumer before backpressure kicks in.
    max_per_consumer: usize,
}

/// An in-flight message entry.
#[derive(Debug)]
pub(crate) struct InFlightEntry {
    pub(crate) delivery: PendingDelivery,
    pub(crate) routing_key: String,
    pub(crate) consumer_id: u64,
}

impl InFlightWindow {
    pub(crate) fn new(max_window_size: usize) -> Self {
        Self {
            in_flight: BTreeMap::new(),
            active_keys: HashMap::new(),
            blocked_queue: VecDeque::new(),
            acked_offsets: BTreeSet::new(),
            safe_cursor: None,
            first_offset: None,
            max_window_size,
            per_consumer_in_flight: HashMap::new(),
            max_per_consumer: 1_000,
        }
    }

    /// Check if we can accept more messages from the WAL.
    pub(crate) fn has_capacity(&self) -> bool {
        self.in_flight.len() + self.blocked_queue.len() < self.max_window_size
    }

    /// Get the current safe cursor value.
    pub(crate) fn get_safe_cursor(&self) -> Option<u64> {
        self.safe_cursor
    }

    /// Check if a routing key is currently in-flight (would need to be blocked).
    pub(crate) fn is_key_active(&self, key: &str) -> bool {
        self.active_keys.contains_key(key)
    }

    /// Record a message as dispatched to a consumer.
    pub(crate) fn mark_dispatched(
        &mut self,
        offset: u64,
        routing_key: String,
        consumer_id: u64,
        delivery: PendingDelivery,
    ) {
        self.active_keys.insert(routing_key.clone(), offset);
        self.in_flight.insert(
            offset,
            InFlightEntry {
                delivery,
                routing_key,
                consumer_id,
            },
        );
        *self.per_consumer_in_flight.entry(consumer_id).or_insert(0) += 1;
        if self.first_offset.is_none() {
            self.first_offset = Some(offset);
        }
    }

    /// Push a blocked message (key is currently in-flight).
    pub(crate) fn push_blocked(&mut self, msg: StreamMessage) {
        self.blocked_queue.push_back(msg);
    }

    /// Record WAL offset just polled (for cursor initialization).
    pub(crate) fn record_polled(&mut self, offset: u64) {
        if self.first_offset.is_none() {
            self.first_offset = Some(offset);
        }
    }

    /// Process an ACK. Returns:
    /// - The unblocked routing key (so caller can check blocked_queue)
    /// - The new safe cursor value (if it advanced), to persist to engine
    pub(crate) fn on_ack(&mut self, offset: u64) -> (Option<String>, Option<u64>) {
        let unblocked_key = if let Some(entry) = self.in_flight.remove(&offset) {
            let key = entry.routing_key.clone();
            // Only release the key lock if this offset owns it
            if self.active_keys.get(&key) == Some(&offset) {
                self.active_keys.remove(&key);
            }
            // Decrement per-consumer count
            if let Some(count) = self.per_consumer_in_flight.get_mut(&entry.consumer_id) {
                *count = count.saturating_sub(1);
            }
            Some(key)
        } else {
            None
        };

        // Track acked offset for contiguous cursor advancement
        self.acked_offsets.insert(offset);
        let new_cursor = self.advance_safe_cursor();

        (unblocked_key, new_cursor)
    }

    /// Process a skipped message (filtered out — no consumer wants it).
    /// Treated like an immediate ack for cursor purposes.
    pub(crate) fn on_skipped(&mut self, offset: u64) -> Option<u64> {
        if self.first_offset.is_none() {
            self.first_offset = Some(offset);
        }
        self.acked_offsets.insert(offset);
        self.advance_safe_cursor()
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

    /// Take the first blocked message whose key is now free.
    pub(crate) fn take_unblocked(&mut self) -> Option<StreamMessage> {
        let pos = self.blocked_queue.iter().position(|msg| {
            let key = msg.effective_routing_key();
            !self.active_keys.contains_key(key)
        });
        pos.and_then(|i| self.blocked_queue.remove(i))
    }

    /// Remove a single in-flight entry by offset.
    /// Releases the key lock and decrements per-consumer count.
    /// Returns the removed entry if found.
    pub(crate) fn remove_entry(&mut self, offset: u64) -> Option<InFlightEntry> {
        if let Some(entry) = self.in_flight.remove(&offset) {
            if self.active_keys.get(&entry.routing_key) == Some(&offset) {
                self.active_keys.remove(&entry.routing_key);
            }
            if let Some(count) = self.per_consumer_in_flight.get_mut(&entry.consumer_id) {
                *count = count.saturating_sub(1);
            }
            Some(entry)
        } else {
            None
        }
    }

    /// Remove all blocked messages for a specific key.
    /// Used when a poison-dropped key needs its blocked queue cleared.
    pub(crate) fn drain_blocked_for_key(&mut self, key: &str) -> Vec<StreamMessage> {
        let mut drained = Vec::new();
        let mut i = 0;
        while i < self.blocked_queue.len() {
            if self.blocked_queue[i].effective_routing_key() == key {
                if let Some(msg) = self.blocked_queue.remove(i) {
                    drained.push(msg);
                }
            } else {
                i += 1;
            }
        }
        drained
    }

    /// Check if a consumer has capacity for more in-flight messages.
    pub(crate) fn consumer_has_capacity(&self, consumer_id: u64) -> bool {
        let count = self.per_consumer_in_flight.get(&consumer_id).copied().unwrap_or(0);
        count < self.max_per_consumer
    }

    /// Remove all in-flight entries for a given consumer (e.g., on disconnect/eviction).
    /// Returns the list of routing keys that were freed.
    ///
    /// Adds removed offsets to `acked_offsets` so the safe cursor can advance
    /// past them. Without this, orphaned offsets permanently stall cursor
    /// advancement since they're no longer in `in_flight` but were never acked.
    pub(crate) fn remove_consumer_entries(&mut self, consumer_id: u64) -> Vec<String> {
        let offsets: Vec<u64> = self
            .in_flight
            .iter()
            .filter(|(_, e)| e.consumer_id == consumer_id)
            .map(|(off, _)| *off)
            .collect();

        let mut freed_keys = Vec::new();
        for offset in &offsets {
            if let Some(entry) = self.in_flight.remove(offset) {
                if self.active_keys.get(&entry.routing_key) == Some(offset) {
                    self.active_keys.remove(&entry.routing_key);
                    freed_keys.push(entry.routing_key);
                }
                // Mark as acked so safe_cursor can advance past this offset.
                // The message is lost (consumer is gone), but the cursor must not stall.
                self.acked_offsets.insert(*offset);
            }
        }
        self.per_consumer_in_flight.remove(&consumer_id);

        // Try to advance cursor past newly-acked offsets
        if let Some(safe) = self.advance_safe_cursor() {
            tracing::trace!(safe_cursor = %safe, "cursor advanced after consumer eviction");
        }

        freed_keys
    }

    #[cfg(test)]
    pub(crate) fn blocked_count(&self) -> usize {
        self.blocked_queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use danube_core::message::MessageID;
    use std::collections::HashMap;

    fn make_msg(offset: u64, key: &str) -> StreamMessage {
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
            routing_key: Some(key.to_string()),
        }
    }

    #[test]
    fn window_block_same_key() {
        let mut window = InFlightWindow::new(100);
        let msg1 = make_msg(1, "order-123");
        let msg2 = make_msg(2, "order-123");

        // Dispatch first message
        let delivery = PendingDelivery::new(msg1.clone());
        window.mark_dispatched(1, "order-123".to_string(), 100, delivery);

        // Second message with same key should be blocked
        assert!(window.is_key_active("order-123"));
        window.push_blocked(msg2);
        assert_eq!(window.blocked_count(), 1);
    }

    #[test]
    fn window_ack_unblocks_key() {
        let mut window = InFlightWindow::new(100);
        let msg1 = make_msg(1, "order-123");

        let delivery = PendingDelivery::new(msg1.clone());
        window.mark_dispatched(1, "order-123".to_string(), 100, delivery);
        assert!(window.is_key_active("order-123"));

        let (unblocked_key, _) = window.on_ack(1);
        assert_eq!(unblocked_key, Some("order-123".to_string()));
        assert!(!window.is_key_active("order-123"));
    }

    #[test]
    fn safe_cursor_contiguous() {
        let mut window = InFlightWindow::new(100);

        // Dispatch offsets 1, 2, 3
        for i in 1..=3 {
            let msg = make_msg(i, &format!("key-{}", i));
            let delivery = PendingDelivery::new(msg.clone());
            window.mark_dispatched(i, format!("key-{}", i), 100, delivery);
        }

        // Ack in order: 1, 2, 3
        let (_, cursor) = window.on_ack(1);
        assert_eq!(cursor, Some(1));
        let (_, cursor) = window.on_ack(2);
        assert_eq!(cursor, Some(2));
        let (_, cursor) = window.on_ack(3);
        assert_eq!(cursor, Some(3));
    }

    #[test]
    fn safe_cursor_gap() {
        let mut window = InFlightWindow::new(100);

        // Dispatch offsets 1, 2, 3
        for i in 1..=3 {
            let msg = make_msg(i, &format!("key-{}", i));
            let delivery = PendingDelivery::new(msg.clone());
            window.mark_dispatched(i, format!("key-{}", i), 100, delivery);
        }

        // Ack out of order: 3, then 1
        let (_, cursor) = window.on_ack(3);
        // 3 acked but 1 and 2 still in-flight → no cursor advancement
        assert_eq!(cursor, None);

        let (_, cursor) = window.on_ack(1);
        // 1 acked, but 2 still in-flight → cursor advances to 1 only
        assert_eq!(cursor, Some(1));

        let (_, cursor) = window.on_ack(2);
        // 2 acked, now 1,2,3 all acked → cursor jumps to 3
        assert_eq!(cursor, Some(3));
    }

    #[test]
    fn take_unblocked_fifo() {
        let mut window = InFlightWindow::new(100);

        // Block two messages with different keys
        let msg_a = make_msg(10, "key-a");
        let msg_b = make_msg(11, "key-b");

        // key-a is active (in-flight)
        let delivery = PendingDelivery::new(make_msg(1, "key-a"));
        window.mark_dispatched(1, "key-a".to_string(), 100, delivery);

        window.push_blocked(msg_a); // blocked: key-a is active
        window.push_blocked(msg_b); // blocked: we put it there manually

        // key-b is NOT active, so take_unblocked should return msg_b
        let unblocked = window.take_unblocked();
        assert!(unblocked.is_some());
        assert_eq!(
            unblocked.unwrap().effective_routing_key(),
            "key-b"
        );
    }

    #[test]
    fn on_skipped_advances_cursor() {
        let mut window = InFlightWindow::new(100);

        // Skip offset 1
        let cursor = window.on_skipped(1);
        assert_eq!(cursor, Some(1));

        // Skip offset 2
        let cursor = window.on_skipped(2);
        assert_eq!(cursor, Some(2));

        // Skip offset 4 (gap at 3)
        let cursor = window.on_skipped(4);
        assert_eq!(cursor, None); // 3 is missing

        // Skip offset 3 (fills gap)
        let cursor = window.on_skipped(3);
        assert_eq!(cursor, Some(4)); // now 1,2,3,4 all done
    }

    #[test]
    fn has_capacity_limit() {
        let mut window = InFlightWindow::new(2);
        let msg1 = make_msg(1, "key-1");
        let msg2 = make_msg(2, "key-2");

        let d1 = PendingDelivery::new(msg1.clone());
        window.mark_dispatched(1, "key-1".to_string(), 100, d1);
        assert!(window.has_capacity()); // 1 in-flight, limit 2

        let d2 = PendingDelivery::new(msg2.clone());
        window.mark_dispatched(2, "key-2".to_string(), 100, d2);
        assert!(!window.has_capacity()); // 2 in-flight, limit 2
    }
}
