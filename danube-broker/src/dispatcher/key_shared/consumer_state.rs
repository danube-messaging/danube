//! Consumer state for Key-Shared dispatch.
//!
//! Maps routing keys to consumers via consistent hashing (virtual-node hash ring),
//! respecting per-consumer key filter patterns (glob matching).
//!
//! # Consistent Hashing
//!
//! Each consumer is assigned `VNODES_PER_CONSUMER` virtual nodes on a hash ring.
//! When a consumer joins or leaves, only ~1/N of keys are remapped (where N is
//! the number of consumers), providing smooth rebalancing.
//!
//! This is similar to Pulsar's "Auto-split Consistent Hashing" mode.

use crate::consumer::Consumer;

/// Number of virtual nodes per consumer on the hash ring.
/// Higher values = more even distribution but slightly more memory/lookup cost.
/// Pulsar uses 100; we use the same default.
const VNODES_PER_CONSUMER: usize = 100;

/// Manages Key-Shared consumer membership and key-to-consumer routing.
#[derive(Debug)]
pub(crate) struct KeySharedConsumerState {
    pub(crate) consumers: Vec<ConsumerWithFilters>,

    /// Sorted virtual-node ring: (hash_point, consumer_index).
    /// Rebuilt on consumer add/remove.
    hash_ring: Vec<(u32, usize)>,
}

/// A consumer paired with its key filter patterns.
#[derive(Debug)]
pub(crate) struct ConsumerWithFilters {
    pub(crate) consumer: Consumer,
    pub(crate) key_filters: Vec<String>,
}

impl KeySharedConsumerState {
    pub(crate) fn new() -> Self {
        Self {
            consumers: Vec::new(),
            hash_ring: Vec::new(),
        }
    }

    pub(crate) fn add_consumer(&mut self, consumer: Consumer, filters: Vec<String>) {
        self.consumers.push(ConsumerWithFilters {
            consumer,
            key_filters: filters,
        });
        self.rebuild_ring();
    }

    pub(crate) fn remove_consumer(&mut self, consumer_id: u64) {
        self.consumers
            .retain(|c| c.consumer.consumer_id != consumer_id);
        self.rebuild_ring();
    }

    /// Select which consumer should handle this routing key.
    /// Returns None if no consumer matches (all filters reject the key).
    ///
    /// Algorithm:
    /// 1. Filter eligible consumers (those whose filters accept the key, or have no filters)
    /// 2. Consistent-hash the routing key on the virtual-node ring among eligible consumers
    pub(crate) fn select_consumer(&self, routing_key: &str) -> Option<usize> {
        if self.consumers.is_empty() {
            return None;
        }

        // 1. Filter: which consumers accept this key?
        let eligible: Vec<usize> = self
            .consumers
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                c.key_filters.is_empty() || c.key_filters.iter().any(|p| glob_match(routing_key, p))
            })
            .map(|(idx, _)| idx)
            .collect();

        if eligible.is_empty() {
            return None; // No consumer wants this key
        }
        if eligible.len() == 1 {
            return Some(eligible[0]);
        }

        // 2. Consistent hash: find the consumer on the ring
        let key_hash = fnv1a_hash_32(routing_key);

        // If all consumers are eligible, use the full ring directly
        if eligible.len() == self.consumers.len() {
            return Some(self.lookup_ring(&self.hash_ring, key_hash));
        }

        // Filter the ring to only eligible consumers
        // (this is the less common path — only when some consumers have key_filters)
        let filtered_ring: Vec<(u32, usize)> = self
            .hash_ring
            .iter()
            .filter(|(_, idx)| eligible.contains(idx))
            .cloned()
            .collect();

        if filtered_ring.is_empty() {
            return None;
        }
        Some(self.lookup_ring(&filtered_ring, key_hash))
    }

    /// Lookup the consumer for a hash value on a given ring.
    /// Finds the first virtual node with hash >= key_hash (clockwise walk).
    /// Wraps around to the first node if key_hash is past the last node.
    fn lookup_ring(&self, ring: &[(u32, usize)], key_hash: u32) -> usize {
        // Binary search for the first node >= key_hash
        match ring.binary_search_by_key(&key_hash, |(h, _)| *h) {
            Ok(pos) => ring[pos].1,
            Err(pos) => {
                if pos < ring.len() {
                    ring[pos].1
                } else {
                    ring[0].1 // wrap around
                }
            }
        }
    }

    /// Rebuild the virtual-node hash ring from current consumer list.
    fn rebuild_ring(&mut self) {
        self.hash_ring.clear();
        for (idx, cwf) in self.consumers.iter().enumerate() {
            let consumer_id = cwf.consumer.consumer_id;
            for vnode in 0..VNODES_PER_CONSUMER {
                // Hash: "{consumer_id}-{vnode}" — deterministic placement
                let vnode_key = format!("{}-{}", consumer_id, vnode);
                let hash = fnv1a_hash_32(&vnode_key);
                self.hash_ring.push((hash, idx));
            }
        }
        self.hash_ring.sort_by_key(|(h, _)| *h);
    }

    pub(crate) fn get_consumer_mut(&mut self, idx: usize) -> &mut Consumer {
        &mut self.consumers[idx].consumer
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }

    pub(crate) fn disconnect_all(&mut self) {
        self.consumers.clear();
        self.hash_ring.clear();
    }
}

/// FNV-1a hash producing a 32-bit value for ring placement.
/// Uses the standard 64-bit FNV-1a and folds to 32 bits via xor-folding
/// for better distribution than a simple truncation.
pub(crate) fn fnv1a_hash_32(key: &str) -> u32 {
    let h64 = fnv1a_hash_64(key);
    // XOR-fold 64→32: combines upper and lower halves
    ((h64 >> 32) ^ h64) as u32
}

/// FNV-1a 64-bit hash used internally for 32-bit xor-folding.
fn fnv1a_hash_64(key: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in key.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Simple glob matching: `*` matches any sequence, `?` matches one char.
fn glob_match(text: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    simple_glob(text.as_bytes(), pattern.as_bytes())
}

/// Iterative glob matching with `*` backtracking.
fn simple_glob(text: &[u8], pattern: &[u8]) -> bool {
    let (mut ti, mut pi) = (0usize, 0usize);
    let mut star = usize::MAX;
    let mut match_pos = 0usize;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star = pi;
            match_pos = ti;
            pi += 1;
        } else if star != usize::MAX {
            pi = star + 1;
            match_pos += 1;
            ti = match_pos;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_deterministic() {
        assert_eq!(fnv1a_hash_32("order-123"), fnv1a_hash_32("order-123"));
        assert_ne!(fnv1a_hash_32("order-123"), fnv1a_hash_32("order-124"));
    }

    #[test]
    fn glob_match_star() {
        assert!(glob_match("user-123", "user-*"));
        assert!(glob_match("user-abc", "user-*"));
        assert!(!glob_match("order-123", "user-*"));
    }

    #[test]
    fn glob_match_question() {
        assert!(glob_match("us-a", "us-?"));
        assert!(!glob_match("us-ab", "us-?"));
    }

    #[test]
    fn glob_match_wildcard_all() {
        assert!(glob_match("anything", "*"));
        assert!(glob_match("", "*"));
    }

    #[test]
    fn glob_match_exact() {
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "other"));
    }

    #[test]
    fn glob_match_complex() {
        assert!(glob_match("eu-west-1-order", "eu-*-order"));
        assert!(!glob_match("eu-west-1-payment", "eu-*-order"));
        assert!(glob_match("a-b-c", "*-*-*"));
    }

    /// Verify that the consistent hash ring provides stable mapping:
    /// same key → same consumer, regardless of how many times we query.
    #[test]
    fn consistent_hash_stable() {
        use std::sync::Arc;
        use tokio::sync::{mpsc, Mutex};

        let mut state = KeySharedConsumerState::new();

        // Create 3 mock consumers
        for i in 0..3u64 {
            let (tx, _rx) = mpsc::channel(4);
            let session = Arc::new(Mutex::new(crate::consumer::ConsumerSession::new()));
            let rx_arc = Arc::new(Mutex::new(_rx));
            let consumer = crate::consumer::Consumer::new(
                i + 100,
                &format!("consumer-{}", i),
                3, // KeyShared
                "test-topic",
                "test-sub",
                tx,
                session,
                rx_arc,
            );
            state.add_consumer(consumer, Vec::new());
        }

        // Same key should always select the same consumer
        let c1 = state.select_consumer("order-123");
        let c2 = state.select_consumer("order-123");
        assert_eq!(c1, c2);

        // Different keys should still be deterministic
        let ca = state.select_consumer("key-alpha");
        let cb = state.select_consumer("key-alpha");
        assert_eq!(ca, cb);
    }

    /// Verify that adding a consumer only remaps ~1/N of keys (consistent hashing property).
    #[test]
    fn consistent_hash_minimal_remapping() {
        use std::sync::Arc;
        use tokio::sync::{mpsc, Mutex};

        let make_consumer = |id: u64| -> Consumer {
            let (tx, _rx) = mpsc::channel(4);
            let session = Arc::new(Mutex::new(crate::consumer::ConsumerSession::new()));
            let rx_arc = Arc::new(Mutex::new(_rx));
            crate::consumer::Consumer::new(
                id,
                &format!("consumer-{}", id),
                3,
                "test-topic",
                "test-sub",
                tx,
                session,
                rx_arc,
            )
        };

        let mut state = KeySharedConsumerState::new();
        state.add_consumer(make_consumer(1), Vec::new());
        state.add_consumer(make_consumer(2), Vec::new());

        // Record assignments for 1000 keys with 2 consumers
        let keys: Vec<String> = (0..1000).map(|i| format!("key-{}", i)).collect();
        let before: Vec<Option<usize>> = keys.iter().map(|k| state.select_consumer(k)).collect();

        // Add a third consumer
        state.add_consumer(make_consumer(3), Vec::new());

        // Record assignments with 3 consumers
        let after: Vec<Option<usize>> = keys.iter().map(|k| state.select_consumer(k)).collect();

        // Count how many keys changed assignment
        let changed = before
            .iter()
            .zip(after.iter())
            .filter(|(b, a)| b != a)
            .count();

        // With consistent hashing, ~1/3 of keys should remap (±tolerance).
        // With modulo hashing, nearly ALL keys would remap.
        // Allow 15%-55% range to account for hash distribution variance on 1000 keys.
        let change_pct = changed as f64 / keys.len() as f64;
        assert!(
            change_pct < 0.55,
            "Too many keys remapped: {}% ({}/{}). Expected ~33% for consistent hashing.",
            (change_pct * 100.0) as u32,
            changed,
            keys.len()
        );
        assert!(
            change_pct > 0.15,
            "Too few keys remapped: {}% ({}/{}). Expected ~33% for consistent hashing.",
            (change_pct * 100.0) as u32,
            changed,
            keys.len()
        );
    }
}
