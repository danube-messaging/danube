//! Consumer state for Key-Shared dispatch.
//!
//! Maps routing keys to consumers via deterministic hashing,
//! respecting per-consumer key filter patterns (glob matching).

use crate::consumer::Consumer;

/// Manages Key-Shared consumer membership and key-to-consumer routing.
#[derive(Debug)]
pub(crate) struct KeySharedConsumerState {
    pub(crate) consumers: Vec<ConsumerWithFilters>,
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
        }
    }

    pub(crate) fn add_consumer(&mut self, consumer: Consumer, filters: Vec<String>) {
        self.consumers.push(ConsumerWithFilters {
            consumer,
            key_filters: filters,
        });
    }

    pub(crate) fn remove_consumer(&mut self, consumer_id: u64) {
        self.consumers
            .retain(|c| c.consumer.consumer_id != consumer_id);
    }

    /// Select which consumer should handle this routing key.
    /// Returns None if no consumer matches (all filters reject the key).
    ///
    /// Algorithm:
    /// 1. Filter eligible consumers (those whose filters accept the key, or have no filters)
    /// 2. Hash the routing key deterministically among eligible consumers
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
                c.key_filters.is_empty()
                    || c.key_filters.iter().any(|p| glob_match(routing_key, p))
            })
            .map(|(idx, _)| idx)
            .collect();

        if eligible.is_empty() {
            return None; // No consumer wants this key
        }
        if eligible.len() == 1 {
            return Some(eligible[0]);
        }

        // 2. Hash: deterministic selection among eligible consumers
        let hash = fnv1a_hash(routing_key);
        let idx = eligible[hash as usize % eligible.len()];
        Some(idx)
    }

    pub(crate) fn get_consumer_mut(&mut self, idx: usize) -> &mut Consumer {
        &mut self.consumers[idx].consumer
    }

    pub(crate) fn get_consumer(&self, idx: usize) -> &Consumer {
        &self.consumers[idx].consumer
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.consumers.len()
    }

    pub(crate) fn disconnect_all(&mut self) {
        self.consumers.clear();
    }

    /// Find consumer index by consumer_id.
    pub(crate) fn find_by_id(&self, consumer_id: u64) -> Option<usize> {
        self.consumers
            .iter()
            .position(|c| c.consumer.consumer_id == consumer_id)
    }
}

/// FNV-1a hash for deterministic key-to-consumer mapping.
pub(crate) fn fnv1a_hash(key: &str) -> u64 {
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
        assert_eq!(fnv1a_hash("order-123"), fnv1a_hash("order-123"));
        assert_ne!(fnv1a_hash("order-123"), fnv1a_hash("order-124"));
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
}
