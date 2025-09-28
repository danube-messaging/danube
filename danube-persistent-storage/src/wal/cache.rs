use std::collections::BTreeMap;

use danube_core::message::StreamMessage;

/// Ordered in-memory cache keyed by WAL offset.
///
/// Notes
/// - Backed by `BTreeMap<u64, StreamMessage>` so iteration is naturally sorted by offset.
/// - Used to satisfy replay for recent messages without hitting disk.
#[derive(Debug, Default)]
pub(crate) struct Cache {
    map: BTreeMap<u64, StreamMessage>,
}

impl Cache {
    pub(crate) fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    /// Insert a message at its assigned WAL `offset`.
    pub(crate) fn insert(&mut self, offset: u64, msg: StreamMessage) {
        self.map.insert(offset, msg);
    }

    /// Evict the oldest entries until the cache holds at most `capacity` items.
    pub(crate) fn evict_to(&mut self, capacity: usize) {
        while self.map.len() > capacity {
            if let Some(oldest) = self.map.keys().next().cloned() {
                self.map.remove(&oldest);
            } else {
                break;
            }
        }
    }

    /// Iterate `(offset, message)` pairs with offsets `>= from` in ascending order.
    pub(crate) fn range_from(&self, from: u64) -> impl Iterator<Item = (u64, StreamMessage)> + '_ {
        self.map.range(from..).map(|(k, v)| (*k, v.clone()))
    }

    /// Test helper: get item by exact offset.
    #[allow(dead_code)]
    pub(crate) fn get(&self, offset: u64) -> Option<(u64, StreamMessage)> {
        self.map.get(&offset).cloned().map(|m| (offset, m))
    }
}
