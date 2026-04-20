use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub(crate) struct MessageRouter {
    partitions: usize,
    last_partition: AtomicUsize,
}

impl MessageRouter {
    pub(crate) fn new(partitions: usize) -> Self {
        MessageRouter {
            partitions,
            last_partition: AtomicUsize::new(0),
        }
    }
    pub(crate) fn round_robin(&self) -> usize {
        self.last_partition.fetch_add(1, Ordering::Relaxed) % self.partitions
    }

    /// Route by hashing the routing key to a deterministic partition.
    /// Ensures all messages with the same key always go to the same partition,
    /// which is required for per-key ordering on partitioned Key-Shared topics.
    pub(crate) fn key_route(&self, routing_key: &str) -> usize {
        let hash = Self::fnv1a_hash(routing_key);
        hash as usize % self.partitions
    }

    fn fnv1a_hash(key: &str) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in key.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}
