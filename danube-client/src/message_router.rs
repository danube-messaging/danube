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
}
