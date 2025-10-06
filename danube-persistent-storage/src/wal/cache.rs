use std::collections::BTreeMap;
use std::sync::Arc;

use danube_core::message::StreamMessage;
use danube_core::storage::TopicStream;
use futures::StreamExt;

/// Ordered in-memory cache keyed by WAL offset.
///
/// Notes
/// - Backed by `BTreeMap<u64, StreamMessage>` so iteration is naturally sorted by offset.
/// - Used to satisfy replay for recent messages without hitting disk.
#[derive(Debug, Default)]
pub(crate) struct Cache {
    map: BTreeMap<u64, StreamMessage>,
}

/// Build a cache replay stream that yields messages starting at `from_offset` in bounded batches.
/// This keeps lock hold times short and memory per reader bounded.
pub(crate) async fn build_cache_stream(
    wal_inner: Arc<super::WalInner>,
    mut from_offset: u64,
    batch_size: usize,
) -> TopicStream {
    use futures::stream;

    let mut segments: Vec<TopicStream> = Vec::new();
    loop {
        let batch: Vec<StreamMessage> = {
            let cache = wal_inner.cache.lock().await;
            cache
                .range_from(from_offset)
                .take(batch_size)
                .map(|(_off, msg)| msg)
                .collect()
        };
        if batch.is_empty() {
            break;
        }
        // advance from_offset to after the last yielded item
        if let Some(last) = batch.last() {
            from_offset = last.msg_id.segment_offset.saturating_add(1);
        }
        let seg = Box::pin(stream::iter(batch.into_iter().map(Ok))) as TopicStream;
        segments.push(seg);
        if segments.len() >= 32 {
            break;
        }
    }

    let mut it = segments.into_iter();
    if let Some(mut acc) = it.next() {
        for s in it {
            acc = Box::pin(acc.chain(s));
        }
        acc
    } else {
        Box::pin(futures::stream::empty())
    }
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
