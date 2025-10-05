use std::sync::Arc;

use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::info;

use super::cache::build_cache_stream;
use super::streaming_reader;
use super::WalInner;
use crate::checkpoint::WalCheckpoint;

/// Read and decode WAL frames in `[from_offset, to_exclusive)` from file at `path`.
///
/// Frame format
/// - `[u64 offset][u32 len][u32 crc][bytes]` with CRC32 over `bytes`.
/// - Stops on EOF, CRC mismatch, or when `off >= to_exclusive`.
///
/// Returns ordered `(offset, StreamMessage)` pairs suitable for stitching with cache replay.
// Legacy range reader replaced by streaming implementation in streaming_reader.rs

/// Build a combined replay+live stream starting from `from_offset` using the provided
/// optional WAL file path, a snapshot of cache items, and a broadcast receiver for live items.
///
/// Ordering guarantee
/// - The `cache_snapshot` must be ordered ascending by offset. This holds because it is produced
///   by `Cache::range_from()` over a `BTreeMap`, whose iterators yield keys in sorted order.
///   Therefore no additional sorting is required here.
///
/// Replay strategy
/// - If `from_offset < cache_start`, replay `[from_offset, cache_start)` from the file.
/// - Then append cache items `>= max(from_offset, cache_start)`.
/// - Finally switch to live tail (broadcast) starting after the last replayed offset.
#[allow(dead_code)]
pub(crate) async fn build_tail_stream(
    checkpoint_opt: Option<WalCheckpoint>,
    wal_inner: Arc<WalInner>,
    from_offset: u64,
    rx: broadcast::Receiver<(u64, StreamMessage)>,
) -> Result<TopicStream, PersistentStorageError> {
    // Determine the earliest offset currently present in cache
    let cache_start = {
        let cache = wal_inner.cache.lock().await;
        // Ensure the iterator drops before leaving the block to avoid E0597
        let mut iter = cache.range_from(0);
        let first = iter.next().map(|(off, _)| off).unwrap_or(u64::MAX);
        first
    };

    // Phase 1: Stream items from local WAL files if needed (using rotated file list from checkpoint)
    let file_stream: Option<TopicStream> = if let Some(ckpt) = checkpoint_opt {
        if from_offset < cache_start {
            info!(
                target = "wal_reader",
                from_offset,
                cache_start,
                decision = "Files→Cache→Live",
                "replay decision: using files first, then cache, then live"
            );
            // Default chunk size: 10 MiB (aligned with uploader)
            let chunk = 10 * 1024 * 1024;
            let fs = streaming_reader::stream_from_wal_files(&ckpt, from_offset, chunk).await?;
            Some(fs)
        } else {
            info!(
                target = "wal_reader",
                from_offset,
                cache_start,
                decision = "Cache→Live",
                "replay decision: using cache, then live (skip files)"
            );
            None
        }
    } else {
        None
    };

    // Build cache stream from requested offset (bounded batches under brief locks)
    let cache_stream = build_cache_stream(wal_inner.clone(), from_offset, 512).await;

    // Phase 3: Live tailing from broadcast, starting at computed watermark

    // Live tailing from broadcast. Since append stamps offsets, we don't need to mutate messages.
    // We don't attempt duplicate filtering here; for robust de-dup we will switch to a stateful reader.
    let live_stream = BroadcastStream::new(rx).map(move |item| match item {
        Ok((_off, msg)) => Ok(msg),
        Err(e) => Err(PersistentStorageError::Other(format!(
            "broadcast error: {}",
            e
        ))),
    });

    // Chain: file (optional) -> cache -> live
    let stream: TopicStream = if let Some(fs) = file_stream {
        Box::pin(fs.chain(cache_stream).chain(live_stream)) as TopicStream
    } else {
        Box::pin(cache_stream.chain(live_stream)) as TopicStream
    };
    Ok(stream)
}
