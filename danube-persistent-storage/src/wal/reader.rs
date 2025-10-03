use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::info;

use crate::checkpoint::WalCheckpoint;
use super::streaming_reader;

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
pub(crate) async fn build_tail_stream(
    checkpoint_opt: Option<WalCheckpoint>,
    cache_snapshot: Vec<(u64, StreamMessage)>,
    from_offset: u64,
    rx: broadcast::Receiver<(u64, StreamMessage)>,
) -> Result<TopicStream, PersistentStorageError> {
    // Determine the earliest offset in the (already ordered) cache snapshot
    let cache_start = cache_snapshot.first().map(|(o, _)| *o).unwrap_or(u64::MAX);

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

    // Compute live watermark:
    // - If cache is empty, start live from `from_offset`.
    // - Otherwise, start after the last cached item we will emit to avoid duplicates.
    let start_from_live = if cache_snapshot.is_empty() {
        from_offset
    } else {
        let last_cache_off = cache_snapshot.last().map(|(o, _)| *o).unwrap_or(from_offset);
        from_offset.max(last_cache_off.saturating_add(1))
    };

    // Now extend with cache items that are >= max(from_offset, cache_start)
    let cache_lower = from_offset.max(cache_start);
    let cache_stream = tokio_stream::iter(
        cache_snapshot
            .into_iter()
            .filter(move |(off, _)| *off >= cache_lower)
            .map(|(off, mut msg)| {
                msg.msg_id.segment_offset = off;
                Ok(msg)
            }),
    );

    // Phase 3: Live tailing from broadcast, starting at computed watermark
    
    let live_stream = BroadcastStream::new(rx).filter_map(move |item| match item {
        Ok((off, mut msg)) if off >= start_from_live => {
            msg.msg_id.segment_offset = off;
            Some(Ok(msg))
        }
        Ok(_) => None, // skip older
        Err(e) => Some(Err(PersistentStorageError::Other(format!(
            "broadcast error: {}",
            e
        )))),
    });

    // Chain: file (optional) -> cache -> live
    let stream: TopicStream = if let Some(fs) = file_stream {
        Box::pin(fs.chain(cache_stream).chain(live_stream)) as TopicStream
    } else {
        Box::pin(cache_stream.chain(live_stream)) as TopicStream
    };
    Ok(stream)
}
