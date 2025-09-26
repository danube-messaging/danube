use std::path::PathBuf;

use crc32fast;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};
use tokio::io::AsyncReadExt;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::{debug, warn};

/// Read and decode WAL frames in `[from_offset, to_exclusive)` from file at `path`.
///
/// Frame format
/// - `[u64 offset][u32 len][u32 crc][bytes]` with CRC32 over `bytes`.
/// - Stops on EOF, CRC mismatch, or when `off >= to_exclusive`.
///
/// Returns ordered `(offset, StreamMessage)` pairs suitable for stitching with cache replay.
pub(crate) async fn read_file_range(
    path: &PathBuf,
    from_offset: u64,
    to_exclusive: u64,
) -> Result<Vec<(u64, StreamMessage)>, PersistentStorageError> {
    let mut f = tokio::fs::File::open(path)
        .await
        .map_err(|e| PersistentStorageError::Io(format!("open wal file failed: {}", e)))?;
    let mut out = Vec::new();
    let mut header = [0u8; 8 + 4 + 4];
    loop {
        match f.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(PersistentStorageError::Io(format!(
                    "wal read header failed: {}",
                    e
                )));
            }
        }
        let off = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(header[12..16].try_into().unwrap());

        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf)
            .await
            .map_err(|e| PersistentStorageError::Io(format!("wal read frame failed: {}", e)))?;
        let actual_crc = crc32fast::hash(&buf);
        if actual_crc != crc {
            warn!(target = "wal", path = %path.display(), offset = off, "CRC mismatch detected; stopping file replay");
            break; // treat CRC mismatch as logical end-of-log
        }
        if off >= to_exclusive {
            break;
        }
        if off >= from_offset {
            let msg: StreamMessage = bincode::deserialize(&buf).map_err(|e| {
                PersistentStorageError::Io(format!("bincode deserialize failed: {}", e))
            })?;
            out.push((off, msg));
        }
    }
    Ok(out)
}

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
    wal_path_opt: Option<PathBuf>,
    cache_snapshot: Vec<(u64, StreamMessage)>,
    from_offset: u64,
    rx: broadcast::Receiver<(u64, StreamMessage)>,
) -> Result<TopicStream, PersistentStorageError> {
    // Determine the earliest offset in the (already ordered) cache snapshot
    let cache_start = cache_snapshot.first().map(|(o, _)| *o).unwrap_or(u64::MAX);

    // Replay items from file if needed
    let mut replay_items: Vec<(u64, StreamMessage)> = Vec::new();
    if let Some(path) = &wal_path_opt {
        if from_offset < cache_start {
            let mut file_part = read_file_range(path, from_offset, cache_start).await?;
            debug!(target = "wal", from = from_offset, to_exclusive = cache_start, file = %path.display(), count = file_part.len(), "replayed frames from file");
            replay_items.append(&mut file_part);
        }
    }

    // Now extend with cache items that are >= max(from_offset, cache_start)
    let cache_lower = from_offset.max(cache_start);
    for (off, msg) in cache_snapshot.into_iter() {
        if off >= cache_lower {
            replay_items.push((off, msg));
        }
    }

    // Compute live watermark BEFORE moving replay_items
    let start_from_live = match replay_items.last() {
        Some((last, _)) => last.saturating_add(1),
        None => from_offset,
    };

    // Inject WAL offset into MessageID.segment_offset for replayed items
    let replay_stream = tokio_stream::iter(replay_items.into_iter().map(|(off, mut msg)| {
        msg.msg_id.segment_offset = off;
        Ok(msg)
    }));

    // Live tailing from broadcast, starting after last_replayed if any replay happened,
    // otherwise start exactly at from_offset to include the very next append.
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

    // Chain replay then live
    let stream = replay_stream.chain(live_stream);
    Ok(Box::pin(stream))
}
