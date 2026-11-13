use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

use danube_core::message::StreamMessage;
use danube_core::storage::PersistentStorageError;

use crate::checkpoint::WalCheckpoint;

use super::cache::build_cache_stream;
use super::streaming_reader;
use super::writer::LogCommand;
use super::WalInner;

pub(crate) struct StatefulReader {
    inner: ReceiverStream<Result<StreamMessage, PersistentStorageError>>,
}

impl Stream for StatefulReader {
    type Item = Result<StreamMessage, PersistentStorageError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl StatefulReader {
    /// Create a continuity-preserving WAL reader.
    ///
    /// Functional flow
    /// - Snapshot the earliest cached offset to decide whether file replay is needed.
    /// - Subscribe to the live broadcast stream before any replay begins so new appends are not
    ///   missed during the Files/Cache catch-up phases.
    /// - Spawn a task that drives Files → Cache → Live and forwards results through an
    ///   `mpsc` channel exposed as the reader stream.
    pub(crate) async fn new(
        wal_inner: Arc<WalInner>,
        checkpoint_opt: Option<WalCheckpoint>,
        from_offset: u64,
    ) -> Result<Self, PersistentStorageError> {
        let cache_start = {
            let cache = wal_inner.cache.lock().await;
            cache.first_offset().unwrap_or(u64::MAX)
        };

        let live_rx = wal_inner.tx.subscribe();

        if from_offset >= cache_start {
            info!(
                target = "stateful_reader",
                from_offset,
                cache_start,
                decision = "Cache→Live",
                "replay decision: using cache, then live (skip files)"
            );
        } else {
            if checkpoint_opt.is_some() {
                info!(
                    target = "stateful_reader",
                    from_offset,
                    cache_start,
                    decision = "Files→Cache→Live",
                    "replay decision: using files first, then cache, then live"
                );
            } else {
                info!(
                    target = "stateful_reader",
                    from_offset,
                    cache_start,
                    decision = "Cache→Live",
                    "replay decision: no checkpoint; using cache, then live"
                );
            }
        }

        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            if let Err(e) = Self::run(wal_inner, checkpoint_opt, from_offset, cache_start, live_rx, tx.clone()).await {
                let _ = tx.send(Err(e)).await;
            }
        });

        Ok(Self {
            inner: ReceiverStream::new(rx),
        })
    }

    /// Drive the full replay lifecycle for a WAL reader.
    ///
    /// Functional phases
    /// - **Files**: if the request starts before the cache window and a checkpoint exists, replay
    ///   historical WAL files first.
    /// - **Cache**: drain any contiguous in-memory messages starting at `next_offset`.
    /// - **Live**: consume broadcast updates once the reader has caught up.
    ///
    /// Continuity invariant
    /// - `next_offset` is always the exact offset the reader expects next.
    /// - Offsets lower than `next_offset` are duplicates and are ignored.
    /// - Offsets greater than `next_offset` indicate a gap; the reader first tries to repair the
    ///   gap from cache before surfacing an error.
    ///
    /// Why this task exists
    /// - It centralizes the replay state machine in one async task instead of spreading it across
    ///   manual `poll_next` transitions.
    /// - It preserves ordering while still allowing the reader to recover from brief live-stream
    ///   lag if the missing messages are still present in cache.
    async fn run(
        wal_inner: Arc<WalInner>,
        checkpoint_opt: Option<WalCheckpoint>,
        from_offset: u64,
        cache_start: u64,
        mut live_rx: broadcast::Receiver<(u64, StreamMessage)>,
        tx: mpsc::Sender<Result<StreamMessage, PersistentStorageError>>,
    ) -> Result<(), PersistentStorageError> {
        let mut next_offset = from_offset;

        if from_offset < cache_start {
            if let Some(ckpt) = checkpoint_opt.as_ref() {
                let mut file_stream =
                    streaming_reader::stream_from_wal_files(ckpt, from_offset, 10 * 1024 * 1024)
                        .await?;
                while let Some(item) = file_stream.next().await {
                    match item {
                        Ok(msg) => {
                            let observed = msg.msg_id.topic_offset;
                            if observed < next_offset {
                                continue;
                            }
                            if observed > next_offset {
                                warn!(
                                    target = "stateful_reader",
                                    next_offset,
                                    observed,
                                    "file replay produced a gap; falling back to cache/live catch-up"
                                );
                                break;
                            }
                            if tx.send(Ok(msg)).await.is_err() {
                                return Ok(());
                            }
                            next_offset = next_offset.saturating_add(1);
                        }
                        Err(e) => {
                            warn!(
                                target = "stateful_reader",
                                error = %e,
                                next_offset,
                                "file replay failed; falling back to cache/live catch-up"
                            );
                            break;
                        }
                    }
                }
            }
        }

        loop {
            let cache_first = {
                let cache = wal_inner.cache.lock().await;
                cache.first_offset()
            };
            if cache_first.is_some_and(|first| next_offset < first) {
                Self::recover_gap(&wal_inner, &checkpoint_opt, &tx, &mut next_offset).await?;
            } else {
                Self::drain_cache(&wal_inner, &tx, &mut next_offset).await?;
                let post_cache_first = {
                    let cache = wal_inner.cache.lock().await;
                    cache.first_offset()
                };
                if post_cache_first.is_some_and(|first| next_offset < first) {
                    Self::recover_gap(&wal_inner, &checkpoint_opt, &tx, &mut next_offset).await?;
                }
            }

            match live_rx.recv().await {
                Ok((_off, msg)) => {
                    let observed = msg.msg_id.topic_offset;
                    if observed < next_offset {
                        continue;
                    }
                    if observed > next_offset {
                        warn!(
                            target = "stateful_reader",
                            next_offset,
                            observed,
                            "gap detected in live stream; refilling from files/cache"
                        );
                        Self::recover_gap(&wal_inner, &checkpoint_opt, &tx, &mut next_offset)
                            .await?;
                        if observed > next_offset {
                            Self::replay_from_files(&wal_inner, &checkpoint_opt, &tx, &mut next_offset)
                                .await?;
                            Self::drain_cache(&wal_inner, &tx, &mut next_offset).await?;
                        }
                        if observed < next_offset {
                            continue;
                        }
                        if observed > next_offset {
                            return Err(PersistentStorageError::Other(format!(
                                "wal live gap could not be repaired: expected {}, observed {}",
                                next_offset, observed
                            )));
                        }
                    }
                    if tx.send(Ok(msg)).await.is_err() {
                        return Ok(());
                    }
                    next_offset = next_offset.saturating_add(1);
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(
                        target = "stateful_reader",
                        next_offset,
                        skipped,
                        "live stream lagged; refilling from files and cache"
                    );
                    // Resubscribe immediately so new appends during replay don't hit stale queue lag.
                    live_rx = wal_inner.tx.subscribe();
                    Self::recover_gap(&wal_inner, &checkpoint_opt, &tx, &mut next_offset).await?;
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    /// Recover missed offsets during live tailing by inspecting cache bounds first:
    /// if `next_offset` predates the cache window, flush and replay from WAL files BEFORE
    /// draining cache so no hole between disk and cache can be skipped.
    async fn recover_gap(
        wal_inner: &Arc<WalInner>,
        fallback_ckpt: &Option<WalCheckpoint>,
        tx: &mpsc::Sender<Result<StreamMessage, PersistentStorageError>>,
        next_offset: &mut u64,
    ) -> Result<(), PersistentStorageError> {
        let cache_first = {
            let cache = wal_inner.cache.lock().await;
            cache.first_offset()
        };
        if cache_first.map_or(true, |first| *next_offset < first) {
            Self::replay_from_files(wal_inner, fallback_ckpt, tx, next_offset).await?;
        }
        Self::drain_cache(wal_inner, tx, next_offset).await?;

        // Re-check in case cache evicted past next_offset while draining/replaying.
        let post_cache_first = {
            let cache = wal_inner.cache.lock().await;
            cache.first_offset()
        };
        if post_cache_first.is_some_and(|first| *next_offset < first) {
            Self::replay_from_files(wal_inner, fallback_ckpt, tx, next_offset).await?;
            Self::drain_cache(wal_inner, tx, next_offset).await?;
        }
        Ok(())
    }

    /// Replay missed messages from local WAL files (rotated + active) starting at `*next_offset`.
    async fn replay_from_files(
        wal_inner: &Arc<WalInner>,
        fallback_ckpt: &Option<WalCheckpoint>,
        tx: &mpsc::Sender<Result<StreamMessage, PersistentStorageError>>,
        next_offset: &mut u64,
    ) -> Result<(), PersistentStorageError> {
        if wal_inner.checkpoint_path.is_some() {
            let (ack_tx, ack_rx) = oneshot::channel();
            if wal_inner.cmd_tx.send(LogCommand::Flush(ack_tx)).await.is_ok() {
                let _ = ack_rx.await;
            }
        }
        let latest_ckpt = match &wal_inner.ckpt_store {
            Some(store) => store.get_wal().await.or_else(|| fallback_ckpt.clone()),
            None => match &wal_inner.checkpoint_path {
                Some(path) => crate::checkpoint::CheckPoint::read_wal_from_path(path)
                    .await?
                    .or_else(|| fallback_ckpt.clone()),
                None => fallback_ckpt.clone(),
            },
        };

        let Some(ckpt) = latest_ckpt else {
            return Ok(());
        };

        let mut file_stream =
            streaming_reader::stream_from_wal_files(&ckpt, *next_offset, 10 * 1024 * 1024).await?;
        while let Some(item) = file_stream.next().await {
            match item {
                Ok(msg) => {
                    let observed = msg.msg_id.topic_offset;
                    if observed < *next_offset {
                        continue;
                    }
                    if observed > *next_offset {
                        warn!(
                            target = "stateful_reader",
                            next_offset = *next_offset,
                            observed,
                            "file replay hit gap during lag recovery; switching to cache"
                        );
                        break;
                    }
                    if tx.send(Ok(msg)).await.is_err() {
                        return Ok(());
                    }
                    *next_offset = next_offset.saturating_add(1);
                }
                Err(e) => {
                    warn!(
                        target = "stateful_reader",
                        error = %e,
                        next_offset = *next_offset,
                        "file replay failed during lag recovery; continuing with cache"
                    );
                    break;
                }
            }
        }
        Ok(())
    }

    /// Drain all currently available contiguous cache entries starting at `next_offset`.
    ///
    /// This helper reads the cache in bounded batches so lock hold times stay short. It keeps
    /// re-checking the cache until a pass yields no new items, which lets the caller catch up to
    /// the current cache frontier before switching back to live delivery.
    async fn drain_cache(
        wal_inner: &Arc<WalInner>,
        tx: &mpsc::Sender<Result<StreamMessage, PersistentStorageError>>,
        next_offset: &mut u64,
    ) -> Result<(), PersistentStorageError> {
        loop {
            let mut cache_stream = build_cache_stream(wal_inner.clone(), *next_offset, 512).await;
            let mut drained_any = false;

            while let Some(item) = cache_stream.next().await {
                let msg = item?;
                let observed = msg.msg_id.topic_offset;
                if observed < *next_offset {
                    continue;
                }
                if observed > *next_offset {
                    return Ok(());
                }
                if tx.send(Ok(msg)).await.is_err() {
                    return Ok(());
                }
                *next_offset = next_offset.saturating_add(1);
                drained_any = true;
            }

            if !drained_any {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::wal::{Wal, WalConfig, WalSyncMode};
    use danube_core::message::{MessageID, StreamMessage};
    use std::collections::HashMap;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_stateful_reader_recovers_from_broadcast_lag_via_files() {
        let tmp = TempDir::new().expect("temp dir");
        let cfg = WalConfig {
            dir: Some(tmp.path().to_path_buf()),
            file_name: Some("wal.0.log".to_string()),
            cache_capacity: Some(4),
            flush_interval_ms: Some(50),
            flush_max_batch_bytes: Some(1024),
            rotate_max_bytes: Some(1024),
            rotate_max_seconds: None,
            sync_mode: Some(WalSyncMode::OsCacheFlush),
        };
        let wal = Wal::with_config(cfg).await.expect("wal");

        // Start a reader from offset 0 before appending bursts
        let mut reader = wal.tail_reader(0, false).await.expect("reader");

        // Write more messages than broadcast buffer (256) and cache_capacity (4) without polling reader
        for i in 0..300u64 {
            let msg = StreamMessage {
                request_id: i,
                msg_id: MessageID {
                    producer_id: 1,
                    topic_name: "/default/lag-test".to_string(),
                    broker_addr: "localhost:6650".to_string(),
                    topic_offset: 0,
                },
                payload: format!("payload-{}", i).into_bytes().into(),
                publish_time: 1000 + i,
                producer_name: "p".to_string(),
                subscription_name: None,
                attributes: HashMap::new(),
                schema_id: None,
                schema_version: None,
                routing_key: None,
            };
            wal.append(&msg).await.expect("append");
        }
        wal.flush().await.expect("flush");

        let mut received = Vec::with_capacity(300);
        for _ in 0..300 {
            let item = tokio::time::timeout(std::time::Duration::from_secs(5), reader.next())
                .await
                .expect("timeout waiting for lagged reader")
                .expect("stream ended early")
                .expect("reader returned error instead of recovering from files");
            received.push(item.msg_id.topic_offset);
        }
        let expected: Vec<u64> = (0..300).collect();
        assert_eq!(received, expected);
        wal.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn test_stateful_reader_recovers_from_large_lag_and_cache_eviction_async_wal() {
        let tmp = TempDir::new().expect("temp dir");
        let cfg = WalConfig {
            dir: Some(tmp.path().to_path_buf()),
            file_name: Some("wal.0.log".to_string()),
            cache_capacity: Some(64),
            flush_interval_ms: Some(5_000),
            flush_max_batch_bytes: Some(1024 * 1024),
            rotate_max_bytes: Some(8192),
            rotate_max_seconds: None,
            sync_mode: Some(WalSyncMode::Async),
        };
        let wal = Wal::with_config(cfg).await.expect("wal");

        let make_msg = |i: u64| StreamMessage {
            request_id: i,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "/default/large-lag-test".to_string(),
                broker_addr: "localhost:6650".to_string(),
                topic_offset: 0,
            },
            payload: format!("payload-{i:04}-padding-data").into_bytes().into(),
            publish_time: 2000 + i,
            producer_name: "p".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
            schema_id: None,
            schema_version: None,
            routing_key: None,
        };

        // Append initial 50 messages and read them first so the reader enters live wait loop.
        for i in 0..50u64 {
            wal.append(&make_msg(i)).await.expect("append initial");
        }
        let mut reader = wal.tail_reader(0, false).await.expect("reader");
        let mut received = Vec::with_capacity(2_000);
        for _ in 0..50 {
            let msg = tokio::time::timeout(std::time::Duration::from_secs(5), reader.next())
                .await
                .expect("timeout reading initial")
                .expect("stream ended")
                .expect("reader error");
            received.push(msg.msg_id.topic_offset);
        }

        // Now burst 1,950 more messages without reading (overflowing mpsc(1024), broadcast(256), and cache(64))
        // and WITHOUT calling wal.flush() explicitly.
        for i in 50..2_000u64 {
            wal.append(&make_msg(i)).await.expect("append burst");
        }

        for _ in 50..2_000 {
            let msg = tokio::time::timeout(std::time::Duration::from_secs(10), reader.next())
                .await
                .expect("timeout waiting for lagged reader")
                .expect("stream ended early")
                .expect("reader returned error instead of recovering from files");
            received.push(msg.msg_id.topic_offset);
        }
        let expected: Vec<u64> = (0..2_000).collect();
        assert_eq!(received, expected);
        wal.shutdown().await.expect("shutdown");
    }
}
