use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::{broadcast, mpsc};
use tokio_stream::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

use danube_core::message::StreamMessage;
use danube_core::storage::PersistentStorageError;

use crate::checkpoint::WalCheckpoint;

use super::cache::build_cache_stream;
use super::streaming_reader;
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
            Self::drain_cache(&wal_inner, &tx, &mut next_offset).await?;

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
                            "gap detected in live stream; refilling from cache"
                        );
                        Self::drain_cache(&wal_inner, &tx, &mut next_offset).await?;
                        if observed < next_offset {
                            continue;
                        }
                        if observed > next_offset {
                            return Err(PersistentStorageError::Other(format!(
                                "wal live gap could not be repaired from cache: expected {}, observed {}",
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
                        "live stream lagged; refilling from cache"
                    );
                    let before = next_offset;
                    Self::drain_cache(&wal_inner, &tx, &mut next_offset).await?;
                    if next_offset == before {
                        return Err(PersistentStorageError::Other(format!(
                            "wal live lag could not be repaired from cache at offset {}",
                            next_offset
                        )));
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

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
                    return Err(PersistentStorageError::Other(format!(
                        "wal cache gap: expected {}, observed {}",
                        *next_offset, observed
                    )));
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
