use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Future, Stream, StreamExt};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{info, warn};

use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};

use crate::checkpoint::WalCheckpoint;

use super::cache::build_cache_stream;
use super::streaming_reader;
use super::WalInner;

enum ReaderPhase {
    Files { stream: TopicStream },
    Cache { stream: TopicStream },
    Live { stream: TopicStream },
}

pub(crate) struct StatefulReader {
    wal_inner: Arc<WalInner>,
    phase: ReaderPhase,
    last_yielded: u64, // u64::MAX means "none yielded yet"
}

impl Stream for StatefulReader {
    type Item = Result<StreamMessage, PersistentStorageError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.phase {
                ReaderPhase::Files { stream } => match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(msg))) => {
                        self.update_last_yielded(&msg);
                        return Poll::Ready(Some(Ok(msg)));
                    }
                    Poll::Ready(Some(Err(_e))) => {
                        // If files fail (e.g., pruned), try cache from next needed offset; otherwise propagate
                        let from = self.last_yielded.saturating_add(1);
                        warn!(
                            target = "stateful_reader",
                            last_yielded = self.last_yielded,
                            next_from = from,
                            "file phase errored, transitioning to cache"
                        );
                        if let Poll::Ready(()) = self.poll_transition_to_cache(cx, from) {
                            continue;
                        }
                        // Can't build now, re-poll
                        return Poll::Pending;
                    }
                    Poll::Ready(None) => {
                        // Files exhausted. Transition to Cache from next needed or current cache start
                        let from = self.last_yielded.saturating_add(1);
                        info!(
                            target = "stateful_reader",
                            last_yielded = self.last_yielded,
                            next_from = from,
                            "file phase exhausted, transitioning to cache"
                        );
                        if let Poll::Ready(()) = self.poll_transition_to_cache(cx, from) {
                            continue;
                        }
                        return Poll::Pending;
                    }
                    Poll::Pending => return Poll::Pending,
                },

                ReaderPhase::Cache { stream } => match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(msg))) => {
                        self.update_last_yielded(&msg);
                        return Poll::Ready(Some(Ok(msg)));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        // Cache should be reliable; propagate error
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        // Cache exhausted; before going live, attempt to refill from cache
                        // in case new items were appended between our last batch and now.
                        let from = self.last_yielded.saturating_add(1);
                        if let Poll::Ready(()) = self.poll_transition_to_cache(cx, from) {
                            // Successfully built another cache stream; continue consuming cache
                            continue;
                        }
                        // Could not immediately build (Pending), or none available: proceed to live
                        info!(
                            target = "stateful_reader",
                            last_yielded = self.last_yielded,
                            "cache exhausted (or pending refill), transitioning to live"
                        );
                        self.transition_to_live();
                        return Poll::Pending;
                    }
                    Poll::Pending => return Poll::Pending,
                },

                ReaderPhase::Live { stream } => match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(msg))) => {
                        // Drop duplicates only if we have yielded at least once.
                        if self.last_yielded != u64::MAX && msg.msg_id.topic_offset <= self.last_yielded {
                            continue;
                        }
                        // Detect gaps due to late subscription to broadcast: if we observe an
                        // offset greater than the next expected, fall back to cache to fill the gap
                        // starting from last_yielded + 1. We intentionally drop this broadcast item
                        // because it is present in the cache (cache is updated before broadcast in append()).
                        let expected = if self.last_yielded == u64::MAX { 0 } else { self.last_yielded + 1 };
                        if msg.msg_id.topic_offset > expected {
                            warn!(
                                target = "stateful_reader",
                                last_yielded = self.last_yielded,
                                observed = msg.msg_id.topic_offset,
                                expected,
                                "gap detected in live stream; transitioning to cache to fill"
                            );
                            if let Poll::Ready(()) = self.poll_transition_to_cache(cx, expected) {
                                // Now consuming from cache; do not yield the current broadcast item
                                continue;
                            }
                            return Poll::Pending;
                        }
                        // Exactly next: yield normally
                        self.update_last_yielded(&msg);
                        return Poll::Ready(Some(Ok(msg)));
                    }
                    Poll::Ready(Some(Err(_e))) => {
                        // On broadcast lag, fall back to cache to catch up
                        let from = self.last_yielded.saturating_add(1);
                        warn!(
                            target = "stateful_reader",
                            last_yielded = self.last_yielded,
                            next_from = from,
                            "live phase lag/error, transitioning to cache"
                        );
                        if let Poll::Ready(()) = self.poll_transition_to_cache(cx, from) {
                            continue;
                        }
                        return Poll::Pending;
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

impl StatefulReader {
    pub(crate) async fn new(
        wal_inner: Arc<WalInner>,
        checkpoint_opt: Option<WalCheckpoint>,
        from_offset: u64,
    ) -> Result<Self, PersistentStorageError> {
        // Decide first phase: if requested offset within cache, start with Cache; otherwise Files.
        let cache_start = {
            let cache = wal_inner.cache.lock().await;
            let mut it = cache.range_from(0);
            it.next().map(|(off, _)| off).unwrap_or(u64::MAX)
        };

        let phase = if from_offset >= cache_start {
            // Cache → Live
            info!(
                target = "stateful_reader",
                from_offset,
                cache_start,
                decision = "Cache→Live",
                "replay decision: using cache, then live (skip files)"
            );
            let cache_stream = build_cache_stream(wal_inner.clone(), from_offset, 512).await;
            ReaderPhase::Cache {
                stream: cache_stream,
            }
        } else {
            // Files (from requested) → Cache → Live
            if let Some(ckpt) = checkpoint_opt.as_ref() {
                info!(
                    target = "stateful_reader",
                    from_offset,
                    cache_start,
                    decision = "Files→Cache→Live",
                    "replay decision: using files first, then cache, then live"
                );
                let fs =
                    streaming_reader::stream_from_wal_files(ckpt, from_offset, 10 * 1024 * 1024)
                        .await?;
                ReaderPhase::Files { stream: fs }
            } else {
                info!(
                    target = "stateful_reader",
                    from_offset,
                    cache_start,
                    decision = "Cache→Live",
                    "replay decision: no checkpoint; using cache, then live"
                );
                // No files available, start from cache directly
                let cache_stream = build_cache_stream(wal_inner.clone(), from_offset, 512).await;
                ReaderPhase::Cache {
                    stream: cache_stream,
                }
            }
        };

        // Initialize last_yielded to sentinel when starting from 0 so we can compute expected correctly.
        let last = if from_offset == 0 { u64::MAX } else { from_offset - 1 };
        Ok(Self { wal_inner, phase, last_yielded: last })
    }

    #[inline]
    fn update_last_yielded(&mut self, msg: &StreamMessage) {
        self.last_yielded = msg.msg_id.topic_offset;
    }

    #[inline]
    fn transition_to_live(&mut self) {
        let rx = self.wal_inner.tx.subscribe();
        let live = BroadcastStream::new(rx).map(|item| match item {
            Ok((_off, msg)) => Ok(msg),
            Err(e) => Err(PersistentStorageError::Other(format!(
                "broadcast error: {}",
                e
            ))),
        });
        self.phase = ReaderPhase::Live {
            stream: Box::pin(live),
        };
    }

    /// Asynchronously build a cache stream from the given offset and transition when ready.
    fn poll_transition_to_cache(&mut self, cx: &mut Context<'_>, from: u64) -> Poll<()> {
        let wal_inner = self.wal_inner.clone();
        let fut = async move { build_cache_stream(wal_inner, from, 512).await };
        let mut fut = Box::pin(fut);
        match fut.as_mut().poll(cx) {
            Poll::Ready(cache_stream) => {
                self.phase = ReaderPhase::Cache {
                    stream: cache_stream,
                };
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
