use anyhow::Result;
use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::{StartPosition, TopicStream};
use futures::StreamExt;
use std::sync::Arc;
use tokio::select;
use tokio::time::{Duration, Instant};

use crate::rate_limiter::RateLimiter;
use crate::resources::TopicResources;
use crate::topic::TopicStore;

/// # SubscriptionEngine
///
/// Core component for **reliable message delivery** in durable subscriptions.
///
/// ## Overview
///
/// The SubscriptionEngine manages the stateful mechanics of at-least-once delivery:
/// - **Stream Management**: Polls messages from TopicStore-backed TopicStream (WAL → Cloud Storage)
/// - **Progress Tracking**: Persists subscription cursor (last acked offset) to metadata store
/// - **Lag Detection**: Monitors subscription position relative to WAL head
/// - **Rate Limiting**: Optional throttling of message dispatch for flow control
///
/// ## Architecture
///
/// ```text
/// Topic WAL → TopicStream → SubscriptionEngine → Reliable Dispatcher → Consumer
///                              ↓ ack tracking
///                         TopicResources (metadata)
/// ```
///
/// ## Responsibility Boundary
///
/// - **SubscriptionEngine**: Stream polling, progress persistence, lag detection
/// - **Reliable Dispatcher**: Ack-gating, pending message buffer, consumer management
/// - **TopicStore**: WAL access, stream creation
/// - **TopicResources**: Metadata persistence (subscription cursor storage)
///
/// ## Usage Pattern
///
/// ```rust,ignore
/// // 1. Create engine (reliable dispatcher owns this)
/// let engine = SubscriptionEngine::new_with_progress(
///     "my-subscription".to_string(),
///     "/my/topic".to_string(),
///     topic_store,
///     topic_resources,
///     Duration::from_secs(5), // flush interval
///     Some(rate_limiter),
/// );
///
/// // 2. Initialize stream (typically from persisted progress)
/// engine.init_stream_from_progress_or_latest().await?;
///
/// // 3. Poll messages (dispatcher loop)
/// if let Some(msg) = engine.poll_next().await? {
///     // dispatch to consumer...
/// }
///
/// // 4. Track acknowledgments
/// engine.on_acked(msg_id).await?;
///
/// // 5. Check lag (heartbeat)
/// if engine.has_lag() {
///     // trigger dispatch...
/// }
/// ```
pub(crate) struct SubscriptionEngine {
    /// Subscription name (for metadata lookups)
    pub(crate) _subscription_name: String,

    /// Topic name (required for progress persistence)
    pub(crate) topic_name: Option<String>,

    /// Reference to the topic's storage (WAL-backed)
    pub(crate) topic_store: Arc<TopicStore>,

    /// Active stream for reading messages from the topic
    /// Created via `init_stream_*` methods
    pub(crate) stream: Option<TopicStream>,

    /// Metadata store for persisting subscription progress (cursor)
    /// Used by durable subscriptions to survive broker restarts
    pub(crate) progress_resources: Option<TopicResources>,

    /// Last acknowledged offset (consumer confirmed delivery)
    /// This is the subscription's "cursor" - used for lag detection and persistence
    pub(crate) last_acked: Option<u64>,

    /// Dirty flag indicating cursor needs to be flushed to metadata
    /// Set on ack, cleared after successful flush
    pub(crate) dirty: bool,

    /// Timestamp of last progress flush to metadata
    /// Used for debounced persistence (avoids metadata write on every ack)
    pub(crate) last_flush_at: Instant,

    /// Interval for debounced progress flushes (default: 5 seconds)
    /// Balances durability (frequent flushes) vs metadata load (infrequent flushes)
    pub(crate) sub_progress_flush_interval: Duration,

    /// Optional rate limiter for throttling message dispatch
    /// Used for flow control to prevent overwhelming consumers
    pub(crate) dispatch_rate_limiter: Option<std::sync::Arc<RateLimiter>>,
}

impl SubscriptionEngine {
    /// Create a basic SubscriptionEngine without progress tracking.
    ///
    /// Used primarily for testing. Production code should use `new_with_progress()`.
    ///
    /// # Arguments
    ///
    /// * `subscription_name` - Name of the subscription
    /// * `topic_store` - Reference to the topic's storage (WAL-backed)
    pub(crate) fn new(subscription_name: String, topic_store: Arc<TopicStore>) -> Self {
        Self {
            _subscription_name: subscription_name,
            topic_name: None,
            topic_store,
            stream: None,
            progress_resources: None,
            last_acked: None,
            dirty: false,
            last_flush_at: Instant::now(),
            sub_progress_flush_interval: Duration::from_secs(5),
            dispatch_rate_limiter: None,
        }
    }

    /// Create a SubscriptionEngine with progress tracking and optional rate limiting.
    ///
    /// This is the standard constructor for durable subscriptions.
    ///
    /// # Arguments
    ///
    /// * `subscription_name` - Name of the subscription (for metadata lookups)
    /// * `topic_name` - Name of the topic
    /// * `topic_store` - Reference to the topic's storage
    /// * `progress_resources` - Metadata store for persisting cursor
    /// * `sub_progress_flush_interval` - Debounce interval for cursor persistence (typically 5s)
    /// * `limiter` - Optional rate limiter for throttling dispatch
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let engine = SubscriptionEngine::new_with_progress(
    ///     "my-sub".to_string(),
    ///     "/my/topic".to_string(),
    ///     topic_store,
    ///     topic_resources,
    ///     Duration::from_secs(5),
    ///     None,
    /// );
    /// ```
    pub(crate) fn new_with_progress(
        subscription_name: String,
        topic_name: String,
        topic_store: Arc<TopicStore>,
        progress_resources: TopicResources,
        sub_progress_flush_interval: Duration,
        limiter: Option<std::sync::Arc<RateLimiter>>,
    ) -> Self {
        let mut s = Self::new(subscription_name, topic_store);
        s.topic_name = Some(topic_name);
        s.progress_resources = Some(progress_resources);
        s.sub_progress_flush_interval = sub_progress_flush_interval;
        s.dispatch_rate_limiter = limiter;
        s
    }

    /// Initialize the message stream starting from the **latest** position.
    ///
    /// Used for brand-new subscriptions with no persisted cursor.
    /// Messages produced before subscription creation are skipped.
    ///
    /// # Returns
    ///
    /// `Ok(())` on successful stream creation, `Err` if TopicStore fails.
    pub(crate) async fn init_stream_latest(&mut self) -> Result<()> {
        let stream = self
            .topic_store
            .create_reader(StartPosition::Latest)
            .await?;
        self.stream = Some(stream);
        Ok(())
    }

    /// Initialize the message stream from **persisted cursor** or latest if none exists.
    ///
    /// This is the standard initialization for durable subscriptions:
    /// 1. Check metadata for persisted cursor (last acked offset)
    /// 2. If found, create stream starting at `cursor + 1`
    /// 3. If not found, fall back to `init_stream_latest()`
    ///
    /// Ensures subscriptions survive broker restarts without message loss.
    ///
    /// # Returns
    ///
    /// `Ok(())` on successful stream creation, `Err` if TopicStore or metadata access fails.
    pub(crate) async fn init_stream_from_progress_or_latest(&mut self) -> Result<()> {
        if let (Some(topic), Some(res)) =
            (self.topic_name.as_deref(), self.progress_resources.as_ref())
        {
            if let Ok(Some(cursor)) = res
                .get_subscription_cursor(&self._subscription_name, topic)
                .await
            {
                let stream = self
                    .topic_store
                    .create_reader(StartPosition::Offset(cursor.saturating_add(1)))
                    .await?;
                self.stream = Some(stream);
                return Ok(());
            }
        }
        self.init_stream_latest().await
    }

    /// Poll the next message from the topic stream.
    ///
    /// This is the core method for pulling messages from the WAL/cloud storage.
    ///
    /// # Rate Limiting (Reliable Pacing)
    ///
    /// If a rate limiter is configured and denies the request:
    /// - The method **waits and retries** (exponential backoff up to 100ms)
    /// - Does NOT return `None` immediately
    /// - Preserves reliability while providing smooth flow control
    ///
    /// # Returns
    ///
    /// - `Ok(Some(msg))` - Next message available and rate limit allows
    /// - `Ok(None)` - Stream exhausted (caught up with WAL)
    /// - `Err(_)` - Stream error
    ///
    /// # Usage in Dispatcher
    ///
    /// ```rust,ignore
    /// // Reliable dispatcher poll loop
    /// match engine.poll_next().await? {
    ///     Some(msg) => {
    ///         // dispatch to consumer
    ///     }
    ///     None => {
    ///         // caught up, wait for notification
    ///     }
    /// }
    /// ```
    pub(crate) async fn poll_next(&mut self) -> Result<Option<StreamMessage>> {
        let stream = match &mut self.stream {
            Some(s) => s,
            None => return Ok(None),
        };
        let mut backoff = Duration::from_millis(10);
        loop {
            let msg_opt = select! {
                maybe_msg = stream.next() => {
                    maybe_msg.transpose()?
                }
            };
            if let Some(_msg) = &msg_opt {
                if let Some(lim) = &self.dispatch_rate_limiter {
                    if !lim.try_acquire(1).await {
                        // Pacing: wait and retry until allowed
                        tokio::time::sleep(backoff).await;
                        // Optional: cap backoff to a reasonable upper bound
                        if backoff < Duration::from_millis(100) {
                            backoff = backoff.saturating_mul(2).min(Duration::from_millis(100));
                        }
                        continue;
                    }
                }
            }
            return Ok(msg_opt);
        }
    }

    /// Record message acknowledgment from consumer.
    ///
    /// Updates the subscription cursor and triggers debounced persistence.
    ///
    /// # Debounced Persistence
    ///
    /// The cursor is NOT written to metadata immediately:
    /// 1. Mark `dirty = true` and update `last_acked`
    /// 2. If `sub_progress_flush_interval` has elapsed since last flush:
    ///    - Write cursor to metadata via `TopicResources`
    ///    - Clear `dirty` flag
    ///    - Update `last_flush_at` timestamp
    /// 3. Otherwise, defer flush to next ack
    ///
    /// This batching reduces metadata write load while ensuring eventual durability.
    ///
    /// # Arguments
    ///
    /// * `msg_id` - The acknowledged message ID (contains `topic_offset`)
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())`. Flush failures are logged but don't error (best-effort).
    pub(crate) async fn on_acked(&mut self, msg_id: MessageID) -> Result<()> {
        // Track last acked offset
        self.last_acked = Some(msg_id.topic_offset);
        self.dirty = true;

        // Debounced flush: persist at most every `flush_interval`
        if let Some(res) = &self.progress_resources {
            let now = Instant::now();
            if self.dirty
                && now.duration_since(self.last_flush_at) >= self.sub_progress_flush_interval
            {
                if let Some(off) = self.last_acked {
                    // Best-effort; if it fails, keep dirty=true to retry on next ack
                    if res
                        .set_subscription_cursor(
                            &self._subscription_name,
                            self.topic_name.as_deref().unwrap_or(""),
                            off,
                        )
                        .await
                        .is_ok()
                    {
                        self.last_flush_at = now;
                        self.dirty = false;
                    }
                }
            }
        }
        Ok(())
    }

    /// Immediately flush subscription cursor to metadata.
    ///
    /// Bypasses the debounce interval to force persistence.
    ///
    /// # Use Cases
    ///
    /// - **Consumer disconnect**: Ensure cursor is saved before removing subscription
    /// - **Broker shutdown**: Persist final state before process exit
    /// - **Manual flush**: Explicit durability checkpoint
    ///
    /// # Best-Effort Semantics
    ///
    /// Write failures are silently ignored (logged internally).
    /// This is by design - cursor will be retried on next ack.
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())`.
    pub(crate) async fn flush_progress_now(&mut self) -> Result<()> {
        if let (Some(off), Some(res), Some(topic)) = (
            self.last_acked,
            self.progress_resources.as_ref(),
            self.topic_name.clone(),
        ) {
            let _ = res
                .set_subscription_cursor(&self._subscription_name, &topic, off)
                .await;
            self.dirty = false;
            self.last_flush_at = Instant::now();
        }
        Ok(())
    }

    /// Check if subscription is lagging behind the topic's WAL.
    ///
    /// This is the **core of lag detection** for the heartbeat watchdog.
    ///
    /// # Algorithm
    ///
    /// ```text
    /// wal_head = topic_store.get_last_committed_offset()
    ///
    /// if wal_head == 0:
    ///     return false  // WAL empty, no lag
    ///
    /// if last_acked is None:
    ///     return stream.is_some() && wal_head > 0  // startup case
    ///
    /// // Caught up: cursor == wal_head - 1
    /// // (wal_head is NEXT offset to be written)
    /// return last_acked < wal_head - 1
    /// ```
    ///
    /// # Returns
    ///
    /// - `true` - Subscription is behind, messages waiting
    /// - `false` - Subscription is caught up
    ///
    /// # Usage in Dispatcher
    ///
    /// The reliable dispatcher's heartbeat (500ms) calls this method:
    /// ```rust,ignore
    /// if engine.has_lag() {
    ///     // trigger PollAndDispatch
    /// }
    /// ```
    pub(crate) fn has_lag(&self) -> bool {
        let wal_head = self.topic_store.get_last_committed_offset();

        // If WAL is empty (head = 0), no lag
        if wal_head == 0 {
            return false;
        }

        // Compare our cursor against WAL head
        match self.last_acked {
            Some(cursor) => {
                // We are caught up if: cursor == wal_head - 1
                // (because head is the NEXT offset to be written)
                // We have lag if: cursor < wal_head - 1
                cursor < wal_head.saturating_sub(1)
            }
            None => {
                // No acks yet - we might be at startup
                // Check if stream was initialized and if there are messages
                self.stream.is_some() && wal_head > 0
            }
        }
    }

    /// Get detailed lag information for monitoring and metrics.
    ///
    /// Returns a `LagInfo` struct containing:
    /// - `subscription_cursor`: Last acked offset (None if no acks yet)
    /// - `wal_head`: Current WAL head (next offset to be written)
    /// - `lag_messages`: Number of messages subscription is behind
    /// - `has_lag`: Boolean indicating if lag exists
    ///
    /// # Calculation
    ///
    /// ```text
    /// lag_messages = (wal_head - 1) - last_acked
    /// ```
    ///
    /// # Usage
    ///
    /// ```rust,ignore
    /// let info = engine.get_lag_info();
    /// gauge!("subscription_lag_messages").set(info.lag_messages as f64);
    /// ```
    pub(crate) fn get_lag_info(&self) -> LagInfo {
        let wal_head = self.topic_store.get_last_committed_offset();
        let cursor = self.last_acked;

        let lag = match cursor {
            Some(c) if wal_head > 0 => wal_head.saturating_sub(1).saturating_sub(c),
            _ => 0,
        };

        LagInfo {
            subscription_cursor: cursor,
            wal_head,
            lag_messages: lag,
            has_lag: self.has_lag(),
        }
    }
}

/// Diagnostic information about subscription lag.
///
/// Returned by `SubscriptionEngine::get_lag_info()` for monitoring and metrics.
///
/// # Fields
///
/// - `subscription_cursor`: Last acked offset (cursor position)
/// - `wal_head`: Current WAL head (next offset to write)
/// - `lag_messages`: Count of unprocessed messages
/// - `has_lag`: Boolean flag for quick lag check
///
/// # Example
///
/// ```rust,ignore
/// let info = engine.get_lag_info();
/// if info.has_lag {
///     println!("Subscription lagging by {} messages", info.lag_messages);
///     println!("Cursor: {:?}, WAL Head: {}", info.subscription_cursor, info.wal_head);
/// }
/// ```
#[derive(Debug, Clone)]
pub(crate) struct LagInfo {
    /// The subscription's current cursor (last acked offset)
    #[allow(dead_code)]
    pub(crate) subscription_cursor: Option<u64>,
    /// The WAL's current head (next offset to be written)
    #[allow(dead_code)]
    pub(crate) wal_head: u64,
    /// Number of messages the subscription is behind
    pub(crate) lag_messages: u64,
    /// Whether the subscription currently has lag
    pub(crate) has_lag: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_storage::MetadataStorage;
    use crate::resources::TopicResources;
    use danube_core::message::MessageID;
    use danube_core::metadata::MemoryStore;
    use danube_persistent_storage::wal::{Wal, WalConfig};
    use danube_persistent_storage::WalStorage;
    use tokio::time::{sleep, Duration};

    /// What this test validates
    ///
    /// - Ensures `SubscriptionEngine` persists the subscription cursor (offset) to metadata
    ///   after an acknowledgment, when the debounce window (`sub_progress_flush_interval`) elapsed.
    ///
    /// Inputs / Setup
    /// - In-memory `MetadataStorage::InMemory` wrapped by `TopicResources`.
    /// - `SubscriptionEngine::new_with_progress(...)` configured for subscription `sub-a` on topic `/ns/topic-a`.
    /// - `sub_progress_flush_interval` set to 50ms (short for test).
    /// - A `MessageID` with `topic_offset = 5` to simulate an acked WAL offset.
    ///
    /// Execution Steps
    /// 1. Call `on_acked(100, msg_id)` to mark engine state as dirty with last_acked = 5.
    /// 2. Wait (>50ms) to surpass the debounce window.
    /// 3. Call `on_acked(101, msg_id)` again to trigger the debounced flush to metadata.
    ///
    /// Expected Behavior
    /// - `TopicResources::get_subscription_cursor("sub-a", "/ns/topic-a")` returns `Some(5)`.
    /// - Confirms the cursor was written once the debounce interval elapsed.
    #[tokio::test]
    async fn persists_cursor_after_debounce() {
        // In-memory metadata store
        let mem = MemoryStore::new().await.expect("init memory store");
        let store = MetadataStorage::InMemory(mem);
        let topic_resources = TopicResources::new(store.clone());
        let topic_resources_reader = topic_resources.clone();

        // Engine with short debounce interval (50ms). TopicStore not used here, but must be provided.
        let wal = Wal::with_config(WalConfig::default())
            .await
            .expect("init wal");
        let wal_storage = WalStorage::from_wal(wal);
        let ts = TopicStore::new("/ns/topic-a".to_string(), wal_storage);
        let mut engine = SubscriptionEngine::new_with_progress(
            "sub-a".to_string(),
            "/ns/topic-a".to_string(),
            Arc::new(ts),
            topic_resources,
            Duration::from_millis(50),
            None,
        );

        // First ack (should mark dirty but not flush immediately)
        let msg_id = MessageID {
            producer_id: 1,
            topic_name: "/ns/topic-a".to_string(),
            broker_addr: "127.0.0.1:8080".to_string(),
            topic_offset: 5,
        };
        engine.on_acked(msg_id.clone()).await.unwrap();

        // Wait longer than debounce interval
        sleep(Duration::from_millis(60)).await;

        // Second ack (same offset); should trigger flush
        engine.on_acked(msg_id.clone()).await.unwrap();

        // Validate cursor persisted
        let reader = topic_resources_reader;
        let got = reader
            .get_subscription_cursor("sub-a", "/ns/topic-a")
            .await
            .unwrap();
        assert_eq!(got, Some(5));
    }
}

// Lag detection tests are in a separate file for better organization
#[cfg(test)]
#[path = "subscription_engine_test.rs"]
mod lag_tests;
