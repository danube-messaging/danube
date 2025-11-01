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

/// SubscriptionEngine encapsulates reliable delivery mechanics for a subscription:
/// - Polling from TopicStore-backed TopicStream (WAL tail + CloudReader handoff)
/// - Durable progress tracking via TopicResources (optional)
pub(crate) struct SubscriptionEngine {
    pub(crate) _subscription_name: String,
    pub(crate) topic_name: Option<String>,
    pub(crate) topic_store: Arc<TopicStore>,
    pub(crate) stream: Option<TopicStream>,
    // progress persistence (optional)
    pub(crate) progress_resources: Option<tokio::sync::Mutex<TopicResources>>,
    pub(crate) last_acked: Option<u64>,
    pub(crate) dirty: bool,
    pub(crate) last_flush_at: Instant,
    pub(crate) sub_progress_flush_interval: Duration,
    // Phase 3: optional per-subscription dispatch limiter (messages/sec)
    pub(crate) dispatch_rate_limiter: Option<std::sync::Arc<RateLimiter>>,
}

impl SubscriptionEngine {
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
        s.progress_resources = Some(tokio::sync::Mutex::new(progress_resources));
        s.sub_progress_flush_interval = sub_progress_flush_interval;
        s.dispatch_rate_limiter = limiter;
        s
    }

    // with_progress_and_limiter removed in favor of unified new_with_progress

    /// Initialize underlying stream. For a brand-new subscription we default to Latest.
    pub(crate) async fn init_stream_latest(&mut self) -> Result<()> {
        let stream = self
            .topic_store
            .create_reader(StartPosition::Latest)
            .await?;
        self.stream = Some(stream);
        Ok(())
    }

    /// Initialize using persisted progress if available, otherwise Latest
    pub(crate) async fn init_stream_from_progress_or_latest(&mut self) -> Result<()> {
        if let (Some(topic), Some(res_mx)) =
            (self.topic_name.as_deref(), self.progress_resources.as_ref())
        {
            let mut res = res_mx.lock().await;
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

    /// Polls next message from the underlying stream.
    /// Reliable pacing: if a message is ready but the limiter denies, wait and retry
    /// instead of returning None. This preserves reliability and provides smooth pacing.
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

    /// Called by dispatcher when a message has been acknowledged by a consumer.
    pub(crate) async fn on_acked(&mut self, msg_id: MessageID) -> Result<()> {
        // Track last acked offset
        self.last_acked = Some(msg_id.topic_offset);
        self.dirty = true;

        // Debounced flush: persist at most every `flush_interval`
        if let Some(res_mx) = &self.progress_resources {
            let now = Instant::now();
            if self.dirty
                && now.duration_since(self.last_flush_at) >= self.sub_progress_flush_interval
            {
                if let Some(off) = self.last_acked {
                    let mut res = res_mx.lock().await;
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

    /// Immediately flush the last_acked progress to metadata (best-effort).
    /// Used during unload pause to persist cursor without waiting for debounce.
    pub(crate) async fn flush_progress_now(&mut self) -> Result<()> {
        if let (Some(off), Some(res_mx), Some(topic)) = (
            self.last_acked,
            self.progress_resources.as_ref(),
            self.topic_name.clone(),
        ) {
            let mut res = res_mx.lock().await;
            let _ = res
                .set_subscription_cursor(&self._subscription_name, &topic, off)
                .await;
            self.dirty = false;
            self.last_flush_at = Instant::now();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::danube_service::LocalCache;
    use crate::resources::TopicResources;
    use danube_core::message::MessageID;
    use danube_metadata_store::{MemoryStore, MetadataStorage};
    use danube_persistent_storage::wal::{Wal, WalConfig};
    use danube_persistent_storage::WalStorage;
    use tokio::time::{sleep, Duration};

    /// What this test validates
    ///
    /// - Ensures `SubscriptionEngine` persists the subscription cursor (offset) to metadata
    ///   after an acknowledgment, when the debounce window (`sub_progress_flush_interval`) elapsed.
    ///
    /// Inputs / Setup
    /// - In-memory `MetadataStorage::InMemory` with `LocalCache`, wrapped by `TopicResources`.
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
        // In-memory metadata store and local cache
        let mem = MemoryStore::new().await.expect("init memory store");
        let store = MetadataStorage::InMemory(mem);
        let local_cache = LocalCache::new(store.clone());
        let topic_resources = TopicResources::new(local_cache, store.clone());
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
        let mut reader = topic_resources_reader;
        let got = reader
            .get_subscription_cursor("sub-a", "/ns/topic-a")
            .await
            .unwrap();
        assert_eq!(got, Some(5));
    }
}
