use anyhow::Result;
use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::{StartPosition, TopicStream};
use futures::StreamExt;
use std::sync::Arc;
use tokio::time::Instant;

/// Minimal trait to decouple SubscriptionEngine from a concrete TopicStore location.
/// The real `TopicStore` in the broker should implement this trait.
pub(crate) trait TopicStoreLike: Send + Sync {
    fn create_reader(
        &self,
        start: StartPosition,
    ) -> core::pin::Pin<Box<dyn core::future::Future<Output = Result<TopicStream>> + Send + '_>>;
}

/// Allow using unit type for Non-Reliable unified dispatchers (reader is never used there)
impl TopicStoreLike for () {
    fn create_reader(
        &self,
        _start: StartPosition,
    ) -> core::pin::Pin<Box<dyn core::future::Future<Output = Result<TopicStream>> + Send + '_>>
    {
        Box::pin(
            async move { unreachable!("Non-reliable unified dispatchers do not use TopicStore") },
        )
    }
}

/// SubscriptionEngine encapsulates reliable delivery mechanics for a subscription:
/// - Polling from TopicStore-backed TopicStream (WAL tail + CloudReader handoff)
/// - Tracking in-flight message for ack
/// - Simple retry tracking hooks (policy to be implemented as needed)
pub(crate) struct SubscriptionEngine {
    // Currently unused. Keep for tracing/diagnostics; prefix with '_' to silence warnings.
    pub(crate) _subscription_name: String,
    pub(crate) topic_store: Arc<dyn TopicStoreLike>,
    pub(crate) stream: Option<TopicStream>,
    pub(crate) pending_ack: Option<(u64, MessageID)>,
    // Retry fields reserved for future policy; underscore to silence warnings for now
    pub(crate) _retry_count: u8,
    pub(crate) _last_retry_at: Option<Instant>,
}

impl SubscriptionEngine {
    pub(crate) fn new(subscription_name: String, topic_store: Arc<dyn TopicStoreLike>) -> Self {
        Self {
            _subscription_name: subscription_name,
            topic_store,
            stream: None,
            pending_ack: None,
            _retry_count: 0,
            _last_retry_at: None,
        }
    }

    /// Initialize underlying stream. For a brand-new subscription we default to Latest.
    pub(crate) async fn init_stream_latest(&mut self) -> Result<()> {
        let stream = self
            .topic_store
            .create_reader(StartPosition::Latest)
            .await?;
        self.stream = Some(stream);
        Ok(())
    }

    /// Polls next message from the underlying stream.
    pub(crate) async fn poll_next(&mut self) -> Result<Option<StreamMessage>> {
        let stream = match &mut self.stream {
            Some(s) => s,
            None => return Ok(None),
        };
        tokio::select! {
            maybe_msg = stream.next() => {
                Ok(maybe_msg.transpose()?)
            }
        }
    }

    /// Called by dispatcher when a message has been acknowledged by a consumer.
    pub(crate) async fn on_acked(&mut self, _request_id: u64, _msg_id: MessageID) -> Result<()> {
        // TODO: persist progress via ProgressUpdater when integrated
        self.pending_ack = None;
        Ok(())
    }
}
