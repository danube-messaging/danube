use anyhow::{anyhow, Result};
use danube_core::message::StreamMessage;
use metrics::counter;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::broker_metrics::{CONSUMER_BYTES_OUT_TOTAL, CONSUMER_MESSAGES_OUT_TOTAL};
use crate::utils::get_random_id;

/// Represents a consumer connected and associated with a Subscription.
///
/// # Architecture Overview
///
/// The Consumer struct manages the complete message pipeline from dispatcher to client:
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────────────────────┐
/// │                           MESSAGE FLOW PIPELINE                               │
/// └──────────────────────────────────────────────────────────────────────────────┘
///
///  1. Producer      2. Dispatcher         3. Internal       4. gRPC          5. Client
///     publishes        routes to             Channel          Stream             App
///     message          consumer              buffers          sends
///       │                 │                     │               │                 │
///       ├─────────────────▶                     │               │                 │
///       │ send_message()  │                     │               │                 │
///       │ or              │                     │               │                 │
///       │ try_send_message()                    │               │                 │
///       │                 │                     │               │                 │
///       │                 │ tx_cons.send()      │               │                 │
///       │                 │ or tx_cons.try_send()               │                 │
///       │                 ├────────────────────▶│               │                 │
///       │                 │    (Producer)       │               │                 │
///       │                 │                     │  rx_cons      │                 │
///       │                 │                     │  .recv()      │                 │
///       │                 │                     ├──────────────▶│                 │
///       │                 │                     │  (Consumer)   │                 │
///       │                 │                     │               │  grpc_tx.send() │
///       │                 │                     │               ├────────────────▶│
///       │                 │                     │               │                 │
///       ▼                 ▼                     ▼               ▼                 ▼
///
/// Components:
/// - tx_cons: Sender half - used by dispatcher to push messages
/// - rx_cons: Receiver half - used by gRPC handler to pull messages
/// - session: Tracks connection state (active, cancellation, session_id)
/// - reliable dispatch blocks on a full internal channel; non-reliable dispatch can drop/skip on full
/// ```
///
/// # Lock Separation Strategy
///
/// To avoid deadlock, we use separate locks for different access patterns:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                          LOCK OWNERSHIP                                  │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///  Dispatcher Thread              │              gRPC Stream Thread
///                                 │
///  1. Check if consumer active    │              1. Lock rx_cons (hold forever)
///     session.lock().await        │                 rx_cons.lock().await
///     ↓                           │                 ↓
///  2. Read session.active         │              2. Loop: receive messages
///     (quick unlock)              │                 loop { rx.recv().await }
///     ↓                           │                 ↓
///  3. Send message                │              3. Forward to client
///     send_message().await        │                 grpc_tx.send().await
///     or try_send_message()       │
///                                 │
///  ✓ No deadlock: different locks │
/// ```
///
/// # Takeover Mechanism
///
/// When a consumer reconnects with the same name (single-attach):
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                          TAKEOVER FLOW                                   │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///  Old Connection                New Connection (same consumer_name)
///       │                               │
///       │  Streaming messages           │  1. subscribe() called
///       │  via rx_cons                  │     ↓
///       │                               │  2. session.cancel_stream()
///       │  ◄─────────────────────────────────┘  cancels old task
///       │  (cancellation.cancel())      │
///       │                               │  3. session.takeover()
///       │  Task exits                   │     - new session_id
///       │  (cancelled)                  │     - new cancellation token
///       │                               │     - set active=true
///       ▼                               │
///    Closed                             │  4. receive_messages() starts
///                                       │     new streaming task
///                                       │     ↓
///                                       │  Streaming messages
///                                       ▼  via same rx_cons
/// ```
///
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Consumer {
    /// Unique identifier for this consumer instance.
    /// Generated randomly on creation and never changes.
    pub(crate) consumer_id: u64,

    /// Human-readable name provided by the client.
    /// Used for identifying consumers and enforcing single-attach semantics
    /// (multiple connections with same name trigger takeover).
    pub(crate) consumer_name: String,

    /// Type of subscription this consumer belongs to.
    /// - 0: Exclusive (one consumer per subscription)
    /// - 1: Shared (round-robin distribution)
    /// - 2: Failover (active + standby consumers)
    pub(crate) subscription_type: i32,

    /// Full topic name this consumer is subscribed to.
    /// Example: "/default/my-topic"
    pub(crate) topic_name: String,

    /// Name of the subscription this consumer belongs to.
    /// Multiple consumers can share the same subscription (except Exclusive).
    pub(crate) subscription_name: String,

    /// **Message sender (Producer end of the internal channel)**.
    ///
    /// Used by: Dispatcher
    /// - Reliable dispatchers call `consumer.send_message(msg)` which uses this sender
    /// - Non-reliable dispatchers call `consumer.try_send_message(msg)` which uses this sender
    /// - Pushes messages into the internal channel (buffer size: 4)
    /// - Reliable sends block on full channel (backpressure)
    /// - Non-reliable sends do not wait on full channel; callers can drop or skip the message
    ///
    /// Flow: `Dispatcher → tx_cons.send()/try_send() → Channel → rx_cons.recv() → gRPC`
    pub(crate) tx_cons: mpsc::Sender<StreamMessage>,

    /// **Session state: active status, cancellation token, session ID**.
    ///
    /// Used by: Both Dispatcher and gRPC handler
    /// - Dispatcher: Checks `session.active` to see if consumer is healthy
    /// - gRPC handler: Updates `session.active` on connect/disconnect
    /// - Takeover: `session.takeover()` cancels old task and creates new session
    ///
    /// Locked separately from `rx_cons` to avoid deadlock:
    /// - Dispatcher needs quick status checks (brief lock)
    /// - gRPC handler holds `rx_cons` lock for entire stream duration
    pub(crate) session: Arc<Mutex<ConsumerSession>>,

    pub(crate) active: Arc<AtomicBool>,

    /// **Message receiver (Consumer end of the internal channel)**.
    ///
    /// Used by: gRPC streaming task (consumer_handler.rs)
    /// - Locked once at the start of `receive_messages()`
    /// - Held for the entire duration of the streaming connection
    /// - Continuously calls `rx_cons.recv()` to pull messages from channel
    /// - Forwards received messages to client via `grpc_tx.send()`
    ///
    /// Flow: `Channel → rx_cons.recv() → gRPC → Client`
    ///
    /// **Why separate lock from `session`?**
    /// - gRPC task holds this lock forever (while streaming)
    /// - Dispatcher needs to check `session.active` frequently
    /// - If both were in same lock → deadlock (gRPC holds, dispatcher waits)
    pub(crate) rx_cons: Arc<Mutex<mpsc::Receiver<StreamMessage>>>,
}

/// Result of a non-blocking send attempt to the consumer's internal channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConsumerSendStatus {
    Sent,
    Full,
    Closed,
}

impl Consumer {
    pub(crate) fn new(
        consumer_id: u64,
        consumer_name: &str,
        subscription_type: i32,
        topic_name: &str,
        subscription_name: &str,
        tx_cons: mpsc::Sender<StreamMessage>,
        session: Arc<Mutex<ConsumerSession>>,
        rx_cons: Arc<Mutex<mpsc::Receiver<StreamMessage>>>,
    ) -> Self {
        let active = session
            .try_lock()
            .expect("session mutex should not be held during consumer construction")
            .active
            .clone();

        Consumer {
            consumer_id: consumer_id.into(),
            consumer_name: consumer_name.into(),
            subscription_type,
            topic_name: topic_name.into(),
            subscription_name: subscription_name.into(),
            tx_cons,
            session,
            active,
            rx_cons,
        }
    }

    /// Blocking send path used by reliable dispatchers.
    ///
    /// This method awaits channel capacity and returns an error if the consumer
    /// channel has been closed.
    pub(crate) async fn send_message(&mut self, message: StreamMessage) -> Result<()> {
        // Since u8 is exactly 1 byte, the size in bytes will be equal to the number of elements in the vector.
        let payload_size = message.payload.len();
        // Send the message to the other channel
        if let Err(err) = self.tx_cons.send(message).await {
            // Log the error and handle the channel closure scenario
            warn!(
                consumer_id = %self.consumer_id,
                subscription = %self.subscription_name,
                topic = %self.topic_name,
                error = ?err,
                "failed to send message to consumer"
            );
            return Err(anyhow!("failed to send message to consumer: {}", err));
        } else {
            trace!(consumer_id = %self.consumer_id, "sending the message over channel to consumer");
            counter!(CONSUMER_MESSAGES_OUT_TOTAL.name, "topic"=> self.topic_name.clone() , "subscription" => self.subscription_name.clone()).increment(1);
            counter!(CONSUMER_BYTES_OUT_TOTAL.name, "topic"=> self.topic_name.clone() , "subscription" => self.subscription_name.clone()).increment(payload_size as u64);
        }

        // info!("Consumer task ended for consumer_id: {}", self.consumer_id);
        Ok(())
    }

    /// Non-blocking send path used by non-reliable dispatchers.
    ///
    /// This method never awaits channel capacity:
    /// - `Sent`: message was enqueued
    /// - `Full`: channel is saturated; caller can drop or try another consumer
    /// - `Closed`: receiver is gone; caller should treat the consumer as unavailable
    pub(crate) fn try_send_message(&mut self, message: StreamMessage) -> ConsumerSendStatus {
        let payload_size = message.payload.len();

        match self.tx_cons.try_send(message) {
            Ok(()) => {
                trace!(consumer_id = %self.consumer_id, "sending the message over channel to consumer");
                counter!(CONSUMER_MESSAGES_OUT_TOTAL.name, "topic"=> self.topic_name.clone() , "subscription" => self.subscription_name.clone()).increment(1);
                counter!(CONSUMER_BYTES_OUT_TOTAL.name, "topic"=> self.topic_name.clone() , "subscription" => self.subscription_name.clone()).increment(payload_size as u64);
                ConsumerSendStatus::Sent
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    consumer_id = %self.consumer_id,
                    subscription = %self.subscription_name,
                    topic = %self.topic_name,
                    "consumer channel full; dropping non-reliable message"
                );
                ConsumerSendStatus::Full
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!(
                    consumer_id = %self.consumer_id,
                    subscription = %self.subscription_name,
                    topic = %self.topic_name,
                    "failed to send message to consumer"
                );
                ConsumerSendStatus::Closed
            }
        }
    }

    /// Get the current active status of this consumer
    pub(crate) async fn get_status(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Set the consumer status to active
    pub(crate) async fn set_status_active(&self) {
        self.active.store(true, Ordering::Release);
    }

    /// Set the consumer status to inactive
    pub(crate) async fn set_status_inactive(&self) {
        self.active.store(false, Ordering::Release);
    }
}

/// Represents the session state for a consumer connection.
///
/// # Purpose
///
/// Tracks the lifecycle of a single consumer connection session. When a consumer
/// reconnects (takeover), a new session is created with a new `session_id` and
/// `cancellation` token, but the same `Consumer` struct is reused.
///
/// # Why Separate from Consumer?
///
/// Session state changes frequently (on connect/disconnect/takeover) while the
/// consumer identity (consumer_id, topic_name, etc.) remains constant.
///
/// # Lock Contention Strategy
///
/// This struct is kept separate from `rx_cons` to avoid deadlock:
/// - **Dispatcher**: Needs quick, frequent access to `active` status
/// - **gRPC handler**: Holds `rx_cons` lock for entire streaming duration
///
/// If both were in the same lock, the dispatcher would block forever waiting
/// for the gRPC task to release the lock (which it never does while streaming).
///
/// # Fields
///
#[derive(Debug)]
pub(crate) struct ConsumerSession {
    /// Unique ID for this session (changes on reconnect/takeover).
    ///
    /// Each time a consumer reconnects, `takeover()` generates a new session_id.
    /// This helps track and debug connection lifecycle in logs.
    pub(crate) session_id: u64,

    /// Whether this consumer is currently active and able to receive messages.
    ///
    /// **State transitions**:
    /// - `true`: Consumer is connected and streaming messages
    /// - `false`: Consumer disconnected or inactive
    ///
    /// **Updated by**:
    /// - `new()`: Sets to `true` (new consumer starts active)
    /// - `takeover()`: Sets to `true` (reconnection activates consumer)
    /// - `set_status_inactive()`: Sets to `false` (on disconnect)
    ///
    /// **Read by**:
    /// - Dispatcher: Checks before sending messages (skip inactive consumers)
    pub(crate) active: Arc<AtomicBool>,

    /// Cancellation token for the gRPC streaming task.
    ///
    /// **Purpose**: Signals the streaming task to stop when:
    /// - Consumer disconnects (normal shutdown)
    /// - New connection arrives (takeover - cancel old task)
    ///
    /// **Lifecycle**:
    /// - Created fresh on each `new()` or `takeover()`
    /// - Cancelled via `cancel_stream()` or `takeover()`
    /// - Streaming task monitors via `token.cancelled().await`
    pub(crate) cancellation: CancellationToken,
}

impl ConsumerSession {
    /// Create a new session
    pub(crate) fn new() -> Self {
        Self {
            session_id: get_random_id(),
            active: Arc::new(AtomicBool::new(true)),
            cancellation: CancellationToken::new(),
        }
    }

    /// Takeover: cancel the current session and start a new one.
    /// Returns the new cancellation token for the streaming task.
    pub(crate) fn takeover(&mut self) -> CancellationToken {
        // Cancel the existing streaming task
        self.cancellation.cancel();

        // Create new session
        self.session_id = get_random_id();
        self.active.store(true, Ordering::Release);
        self.cancellation = CancellationToken::new();

        debug!(
            session_id = %self.session_id,
            "consumer session takeover"
        );
        self.cancellation.clone()
    }

    /// Mark this session as inactive (called on disconnect)
    #[allow(dead_code)]
    pub(crate) fn disconnect(&mut self) {
        self.active.store(false, Ordering::Release);
        debug!(
            session_id = %self.session_id,
            "consumer session disconnected"
        );
    }

    /// Cancel the current streaming task without changing active status
    pub(crate) fn cancel_stream(&self) {
        self.cancellation.cancel();
    }
}
