use anyhow::Result;
use danube_core::message::StreamMessage;

use crate::{consumer::Consumer, message::AckMessage};

// Module declarations
pub(crate) mod commands;
pub(crate) mod exclusive;
pub(crate) mod shared;
pub(crate) mod subscription_engine;

use exclusive::ExclusiveDispatcher;
use shared::SharedDispatcher;

#[derive(Debug)]
pub(crate) enum DispatchStrategy {
    // Does not store messages, sends them directly to the dispatcher
    NonReliable,
    // Stores messages for reliable delivery
    Reliable,
}

/// Main dispatcher enum - routes to specific dispatcher implementations
#[derive(Debug, Clone)]
pub(crate) enum Dispatcher {
    Exclusive(ExclusiveDispatcher),
    Shared(SharedDispatcher),
}

impl Dispatcher {
    pub(crate) async fn dispatch_message(&self, message: StreamMessage) -> Result<()> {
        match self {
            Dispatcher::Exclusive(d) => d.dispatch_message(message).await,
            Dispatcher::Shared(d) => d.dispatch_message(message).await,
        }
    }

    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        match self {
            Dispatcher::Exclusive(d) => d.ack_message(ack_msg).await,
            Dispatcher::Shared(d) => d.ack_message(ack_msg).await,
        }
    }

    pub(crate) async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        match self {
            Dispatcher::Exclusive(d) => d.add_consumer(consumer).await,
            Dispatcher::Shared(d) => d.add_consumer(consumer).await,
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn remove_consumer(&self, consumer_id: u64) -> Result<()> {
        match self {
            Dispatcher::Exclusive(d) => d.remove_consumer(consumer_id).await,
            Dispatcher::Shared(d) => d.remove_consumer(consumer_id).await,
        }
    }

    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        match self {
            Dispatcher::Exclusive(d) => d.disconnect_all_consumers().await,
            Dispatcher::Shared(d) => d.disconnect_all_consumers().await,
        }
    }

    pub(crate) async fn reset_pending(&self) -> Result<()> {
        match self {
            Dispatcher::Exclusive(d) => d.reset_pending().await,
            Dispatcher::Shared(d) => d.reset_pending().await,
        }
    }

    /// Force flush durable subscription progress for reliable dispatchers. No-op for non-reliable.
    pub(crate) async fn flush_progress_now(&self) -> Result<()> {
        match self {
            Dispatcher::Exclusive(d) => d.flush_progress_now().await,
            Dispatcher::Shared(d) => d.flush_progress_now().await,
        }
    }
}
