use anyhow::Result;
use danube_core::message::StreamMessage;

use crate::{consumer::Consumer, message::AckMessage};

// Legacy non-unified dispatchers removed from enum; modules kept only if referenced elsewhere
// pub(crate) mod dispatcher_multiple_consumers;
// pub(crate) mod dispatcher_single_consumer;
// Unified dispatcher scaffolding (Phase 1)
pub(crate) mod subscription_engine;
pub(crate) mod unified_multiple;
pub(crate) mod unified_single;
use unified_multiple::UnifiedMultipleDispatcher;
use unified_single::UnifiedSingleDispatcher;

// The dispatchers ensure that messages are routed to consumers according to the semantics of the subscription type
#[derive(Debug)]
pub(crate) enum Dispatcher {
    UnifiedOneConsumer(UnifiedSingleDispatcher),
    UnifiedMultipleConsumers(UnifiedMultipleDispatcher),
}

impl Dispatcher {
    pub(crate) async fn dispatch_message(&self, message: StreamMessage) -> Result<()> {
        match self {
            Dispatcher::UnifiedOneConsumer(dispatcher) => {
                Ok(dispatcher.dispatch_message(message).await?)
            }
            Dispatcher::UnifiedMultipleConsumers(dispatcher) => {
                Ok(dispatcher.dispatch_message(message).await?)
            }
        }
    }
    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        match self {
            Dispatcher::UnifiedOneConsumer(dispatcher) => Ok(dispatcher
                .ack_message(ack_msg.request_id, ack_msg.msg_id)
                .await?),
            Dispatcher::UnifiedMultipleConsumers(dispatcher) => Ok(dispatcher
                .ack_message(ack_msg.request_id, ack_msg.msg_id)
                .await?),
        }
    }
    pub(crate) async fn add_consumer(&mut self, consumer: Consumer) -> Result<()> {
        match self {
            Dispatcher::UnifiedOneConsumer(dispatcher) => {
                Ok(dispatcher.add_consumer(consumer).await?)
            }
            Dispatcher::UnifiedMultipleConsumers(dispatcher) => {
                Ok(dispatcher.add_consumer(consumer).await?)
            }
        }
    }
    #[allow(dead_code)]
    pub(crate) async fn remove_consumer(&mut self, consumer_id: u64) -> Result<()> {
        match self {
            Dispatcher::UnifiedOneConsumer(dispatcher) => {
                Ok(dispatcher.remove_consumer(consumer_id).await?)
            }
            Dispatcher::UnifiedMultipleConsumers(dispatcher) => {
                Ok(dispatcher.remove_consumer(consumer_id).await?)
            }
        }
    }

    pub(crate) async fn disconnect_all_consumers(&mut self) -> Result<()> {
        match self {
            Dispatcher::UnifiedOneConsumer(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
            }
            Dispatcher::UnifiedMultipleConsumers(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
            }
        }
    }

    pub(crate) async fn reset_pending(&self) -> Result<()> {
        match self {
            Dispatcher::UnifiedOneConsumer(dispatcher) => {
                Ok(dispatcher.reset_pending().await?)
            }
            Dispatcher::UnifiedMultipleConsumers(dispatcher) => {
                Ok(dispatcher.reset_pending().await?)
            }
        }
    }
}
