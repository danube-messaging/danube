use anyhow::Result;
use danube_core::message::{MessageID, StreamMessage};

use crate::{consumer::Consumer, message::AckMessage};

pub(crate) mod dispatcher_multiple_consumers;
pub(crate) mod dispatcher_reliable_multiple_consumers;
pub(crate) mod dispatcher_reliable_single_consumer;
pub(crate) mod dispatcher_single_consumer;
pub(crate) use dispatcher_multiple_consumers::DispatcherMultipleConsumers;
pub(crate) use dispatcher_reliable_multiple_consumers::DispatcherReliableMultipleConsumers;

pub(crate) use dispatcher_reliable_single_consumer::DispatcherReliableSingleConsumer;
pub(crate) use dispatcher_single_consumer::DispatcherSingleConsumer;

// The dispatchers ensure that messages are routed to consumers according to the semantics of the subscription type
#[derive(Debug)]
pub(crate) enum Dispatcher {
    OneConsumer(DispatcherSingleConsumer),
    ReliableOneConsumer(DispatcherReliableSingleConsumer),
    MultipleConsumers(DispatcherMultipleConsumers),
    ReliableMultipleConsumers(DispatcherReliableMultipleConsumers),
}

// Control messages for the dispatcher
enum DispatcherCommand {
    AddConsumer(Consumer),
    RemoveConsumer(u64),
    DisconnectAllConsumers,
    DispatchMessage(StreamMessage),
    MessageAcked(u64, MessageID),
}

impl Dispatcher {
    pub(crate) async fn dispatch_message(&self, message: StreamMessage) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.dispatch_message(message).await?),
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.dispatch_message(message).await?)
            }
            Dispatcher::ReliableOneConsumer(_) => {
                unreachable!(
                    "Reliable dispatchers do not receive single messsages, rather segments"
                )
            }
            Dispatcher::ReliableMultipleConsumers(_) => {
                unreachable!(
                    "Reliable dispatchers do not receive single messsages, rather segments"
                )
            }
        }
    }
    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.ack_message(ack_msg).await?),
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.ack_message(ack_msg).await?)
            }
            Dispatcher::ReliableOneConsumer(dispatcher) => {
                Ok(dispatcher.ack_message(ack_msg).await?)
            }
            Dispatcher::ReliableMultipleConsumers(dispatcher) => {
                Ok(dispatcher.ack_message(ack_msg).await?)
            }
        }
    }
    pub(crate) async fn add_consumer(&mut self, consumer: Consumer) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.add_consumer(consumer).await?),
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.add_consumer(consumer).await?)
            }
            Dispatcher::ReliableOneConsumer(dispatcher) => {
                Ok(dispatcher.add_consumer(consumer).await?)
            }
            Dispatcher::ReliableMultipleConsumers(dispatcher) => {
                Ok(dispatcher.add_consumer(consumer).await?)
            }
        }
    }
    pub(crate) async fn remove_consumer(&mut self, consumer_id: u64) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => {
                Ok(dispatcher.remove_consumer(consumer_id).await?)
            }
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.remove_consumer(consumer_id).await?)
            }
            Dispatcher::ReliableOneConsumer(dispatcher) => {
                Ok(dispatcher.remove_consumer(consumer_id).await?)
            }
            Dispatcher::ReliableMultipleConsumers(dispatcher) => {
                Ok(dispatcher.remove_consumer(consumer_id).await?)
            }
        }
    }

    pub(crate) async fn disconnect_all_consumers(&mut self) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
            }
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
            }
            Dispatcher::ReliableOneConsumer(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
            }
            Dispatcher::ReliableMultipleConsumers(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
            }
        }
    }
}
