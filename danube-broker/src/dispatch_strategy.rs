use danube_client::{ConfigDispatchStrategy, ReliableOptions, RetentionPolicy};
use danube_reliable_dispatch::ReliableDispatch;

use crate::proto::TopicDispatchStrategy;

#[derive(Debug)]
pub(crate) enum DispatchStrategy {
    // Does not store messages, sends them directly to the dispatcher
    NonReliable,
    // Stores messages for reliable delivery
    Reliable(ReliableDispatch),
}

impl From<TopicDispatchStrategy> for ConfigDispatchStrategy {
    fn from(strategy: TopicDispatchStrategy) -> Self {
        match strategy.strategy {
            0 => ConfigDispatchStrategy::NonReliable,
            1 => {
                if let Some(reliable_opts) = strategy.reliable_options {
                    let retention_policy = match reliable_opts.retention_policy {
                        0 => RetentionPolicy::RetainUntilAck,
                        1 => RetentionPolicy::RetainUntilExpire,
                        _ => RetentionPolicy::RetainUntilAck,
                    };

                    ConfigDispatchStrategy::Reliable(ReliableOptions {
                        segment_size: reliable_opts.segment_size as usize,
                        retention_policy,
                        retention_period: reliable_opts.retention_period,
                    })
                } else {
                    ConfigDispatchStrategy::NonReliable
                }
            }
            _ => ConfigDispatchStrategy::NonReliable,
        }
    }
}
