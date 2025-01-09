use crate::proto::{ReliableOptions as ProtoReliableOptions, TopicDispatchStrategy};
use serde::{Deserialize, Serialize};

/// Dispatch strategy for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigDispatchStrategy {
    /// Non-reliable dispatch strategy.It means that messages are not guaranteed to be delivered.
    NonReliable,
    /// Reliable dispatch strategy.It means that messages are guaranteed to be delivered.
    Reliable(ReliableOptions),
}

/// Reliable dispatch strategy options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReliableOptions {
    /// Segment size in bytes.
    pub segment_size: usize,
    /// Retention policy for messages in the topic.Could be retain until ack or expire.
    pub retention_policy: RetentionPolicy,
    /// Retention period in seconds.
    pub retention_period: u64,
}

impl ReliableOptions {
    pub fn new(
        segment_size: usize,
        retention_policy: RetentionPolicy,
        retention_period: u64,
    ) -> Self {
        ReliableOptions {
            segment_size,
            retention_policy,
            retention_period,
        }
    }
}

/// Retention policy for messages in the topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetentionPolicy {
    RetainUntilAck,
    RetainUntilExpire,
}

impl Default for ConfigDispatchStrategy {
    fn default() -> Self {
        ConfigDispatchStrategy::NonReliable
    }
}

// Implement conversions from TopicDispatchStrategy to ConfigDispatchStrategy
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

// Implement conversions from ConfigDispatchStrategy to TopicDispatchStrategy
impl From<ConfigDispatchStrategy> for TopicDispatchStrategy {
    fn from(config: ConfigDispatchStrategy) -> Self {
        match config {
            ConfigDispatchStrategy::NonReliable => TopicDispatchStrategy {
                strategy: 0,
                reliable_options: None,
            },
            ConfigDispatchStrategy::Reliable(opts) => {
                let retention_policy = match opts.retention_policy {
                    RetentionPolicy::RetainUntilAck => 0,
                    RetentionPolicy::RetainUntilExpire => 1,
                };

                TopicDispatchStrategy {
                    strategy: 1,
                    reliable_options: Some(ProtoReliableOptions {
                        segment_size: opts.segment_size as u64,
                        retention_policy,
                        retention_period: opts.retention_period,
                    }),
                }
            }
        }
    }
}
