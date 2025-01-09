use danube_core::dispatch_strategy::{ReliableOptions, RetentionPolicy};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigRetentionPolicy {
    RetainUntilExpire,
    RetainUntilAck,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigReliableOptions {
    pub segment_size: u64,
    pub retention_policy: ConfigRetentionPolicy,
    pub retention_time: u64,
}

impl ConfigReliableOptions {
    pub fn new(
        segment_size: u64,
        retention_policy: ConfigRetentionPolicy,
        retention_time: u64,
    ) -> Self {
        ConfigReliableOptions {
            segment_size,
            retention_policy,
            retention_time,
        }
    }
}

impl From<ConfigReliableOptions> for ReliableOptions {
    fn from(config: ConfigReliableOptions) -> Self {
        let retention_policy = match config.retention_policy {
            ConfigRetentionPolicy::RetainUntilExpire => RetentionPolicy::RetainUntilExpire,
            ConfigRetentionPolicy::RetainUntilAck => RetentionPolicy::RetainUntilAck,
        };

        ReliableOptions {
            segment_size: config.segment_size as usize,
            retention_policy,
            retention_period: config.retention_time,
        }
    }
}
