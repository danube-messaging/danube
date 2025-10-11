use crate::proto::DispatchStrategy as ProtoDispatchStrategy;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Dispatch strategy for a topic.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConfigDispatchStrategy {
    /// Non-reliable dispatch strategy.It means that messages are not guaranteed to be delivered.
    NonReliable,
    /// Reliable dispatch strategy.It means that messages are guaranteed to be delivered.
    Reliable,
}

impl Default for ConfigDispatchStrategy {
    fn default() -> Self {
        ConfigDispatchStrategy::NonReliable
    }
}

impl Display for ConfigDispatchStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigDispatchStrategy::NonReliable => write!(f, "Non-Reliable"),
            ConfigDispatchStrategy::Reliable => write!(f, "Reliable"),
        }
    }
}

impl From<ProtoDispatchStrategy> for ConfigDispatchStrategy {
    fn from(s: ProtoDispatchStrategy) -> Self {
        match s {
            ProtoDispatchStrategy::NonReliable => ConfigDispatchStrategy::NonReliable,
            ProtoDispatchStrategy::Reliable => ConfigDispatchStrategy::Reliable,
        }
    }
}
