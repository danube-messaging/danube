use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Dispatch strategy for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

// Note: conversions to/from proto TopicDispatchStrategy were removed because
// the Producer API now uses enum-only DispatchStrategy. Admin-side per-topic
// overrides (Phase 2) will define their own conversion paths as needed.
