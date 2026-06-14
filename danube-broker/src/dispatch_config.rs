use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// Cluster-wide dispatch defaults loaded from the broker YAML config.
///
/// These defaults are applied when creating new subscriptions. Admins can
/// override `max_unacked_messages` per-subscription via the admin API.
///
/// ```yaml
/// dispatch:
///   max_unacked_messages: 10
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DispatchConfig {
    /// Maximum number of unacknowledged messages in the dispatch window.
    /// Controls pipelining depth: higher values increase throughput at the cost
    /// of memory and redelivery scope on failure.
    /// Setting to 1 reproduces single-slot stop-and-wait behavior.
    pub max_unacked_messages: usize,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            max_unacked_messages: 10,
        }
    }
}

impl DispatchConfig {
    pub fn validate(&self) -> Result<()> {
        if self.max_unacked_messages < 1 || self.max_unacked_messages > 10_000 {
            return Err(anyhow!(
                "dispatch.max_unacked_messages must be between 1 and 10000, got {}",
                self.max_unacked_messages
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_dispatch_config() {
        let cfg = DispatchConfig::default();
        assert_eq!(cfg.max_unacked_messages, 10);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validates_bounds() {
        let mut cfg = DispatchConfig::default();

        cfg.max_unacked_messages = 0;
        assert!(cfg.validate().is_err());

        cfg.max_unacked_messages = 10_001;
        assert!(cfg.validate().is_err());

        cfg.max_unacked_messages = 1;
        assert!(cfg.validate().is_ok());

        cfg.max_unacked_messages = 10_000;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn deserializes_from_yaml() {
        let yaml = "max_unacked_messages: 50";
        let cfg: DispatchConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.max_unacked_messages, 50);
    }

    #[test]
    fn deserializes_empty_yaml_uses_defaults() {
        let yaml = "{}";
        let cfg: DispatchConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.max_unacked_messages, 10);
    }
}
