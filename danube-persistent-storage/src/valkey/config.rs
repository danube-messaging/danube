/// Configuration for the external write buffer (Valkey/Redis).
///
/// Controls how `BufferedStorage` replicates WAL writes to an external
/// key-value store for cross-node durability.

/// What to do when the Valkey `WAIT` command times out (replicas are slow/partitioned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitTimeoutPolicy {
    /// ACK the producer despite incomplete replication.
    /// Higher availability, but risk of data loss if the broker crashes
    /// before the replica catches up.
    Ack,
    /// Return an error to the producer, forcing a retry.
    /// Stronger durability guarantee at the cost of reduced availability.
    Fail,
}

impl Default for WaitTimeoutPolicy {
    fn default() -> Self {
        Self::Fail
    }
}

impl WaitTimeoutPolicy {
    pub fn from_str_lossy(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "ack" => Self::Ack,
            _ => Self::Fail,
        }
    }
}

/// Configuration for the write buffer backed by Valkey/Redis.
#[derive(Debug, Clone)]
pub struct WriteBufferConfig {
    /// Valkey/Redis endpoints (e.g., `["redis://127.0.0.1:6379"]`).
    pub endpoints: Vec<String>,
    /// Number of replicas that must confirm via `WAIT` before ACKing.
    pub wait_replicas: u32,
    /// Timeout in milliseconds for the `WAIT` command.
    pub wait_timeout_ms: u64,
    /// What to do if `WAIT` times out.
    pub on_wait_timeout: WaitTimeoutPolicy,
    /// Maximum number of closed segments to keep cached in Valkey.
    pub max_cached_closed_segments: u32,
}

impl Default for WriteBufferConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["redis://127.0.0.1:6379".to_string()],
            wait_replicas: 1,
            wait_timeout_ms: 100,
            on_wait_timeout: WaitTimeoutPolicy::default(),
            max_cached_closed_segments: 5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_timeout_policy_from_str_ack() {
        assert_eq!(WaitTimeoutPolicy::from_str_lossy("ack"), WaitTimeoutPolicy::Ack);
        assert_eq!(WaitTimeoutPolicy::from_str_lossy("ACK"), WaitTimeoutPolicy::Ack);
        assert_eq!(WaitTimeoutPolicy::from_str_lossy("Ack"), WaitTimeoutPolicy::Ack);
    }

    #[test]
    fn wait_timeout_policy_from_str_fail() {
        assert_eq!(WaitTimeoutPolicy::from_str_lossy("fail"), WaitTimeoutPolicy::Fail);
        assert_eq!(WaitTimeoutPolicy::from_str_lossy("FAIL"), WaitTimeoutPolicy::Fail);
    }

    #[test]
    fn wait_timeout_policy_from_str_unknown_defaults_to_fail() {
        assert_eq!(WaitTimeoutPolicy::from_str_lossy("unknown"), WaitTimeoutPolicy::Fail);
        assert_eq!(WaitTimeoutPolicy::from_str_lossy(""), WaitTimeoutPolicy::Fail);
        assert_eq!(WaitTimeoutPolicy::from_str_lossy("retry"), WaitTimeoutPolicy::Fail);
    }

    #[test]
    fn wait_timeout_policy_default_is_fail() {
        assert_eq!(WaitTimeoutPolicy::default(), WaitTimeoutPolicy::Fail);
    }

    #[test]
    fn write_buffer_config_defaults() {
        let cfg = WriteBufferConfig::default();
        assert_eq!(cfg.endpoints, vec!["redis://127.0.0.1:6379"]);
        assert_eq!(cfg.wait_replicas, 1);
        assert_eq!(cfg.wait_timeout_ms, 100);
        assert_eq!(cfg.on_wait_timeout, WaitTimeoutPolicy::Fail);
        assert_eq!(cfg.max_cached_closed_segments, 5);
    }
}
