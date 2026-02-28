//! Background task that periodically checks for expired TTL keys
//! and proposes their deletion through Raft consensus.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openraft::Raft;
use tracing::{debug, warn};

use crate::commands::RaftCommand;
use crate::state_machine::SharedStateMachineData;
use crate::typ::TypeConfig;

/// Spawn the TTL expiration worker.
///
/// Only the Raft **leader** should propose `ExpireTTLKeys` commands.
/// Followers skip the tick. This avoids duplicate proposals.
pub fn spawn_ttl_worker(
    raft: Raft<TypeConfig>,
    data: SharedStateMachineData,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;

            // Only the leader should propose expirations.
            let metrics = raft.metrics().borrow().clone();
            let is_leader = metrics.current_leader == Some(metrics.id);
            if !is_leader {
                continue;
            }

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            let expired_keys = {
                let d = data.read().await;
                let mut keys = Vec::new();
                for (_ts, ks) in d.ttl_entries.range(..=now_ms) {
                    keys.extend(ks.iter().cloned());
                }
                keys
            };

            if expired_keys.is_empty() {
                continue;
            }

            debug!(count = expired_keys.len(), "expiring TTL keys");

            if let Err(e) = raft
                .client_write(RaftCommand::ExpireTTLKeys { keys: expired_keys })
                .await
            {
                warn!(?e, "failed to propose TTL expiration");
            }
        }
    })
}
