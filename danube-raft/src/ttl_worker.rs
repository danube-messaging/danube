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

            let (has_expired_entries, expired_keys) = {
                let d = data.read().await;
                let mut has_entries = false;
                let mut keys = Vec::new();
                for (_ts, ks) in d.ttl_entries.range(..=now_ms) {
                    has_entries = true;
                    // Skip keys that were since rewritten without a TTL (plain `put`):
                    // their old TTL index entry is stale and must not delete them.
                    keys.extend(
                        ks.iter()
                            .filter(|k| d.kv.get(*k).is_some_and(|v| v.ttl_ms.is_some()))
                            .cloned(),
                    );
                }
                (has_entries, keys)
            };

            // Still propose when only stale entries expired (empty key list):
            // applying the command clears the expired TTL index entries.
            if !has_expired_entries {
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
