use crate::broker_metrics::LEADER_ELECTION_STATE;
use crate::metadata_storage::MetadataStorage;
use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_raft::leadership::LeadershipHandle;
use metrics::gauge;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Interval;
use tracing::{debug, warn};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LeaderElectionState {
    NoLeader,
    Leading,
    Following,
}

// Leader Election based on Raft consensus.
// The Raft leader is automatically the cluster leader.
// The leader ID is published to the metadata store for discoverability by other services.
#[derive(Clone)]
pub(crate) struct LeaderElection {
    broker_id: u64,
    handle: Option<LeadershipHandle>,
    store: Option<MetadataStorage>,
    path: String,
    state: Arc<Mutex<LeaderElectionState>>,
}

impl std::fmt::Debug for LeaderElection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderElection")
            .field("broker_id", &self.broker_id)
            .field("has_raft", &self.handle.is_some())
            .finish()
    }
}

impl LeaderElection {
    /// Create a Raft-backed leader election service.
    pub fn new(
        handle: LeadershipHandle,
        store: MetadataStorage,
        path: &str,
        broker_id: u64,
    ) -> Self {
        Self {
            broker_id,
            handle: Some(handle),
            store: Some(store),
            path: path.to_owned(),
            state: Arc::new(Mutex::new(LeaderElectionState::NoLeader)),
        }
    }

    /// Create a standalone instance for unit tests (always NoLeader).
    #[cfg(test)]
    pub fn new_standalone(broker_id: u64) -> Self {
        Self {
            broker_id,
            handle: None,
            store: None,
            path: String::new(),
            state: Arc::new(Mutex::new(LeaderElectionState::NoLeader)),
        }
    }

    pub async fn start(&mut self, mut leader_check_interval: Interval) {
        loop {
            self.check_leader().await;
            leader_check_interval.tick().await;
        }
    }

    pub async fn get_state(&self) -> LeaderElectionState {
        let state = self.state.lock().await;
        state.clone()
    }

    async fn set_state(&self, new_state: LeaderElectionState) {
        let mut state = self.state.lock().await;
        if *state != new_state {
            *state = new_state;
            // 0 = Following/NoLeader, 1 = Leading
            let value = match *state {
                LeaderElectionState::Leading => 1.0,
                _ => 0.0,
            };
            gauge!(LEADER_ELECTION_STATE.name).set(value);
        }
    }

    async fn check_leader(&mut self) {
        let handle = match &self.handle {
            Some(h) => h,
            None => return, // standalone/test mode — keep current state
        };

        if handle.is_leader() {
            self.set_state(LeaderElectionState::Leading).await;
            debug!(broker_id = %self.broker_id, "broker is the Raft leader");

            // Publish leader ID to metadata store for discoverability
            if let Some(ref store) = self.store {
                let payload = serde_json::Value::Number(serde_json::Number::from(self.broker_id));
                if let Err(e) = store.put(&self.path, payload, MetaOptions::None).await {
                    warn!(broker_id = %self.broker_id, error = %e, "failed to publish leader ID");
                }
            }
        } else if handle.current_leader().is_some() {
            self.set_state(LeaderElectionState::Following).await;
            debug!(broker_id = %self.broker_id, "broker is a Raft follower");
        } else {
            self.set_state(LeaderElectionState::NoLeader).await;
            debug!(broker_id = %self.broker_id, "no Raft leader elected yet");
        }
    }
}
