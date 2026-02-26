//! Lightweight handle for querying Raft leadership status.
//!
//! Consumers (e.g. the broker) use this instead of depending on `openraft` directly.

use openraft::Raft;

use crate::typ::TypeConfig;

/// A cloneable, cheaply-copyable handle that answers "is this node the Raft leader?".
#[derive(Clone)]
pub struct LeadershipHandle {
    raft: Raft<TypeConfig>,
    node_id: u64,
}

impl LeadershipHandle {
    pub fn new(raft: Raft<TypeConfig>, node_id: u64) -> Self {
        Self { raft, node_id }
    }

    /// Returns `true` if this node is currently the Raft leader.
    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Returns the node ID of the current Raft leader, if one exists.
    pub fn current_leader(&self) -> Option<u64> {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader
    }

    /// This node's ID.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }
}

impl std::fmt::Debug for LeadershipHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeadershipHandle")
            .field("node_id", &self.node_id)
            .field("is_leader", &self.is_leader())
            .finish()
    }
}
