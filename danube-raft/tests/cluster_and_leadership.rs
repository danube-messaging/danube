//! # Cluster Bootstrap & Leadership Tests
//!
//! Validates the foundational Raft cluster lifecycle:
//!
//! - A single-node cluster can be started and bootstrapped.
//! - After bootstrap, the node becomes the Raft leader.
//! - The [`LeadershipHandle`] correctly reports leadership status.
//! - The leader can publish its identity to the metadata store so that
//!   other broker components (e.g. `ClusterResources::get_cluster_leader`)
//!   can discover who the current leader is.

mod common;

use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_raft::leadership::LeadershipHandle;
use serde_json::Value;

/// **What**: Verify that a single-node Raft cluster elects itself as leader
/// immediately after bootstrap.
///
/// **Why**: This is the very first thing that happens when a broker starts.
/// `RaftNode::start` + `init_cluster` must result in a node that considers
/// itself the leader, otherwise no writes can be proposed.
///
/// **Checks**:
/// - `LeadershipHandle::is_leader()` returns `true`
/// - `LeadershipHandle::current_leader()` returns `Some(node_id)`
/// - `LeadershipHandle::node_id()` matches the configured ID
#[tokio::test]
async fn single_node_becomes_leader_after_bootstrap() {
    let (node, _tmp) = common::start_cluster().await;
    let handle: LeadershipHandle = node.leadership_handle();

    assert!(handle.is_leader(), "single-node should be leader");
    assert_eq!(handle.current_leader(), Some(node.node_id));
    assert_eq!(handle.node_id(), node.node_id);
}

/// **What**: Verify the Raft-based leader election publishing pattern used
/// by the broker's `LeaderElection` service.
///
/// **Why**: After the old ETCD-based leader election was removed, the broker's
/// `LeaderElection::check_leader` publishes the leader's broker ID to the
/// metadata store at `/cluster/leader`. Other components like
/// `ClusterResources::get_cluster_leader` read this key from local cache.
/// This test ensures the full round-trip works through Raft consensus.
///
/// **Checks**:
/// - `LeadershipHandle::is_leader()` confirms leadership before publishing
/// - `put` of the leader broker ID succeeds through Raft consensus
/// - `get` reads back the correct broker ID
#[tokio::test]
async fn leader_publishes_id_to_metadata_store() {
    let (node, _tmp) = common::start_cluster().await;
    let store = &node.store;
    let handle = node.leadership_handle();

    assert!(handle.is_leader());

    // Publish leader ID (like LeaderElection::check_leader does).
    let broker_id: u64 = node.node_id;
    let payload = Value::Number(serde_json::Number::from(broker_id));
    store
        .put("/cluster/leader", payload, MetaOptions::None)
        .await
        .unwrap();

    // Another component reads the leader (like ClusterResources::get_cluster_leader).
    let val = store
        .get("/cluster/leader", MetaOptions::None)
        .await
        .unwrap()
        .expect("leader key should exist");
    assert_eq!(val.as_u64(), Some(broker_id));
}
