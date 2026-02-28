//! Shared test helpers for `danube-raft` integration tests.
//!
//! Provides a [`start_cluster`] function that spins up an ephemeral single-node
//! Raft cluster in a temp directory, bootstraps it, and waits for leader election.
//! Each test gets its own isolated cluster on a unique port.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use danube_raft::node::{RaftNode, RaftNodeConfig};
use tempfile::TempDir;

/// Global port counter so parallel tests don't collide.
static PORT: AtomicU16 = AtomicU16::new(17650);

fn next_port() -> u16 {
    PORT.fetch_add(1, Ordering::Relaxed)
}

/// Spin up a bootstrapped single-node Raft cluster and return the node + temp dir.
///
/// - The TTL worker interval is set to 200 ms so TTL-related tests resolve quickly.
/// - The returned `TempDir` must be kept alive for the duration of the test;
///   dropping it removes the on-disk Raft log store.
pub async fn start_cluster() -> (RaftNode, TempDir) {
    let tmp = TempDir::new().expect("create temp dir");
    let port = next_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let node = RaftNode::start(RaftNodeConfig {
        data_dir: tmp.path().to_path_buf(),
        raft_addr: addr,
        advertised_addr: None,
        ttl_check_interval: Duration::from_millis(200),
    })
    .await
    .expect("start raft node");

    node.init_cluster(&addr.to_string())
        .await
        .expect("bootstrap cluster");

    // Give the node a moment to elect itself leader.
    tokio::time::sleep(Duration::from_millis(300)).await;

    (node, tmp)
}
