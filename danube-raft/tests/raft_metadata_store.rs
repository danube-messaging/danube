//! Integration tests for `danube-raft` — exercises the `RaftMetadataStore`
//! through the `MetadataStore` trait exactly as the broker does.
//!
//! Each test spins up an ephemeral single-node Raft cluster in a temp directory,
//! bootstraps it, and then drives it through the public API.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use danube_core::metadata::{KeyValueVersion, MetaOptions, MetadataStore, WatchEvent};
use danube_raft::leadership::LeadershipHandle;
use danube_raft::node::{RaftNode, RaftNodeConfig};
use futures::StreamExt;
use serde_json::{json, Value};
use tempfile::TempDir;

// ─── helpers ────────────────────────────────────────────────────────────────

/// Global port counter so parallel tests don't collide.
static PORT: AtomicU16 = AtomicU16::new(17650);

fn next_port() -> u16 {
    PORT.fetch_add(1, Ordering::Relaxed)
}

/// Spin up a bootstrapped single-node Raft cluster and return its store + node.
/// The TTL worker interval is kept short (200 ms) so TTL tests don't wait long.
async fn start_cluster() -> (RaftNode, TempDir) {
    let tmp = TempDir::new().expect("create temp dir");
    let port = next_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let node = RaftNode::start(RaftNodeConfig {
        node_id: 1,
        data_dir: tmp.path().to_path_buf(),
        raft_addr: addr,
        ttl_check_interval: Duration::from_millis(200),
    })
    .await
    .expect("start raft node");

    node.init_cluster(1, &addr.to_string())
        .await
        .expect("bootstrap cluster");

    // Give the node a moment to elect itself leader.
    tokio::time::sleep(Duration::from_millis(300)).await;

    (node, tmp)
}

// ─── 1. Cluster bootstrap & LeadershipHandle ────────────────────────────────

#[tokio::test]
async fn cluster_bootstrap_and_leadership() {
    let (node, _tmp) = start_cluster().await;
    let handle: LeadershipHandle = node.leadership_handle(1);

    assert!(handle.is_leader(), "single-node should be leader");
    assert_eq!(handle.current_leader(), Some(1));
    assert_eq!(handle.node_id(), 1);
}

// ─── 2. Basic CRUD ──────────────────────────────────────────────────────────

#[tokio::test]
async fn put_get_delete() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    // Key doesn't exist yet.
    let val = store
        .get("/topics/my-topic", MetaOptions::None)
        .await
        .unwrap();
    assert!(val.is_none());

    // Put a value.
    store
        .put(
            "/topics/my-topic",
            json!({"partitions": 3}),
            MetaOptions::None,
        )
        .await
        .unwrap();

    // Read it back.
    let val = store
        .get("/topics/my-topic", MetaOptions::None)
        .await
        .unwrap();
    assert_eq!(val, Some(json!({"partitions": 3})));

    // Overwrite.
    store
        .put(
            "/topics/my-topic",
            json!({"partitions": 6}),
            MetaOptions::None,
        )
        .await
        .unwrap();
    let val = store
        .get("/topics/my-topic", MetaOptions::None)
        .await
        .unwrap();
    assert_eq!(val, Some(json!({"partitions": 6})));

    // Delete.
    store.delete("/topics/my-topic").await.unwrap();
    let val = store
        .get("/topics/my-topic", MetaOptions::None)
        .await
        .unwrap();
    assert!(val.is_none());
}

// ─── 3. Prefix operations (like namespace / topic listing) ──────────────────

#[tokio::test]
async fn get_childrens_and_prefix_get() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    // Populate a hierarchy like the broker does for namespaces.
    store
        .put("/namespaces/default", json!("ns"), MetaOptions::None)
        .await
        .unwrap();
    store
        .put("/namespaces/system", json!("ns"), MetaOptions::None)
        .await
        .unwrap();
    store
        .put("/namespaces/prod", json!("ns"), MetaOptions::None)
        .await
        .unwrap();
    // A sibling path that should NOT match the prefix.
    store
        .put("/topics/t1", json!("topic"), MetaOptions::None)
        .await
        .unwrap();

    // get_childrens returns all keys under the prefix.
    let mut children = store.get_childrens("/namespaces/").await.unwrap();
    children.sort();
    assert_eq!(
        children,
        vec![
            "/namespaces/default".to_string(),
            "/namespaces/prod".to_string(),
            "/namespaces/system".to_string(),
        ]
    );

    // get with WithPrefix returns the first match.
    let first = store
        .get("/namespaces/", MetaOptions::WithPrefix)
        .await
        .unwrap();
    assert!(first.is_some(), "should find at least one child");
}

#[tokio::test]
async fn get_bulk_returns_keys_values_and_versions() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    store
        .put("/cluster/brokers/1", json!({"url":"h1"}), MetaOptions::None)
        .await
        .unwrap();
    store
        .put("/cluster/brokers/2", json!({"url":"h2"}), MetaOptions::None)
        .await
        .unwrap();
    store
        .put("/cluster/brokers/3", json!({"url":"h3"}), MetaOptions::None)
        .await
        .unwrap();

    let bulk: Vec<KeyValueVersion> = store.get_bulk("/cluster/brokers/").await.unwrap();
    assert_eq!(bulk.len(), 3);

    // Each entry was written once → version should be 1.
    for kv in &bulk {
        assert_eq!(kv.version, 1);
    }

    // Overwrite broker 2 — its version should bump.
    store
        .put(
            "/cluster/brokers/2",
            json!({"url":"h2-new"}),
            MetaOptions::None,
        )
        .await
        .unwrap();
    let bulk: Vec<KeyValueVersion> = store.get_bulk("/cluster/brokers/").await.unwrap();
    let broker2 = bulk
        .iter()
        .find(|kv| kv.key == "/cluster/brokers/2")
        .unwrap();
    assert_eq!(broker2.version, 2);
}

// ─── 4. Watch events ────────────────────────────────────────────────────────

#[tokio::test]
async fn watch_receives_put_and_delete_events() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    // Subscribe to the prefix BEFORE any writes (like LocalCache does).
    let mut watch = store.watch("/cluster/brokers/").await.unwrap();

    // Write two keys under the watched prefix.
    store
        .put("/cluster/brokers/1", json!(42), MetaOptions::None)
        .await
        .unwrap();
    store
        .put("/cluster/brokers/2", json!(99), MetaOptions::None)
        .await
        .unwrap();

    // Read two Put events.
    let ev1 = tokio::time::timeout(Duration::from_secs(2), watch.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("watch error");
    assert!(
        matches!(ev1, WatchEvent::Put { ref key, .. } if key == b"/cluster/brokers/1"),
        "expected Put for broker 1, got {:?}",
        ev1
    );

    let ev2 = tokio::time::timeout(Duration::from_secs(2), watch.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("watch error");
    assert!(
        matches!(ev2, WatchEvent::Put { ref key, .. } if key == b"/cluster/brokers/2"),
        "expected Put for broker 2, got {:?}",
        ev2
    );

    // Delete one key and verify a Delete event is received.
    store.delete("/cluster/brokers/1").await.unwrap();

    let ev3 = tokio::time::timeout(Duration::from_secs(2), watch.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("watch error");
    assert!(
        matches!(ev3, WatchEvent::Delete { ref key, .. } if key == b"/cluster/brokers/1"),
        "expected Delete for broker 1, got {:?}",
        ev3
    );
}

#[tokio::test]
async fn watch_does_not_receive_unrelated_keys() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    let mut watch = store.watch("/topics/").await.unwrap();

    // Write to a different prefix.
    store
        .put("/namespaces/default", json!("ns"), MetaOptions::None)
        .await
        .unwrap();

    // Write to the watched prefix.
    store
        .put("/topics/t1", json!("data"), MetaOptions::None)
        .await
        .unwrap();

    // First event should be the topics write, not the namespace one.
    let ev = tokio::time::timeout(Duration::from_secs(2), watch.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("watch error");
    assert!(
        matches!(ev, WatchEvent::Put { ref key, .. } if key == b"/topics/t1"),
        "should only receive events for watched prefix, got {:?}",
        ev
    );
}

// ─── 5. TTL key expiration (broker registration pattern) ────────────────────

#[tokio::test]
async fn ttl_key_expires_automatically() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    // Register a broker with a short TTL (like broker_register does).
    store
        .put_with_ttl(
            "/cluster/register/broker-1",
            json!({"broker_url": "http://localhost:6650"}),
            Duration::from_millis(400),
        )
        .await
        .unwrap();

    // Key should be readable immediately.
    let val = store
        .get("/cluster/register/broker-1", MetaOptions::None)
        .await
        .unwrap();
    assert!(val.is_some(), "key should exist right after put_with_ttl");

    // Wait for the TTL worker to expire it.
    // TTL is 400ms, worker checks every 200ms → wait ~800ms to be safe.
    tokio::time::sleep(Duration::from_millis(900)).await;

    let val = store
        .get("/cluster/register/broker-1", MetaOptions::None)
        .await
        .unwrap();
    assert!(val.is_none(), "key should have been expired by TTL worker");
}

#[tokio::test]
async fn ttl_renewal_keeps_key_alive() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    // Initial write with short TTL.
    let key = "/cluster/register/broker-2";
    let payload = json!({"broker_url": "http://localhost:6651"});
    store
        .put_with_ttl(key, payload.clone(), Duration::from_millis(500))
        .await
        .unwrap();

    // Renew before expiry (like the broker's background renewal task).
    tokio::time::sleep(Duration::from_millis(300)).await;
    store
        .put_with_ttl(key, payload.clone(), Duration::from_millis(500))
        .await
        .unwrap();

    // Wait past the original TTL but within the renewed TTL.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let val = store.get(key, MetaOptions::None).await.unwrap();
    assert!(val.is_some(), "key should still be alive after renewal");
}

// ─── 6. Versioning ──────────────────────────────────────────────────────────

#[tokio::test]
async fn version_increments_on_repeated_puts() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    let key = "/schemas/my-schema";
    for i in 1..=5 {
        store
            .put(key, json!({"v": i}), MetaOptions::None)
            .await
            .unwrap();
    }

    let bulk = store.get_bulk("/schemas/").await.unwrap();
    assert_eq!(bulk.len(), 1);
    assert_eq!(bulk[0].version, 5, "version should be 5 after 5 puts");

    // Value should be the latest.
    let val: Value = serde_json::from_slice(&bulk[0].value).unwrap();
    assert_eq!(val, json!({"v": 5}));
}

// ─── 7. Broker-like patterns ────────────────────────────────────────────────

/// Simulates the full broker metadata lifecycle:
///   1. Create cluster metadata
///   2. Register namespaces
///   3. Create topics under namespaces
///   4. Register broker with TTL
///   5. Post unassigned topics for load manager
///   6. Watch for new assignments
///   7. Clean up
#[tokio::test]
async fn broker_metadata_lifecycle() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    // 1. Create cluster root.
    store
        .put("/cluster/my-cluster", Value::Null, MetaOptions::None)
        .await
        .unwrap();

    // 2. Namespaces.
    store
        .put(
            "/namespaces/default",
            json!({"policies":{}}),
            MetaOptions::None,
        )
        .await
        .unwrap();
    store
        .put(
            "/namespaces/system",
            json!({"policies":{}}),
            MetaOptions::None,
        )
        .await
        .unwrap();

    // 3. Topics.
    store
        .put(
            "/topics/default/orders",
            json!({"partitions": 1}),
            MetaOptions::None,
        )
        .await
        .unwrap();
    store
        .put(
            "/topics/default/payments",
            json!({"partitions": 3}),
            MetaOptions::None,
        )
        .await
        .unwrap();
    store
        .put(
            "/topics/system/__meta",
            json!({"partitions": 1}),
            MetaOptions::None,
        )
        .await
        .unwrap();

    let default_topics = store.get_childrens("/topics/default/").await.unwrap();
    assert_eq!(default_topics.len(), 2);

    // 4. Register broker with TTL.
    store
        .put_with_ttl(
            "/cluster/register/1",
            json!({"broker_url": "http://127.0.0.1:6650", "connect_url": "http://127.0.0.1:6650"}),
            Duration::from_secs(30),
        )
        .await
        .unwrap();

    // 5. Post unassigned topic.
    store
        .put(
            "/cluster/unassigned/default/orders",
            Value::Null,
            MetaOptions::None,
        )
        .await
        .unwrap();

    // 6. Leader watches unassigned and assigns the topic.
    let mut watch = store.watch("/cluster/unassigned/").await.unwrap();

    store
        .put(
            "/cluster/unassigned/default/payments",
            Value::Null,
            MetaOptions::None,
        )
        .await
        .unwrap();

    let ev = tokio::time::timeout(Duration::from_secs(2), watch.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("watch error");
    match ev {
        WatchEvent::Put { key, .. } => {
            let key_str = String::from_utf8_lossy(&key);
            assert!(
                key_str.contains("payments"),
                "should see the payments topic, got {}",
                key_str
            );
        }
        other => panic!("expected Put event, got {:?}", other),
    }

    // 7. Clean up: delete the unassigned entry after assignment.
    store
        .delete("/cluster/unassigned/default/orders")
        .await
        .unwrap();
    store
        .delete("/cluster/unassigned/default/payments")
        .await
        .unwrap();

    let unassigned = store.get_childrens("/cluster/unassigned/").await.unwrap();
    assert!(
        unassigned.is_empty(),
        "all unassigned topics should be cleaned up"
    );
}

/// Simulates load-report posting and reading (like LoadManager does).
#[tokio::test]
async fn load_report_post_and_bulk_read() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    // Three brokers post load reports.
    for broker_id in 1..=3 {
        let path = format!("/cluster/load/{}", broker_id);
        let report = json!({
            "cpu": 0.3 * broker_id as f64,
            "memory": 0.2 * broker_id as f64,
            "topics": ["t1", "t2"],
        });
        store.put(&path, report, MetaOptions::None).await.unwrap();
    }

    // Leader reads all reports in bulk.
    let reports = store.get_bulk("/cluster/load/").await.unwrap();
    assert_eq!(reports.len(), 3);

    // Verify each report can be deserialized.
    for kv in &reports {
        let val: Value = serde_json::from_slice(&kv.value).unwrap();
        assert!(val.get("cpu").is_some());
        assert!(val.get("topics").is_some());
    }
}

/// Simulates leader election publishing (the new Raft-based pattern).
#[tokio::test]
async fn leader_publishes_id_to_metadata_store() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;
    let handle = node.leadership_handle(1);

    assert!(handle.is_leader());

    // Publish leader ID (like LeaderElection::check_leader does).
    let broker_id: u64 = 1;
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
    assert_eq!(val.as_u64(), Some(1));
}

// ─── 8. Edge cases ──────────────────────────────────────────────────────────

#[tokio::test]
async fn delete_nonexistent_key_is_ok() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    // Deleting a key that doesn't exist should not error.
    store.delete("/does/not/exist").await.unwrap();
}

#[tokio::test]
async fn get_childrens_empty_prefix() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    let children = store.get_childrens("/nothing/here/").await.unwrap();
    assert!(children.is_empty());
}

#[tokio::test]
async fn get_bulk_empty_prefix() {
    let (node, _tmp) = start_cluster().await;
    let store = &node.store;

    let bulk = store.get_bulk("/nothing/here/").await.unwrap();
    assert!(bulk.is_empty());
}
