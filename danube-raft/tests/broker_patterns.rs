//! # Broker Business-Pattern Tests
//!
//! End-to-end tests that replicate the real metadata access patterns used by the
//! Danube broker. Each test mirrors a specific broker subsystem:
//!
//! - **Metadata lifecycle**: The sequence of writes a broker performs at startup —
//!   create cluster, register namespaces, create topics, register itself, post
//!   unassigned topics, watch for assignments, and clean up after assignment.
//! - **Load reports**: The `LoadManager` pattern of brokers posting periodic load
//!   reports and the leader reading them in bulk to compute rankings.
//!
//! These tests exercise multiple `MetadataStore` operations together in the order
//! and combination the broker actually uses them, catching integration issues that
//! isolated unit tests may miss.

mod common;

use std::time::Duration;

use danube_core::metadata::{MetaOptions, MetadataStore, WatchEvent};
use futures::StreamExt;
use serde_json::{json, Value};

/// **What**: Simulate the full broker startup metadata lifecycle from cluster
/// creation through topic assignment and cleanup.
///
/// **Why**: When a broker starts, `DanubeService::start` performs these steps
/// in sequence:
///   1. `ClusterResources::create_cluster` — write cluster root
///   2. `create_namespace_if_absent` — register default + system namespaces
///   3. `TopicResources::create_topic` — create topics under namespaces
///   4. `register_broker` — register with TTL for liveness detection
///   5. `ClusterResources::new_unassigned_topic` — post unassigned topics
///   6. `LoadManager::start` — watch `/cluster/unassigned/` for new assignments
///   7. After assigning a topic, delete the unassigned entry
///
/// This test runs through all seven steps on a real Raft cluster to verify
/// the entire flow works end-to-end through Raft consensus.
///
/// **Checks**:
/// - All puts succeed through Raft consensus
/// - `get_childrens` correctly lists topics under a namespace
/// - Watch stream delivers the unassigned topic event
/// - After cleanup, `get_childrens` on the unassigned prefix returns empty
#[tokio::test]
async fn broker_metadata_lifecycle() {
    let (node, _tmp) = common::start_cluster().await;
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

/// **What**: Simulate the load-report posting and bulk-read pattern used by
/// the `LoadManager`.
///
/// **Why**: Each broker periodically posts a JSON load report to
/// `/cluster/load/{broker_id}` (see `post_broker_load_report` in
/// `danube_service.rs`). The leader's `LoadManager` reads all reports via
/// `get_bulk("/cluster/load/")` to compute broker rankings and make
/// assignment / rebalancing decisions. This test verifies the full cycle.
///
/// **Checks**:
/// - Three brokers can independently post load reports
/// - `get_bulk` returns all three reports
/// - Each report is correctly deserializable from the bulk response
#[tokio::test]
async fn load_report_post_and_bulk_read() {
    let (node, _tmp) = common::start_cluster().await;
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
