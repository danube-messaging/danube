//! # Metadata Store CRUD & Query Tests
//!
//! Exercises the core `MetadataStore` trait operations through Raft consensus:
//!
//! - **Basic CRUD**: `put`, `get`, `delete` — the building blocks used by every
//!   broker component (`Resources`, `BrokerRegister`, `LocalCache`, etc.).
//! - **Prefix queries**: `get_childrens`, `get` with `MetaOptions::WithPrefix` —
//!   used by the broker to list namespaces, topics, and brokers under a path.
//! - **Bulk reads**: `get_bulk` — used by `LoadManager` to read all broker load
//!   reports and by `LocalCache` to populate initial state.
//! - **Versioning**: per-key version tracking — used by `get_bulk` consumers to
//!   detect stale data and by the schema registry for optimistic concurrency.
//! - **Edge cases**: operations on missing keys / empty prefixes.

mod common;

use danube_core::metadata::{KeyValueVersion, MetaOptions, MetadataStore};
use serde_json::{json, Value};

// ─── Basic CRUD ─────────────────────────────────────────────────────────────

/// **What**: Full put → get → overwrite → delete cycle on a single key.
///
/// **Why**: Every broker resource operation (create cluster, register namespace,
/// create topic) ultimately calls `store.put(path, value, opts)` and later reads
/// or removes it. This test verifies the complete lifecycle through Raft consensus.
///
/// **Checks**:
/// - `get` on a missing key returns `None`
/// - `put` followed by `get` returns the written value
/// - A second `put` overwrites the previous value
/// - `delete` followed by `get` returns `None`
#[tokio::test]
async fn put_get_delete() {
    let (node, _tmp) = common::start_cluster().await;
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

// ─── Prefix queries ─────────────────────────────────────────────────────────

/// **What**: Verify `get_childrens` lists all keys under a prefix, and
/// `get` with `WithPrefix` returns the first match.
///
/// **Why**: The broker uses `get_childrens` to enumerate namespaces
/// (`/namespaces/`), topics (`/topics/default/`), and registered brokers
/// (`/cluster/brokers/`). `WithPrefix` is used for existence checks where
/// only the first match matters.
///
/// **Checks**:
/// - `get_childrens("/namespaces/")` returns exactly the 3 namespace keys
/// - Keys under a different prefix (e.g. `/topics/`) are NOT included
/// - `get` with `WithPrefix` returns `Some` when children exist
#[tokio::test]
async fn get_childrens_and_prefix_get() {
    let (node, _tmp) = common::start_cluster().await;
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

/// **What**: Verify `get_bulk` returns keys, serialized values, and per-key
/// versions for all entries matching a prefix.
///
/// **Why**: `LoadManager` reads all broker load reports via
/// `get_bulk("/cluster/load/")`, and `LocalCache` uses bulk reads to populate
/// its initial state. Version tracking lets consumers detect changes.
///
/// **Checks**:
/// - `get_bulk` returns the correct number of entries
/// - Each newly-written key has version 1
/// - After overwriting a key, its version increments to 2
#[tokio::test]
async fn get_bulk_returns_keys_values_and_versions() {
    let (node, _tmp) = common::start_cluster().await;
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

// ─── Versioning ─────────────────────────────────────────────────────────────

/// **What**: Verify that the per-key version counter increments monotonically
/// on every `put` to the same key.
///
/// **Why**: The schema registry uses versions for optimistic concurrency control,
/// and `get_bulk` exposes the version field so consumers can detect stale reads.
/// Correct version tracking is essential for Raft-backed metadata consistency.
///
/// **Checks**:
/// - After 5 sequential puts, `get_bulk` reports version = 5
/// - The stored value is the most recently written one
#[tokio::test]
async fn version_increments_on_repeated_puts() {
    let (node, _tmp) = common::start_cluster().await;
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

// ─── Edge cases ─────────────────────────────────────────────────────────────

/// **What**: Deleting a key that was never written should succeed silently.
///
/// **Why**: The broker deletes unassigned-topic entries after assignment without
/// checking whether the key exists first. The store must tolerate this gracefully.
#[tokio::test]
async fn delete_nonexistent_key_is_ok() {
    let (node, _tmp) = common::start_cluster().await;
    let store = &node.store;

    store.delete("/does/not/exist").await.unwrap();
}

/// **What**: `get_childrens` on a prefix with no matching keys returns an empty vec.
///
/// **Why**: The broker calls `get_childrens` at startup when the metadata store
/// may be empty. It must not error on an empty result set.
#[tokio::test]
async fn get_childrens_empty_prefix() {
    let (node, _tmp) = common::start_cluster().await;
    let store = &node.store;

    let children = store.get_childrens("/nothing/here/").await.unwrap();
    assert!(children.is_empty());
}

/// **What**: `get_bulk` on a prefix with no matching keys returns an empty vec.
///
/// **Why**: `LoadManager` calls `get_bulk("/cluster/load/")` during bootstrap.
/// Before any broker has posted a load report, this must return an empty list.
#[tokio::test]
async fn get_bulk_empty_prefix() {
    let (node, _tmp) = common::start_cluster().await;
    let store = &node.store;

    let bulk = store.get_bulk("/nothing/here/").await.unwrap();
    assert!(bulk.is_empty());
}
