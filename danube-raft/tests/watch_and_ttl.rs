//! # Watch Events & TTL Expiration Tests
//!
//! Validates the reactive and time-based features of the Raft metadata store:
//!
//! - **Watch streams**: The broker's `LocalCache` and `LoadManager` subscribe to
//!   key prefixes via `store.watch(prefix)` and react to `Put` / `Delete` events
//!   in real time. These tests verify that events are delivered correctly and that
//!   prefix isolation is enforced.
//! - **TTL keys**: Broker registration uses `put_with_ttl` to register with a
//!   time-to-live. A background TTL worker periodically proposes deletion of
//!   expired keys through Raft consensus. These tests verify both automatic
//!   expiration and the renewal pattern the broker uses to keep its registration
//!   alive.

mod common;

use std::time::Duration;

use danube_core::metadata::{MetaOptions, MetadataStore, WatchEvent};
use futures::StreamExt;
use serde_json::json;

// ─── Watch streams ──────────────────────────────────────────────────────────

/// **What**: Subscribe to a prefix, write two keys, delete one, and verify
/// that the watch stream delivers the corresponding `Put` and `Delete` events
/// in order.
///
/// **Why**: The broker's `LocalCache::process_event` loop consumes a watch
/// stream and updates its in-memory trie on every event. `LoadManager::start`
/// watches `/cluster/` for broker registrations, load reports, and unassigned
/// topics. Correct event delivery is critical for cluster consistency.
///
/// **Checks**:
/// - Two `Put` events arrive for the two written keys
/// - A `Delete` event arrives after the key is removed
/// - Event keys match the written / deleted paths exactly
#[tokio::test]
async fn watch_receives_put_and_delete_events() {
    let (node, _tmp) = common::start_cluster().await;
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

/// **What**: Verify that a watch stream scoped to `/topics/` does NOT receive
/// events for writes to `/namespaces/`.
///
/// **Why**: The broker sets up multiple independent watches (e.g. `LocalCache`
/// watches the entire `/` tree, while `LoadManager` watches `/cluster/`). Prefix
/// isolation ensures that each watch only sees relevant events, preventing
/// spurious processing and reducing broadcast traffic.
///
/// **Checks**:
/// - A write to `/namespaces/default` does NOT appear on the `/topics/` watch
/// - A subsequent write to `/topics/t1` IS delivered as the first event
#[tokio::test]
async fn watch_does_not_receive_unrelated_keys() {
    let (node, _tmp) = common::start_cluster().await;
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

// ─── TTL key expiration ─────────────────────────────────────────────────────

/// **What**: Write a key with a short TTL and verify it is automatically
/// deleted by the TTL worker after the TTL elapses.
///
/// **Why**: The broker registers itself with `put_with_ttl` (see
/// `broker_register::register_broker`). If the broker crashes without
/// gracefully deregistering, the TTL ensures the stale registration is
/// cleaned up automatically, allowing the load manager to detect the
/// broker as offline.
///
/// **Checks**:
/// - Key is readable immediately after `put_with_ttl`
/// - After waiting longer than the TTL + worker interval, `get` returns `None`
#[tokio::test]
async fn ttl_key_expires_automatically() {
    let (node, _tmp) = common::start_cluster().await;
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

/// **What**: Write a key with a short TTL, renew it before expiry via another
/// `put_with_ttl`, and verify the key survives past the original deadline.
///
/// **Why**: The broker's registration background task calls `put_with_ttl`
/// every `ttl / 3` seconds to renew its registration (see
/// `broker_register::register_broker`). The renewal must cancel the previous
/// TTL and set a fresh one; otherwise the stale expiry will delete the key
/// prematurely and the load manager will consider the broker offline.
///
/// **Checks**:
/// - After renewal, the key still exists past the original TTL deadline
#[tokio::test]
async fn ttl_renewal_keeps_key_alive() {
    let (node, _tmp) = common::start_cluster().await;
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
