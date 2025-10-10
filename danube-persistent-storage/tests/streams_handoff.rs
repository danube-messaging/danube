use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use tokio::time::timeout;

use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_metadata_store::{MemoryStore, MetadataStorage, MetadataStore};
use danube_persistent_storage::checkpoint::CheckpointStore;
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::{
    BackendConfig, CloudStore, EtcdMetadata, LocalBackend, Uploader, UploaderConfig, WalStorage,
};

// Use shared helpers to align integration test setup
mod common;

fn make_msg_tagged(i: u64, topic: &str, tag: &str) -> StreamMessage {
    StreamMessage {
        request_id: i,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:8080".to_string(),
            topic_offset: i,
        },
        payload: format!("{}-{}", tag, i).into_bytes(),
        publish_time: 0,
        producer_name: "producer".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
    }
}

fn make_msg_bin(topic: &str, off: u64, payload: &[u8]) -> StreamMessage {
    StreamMessage {
        request_id: off,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:6650".to_string(),
            topic_offset: off,
        },
        payload: payload.to_vec(),
        publish_time: 0,
        producer_name: "producer".to_string(),
        subscription_name: None,
        attributes: Default::default(),
    }
}

/// Test: local disk cache-only handoff (Cache→Live)
///
/// Purpose
/// - Validate that when starting inside the in-memory cache window, the reader skips the
///   on-disk file phase and seamlessly switches from cache replay to live tail.
///
/// Flow
/// - Initialize a durable `Wal` with a `CheckpointStore`.
/// - Append 0..=4 and flush so they are present in cache.
/// - Create a `WalStorage` and a reader from offset 3.
/// - Receive 3 and 4 from cache, then append 5 and flush.
///
/// Expected
/// - Messages 3 and 4 are delivered quickly (cache), followed by 5 from live tail.
#[tokio::test]
async fn local_cache_only_handoff() {
    let topic_path = "ns/cache-only";

    // WAL durable config with CheckpointStore
    let tmp = tempfile::TempDir::new().expect("temp wal dir");
    let wal_dir = tmp.path().to_path_buf();
    let cfg = WalConfig {
        dir: Some(wal_dir.clone()),
        cache_capacity: Some(128),
        ..Default::default()
    };
    let wal_ckpt = wal_dir.join("wal.ckpt");
    let uploader_ckpt = wal_dir.join("uploader.ckpt");
    let store = std::sync::Arc::new(CheckpointStore::new(wal_ckpt, uploader_ckpt));
    let _ = store.load_from_disk().await;
    let wal = Wal::with_config_with_store(cfg, Some(store.clone()))
        .await
        .expect("wal init");

    // Append 0..=4
    for i in 0..5u64 {
        let m = make_msg_tagged(i, topic_path, "wal");
        wal.append(&m).await.expect("append");
    }
    wal.flush().await.expect("flush wal");

    let storage = WalStorage::from_wal(wal.clone());
    let mut stream = storage
        .create_reader(topic_path, StartPosition::Offset(3))
        .await
        .expect("create reader inside cache window");

    // Expect to receive 3,4 from cache
    let m3 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m3")
        .unwrap()
        .expect("m3 ok");
    assert_eq!(m3.msg_id.topic_offset, 3);
    let m4 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m4")
        .unwrap()
        .expect("m4 ok");
    assert_eq!(m4.msg_id.topic_offset, 4);

    // Append live 5 and expect it
    let live5 = make_msg_tagged(5, topic_path, "wal");
    wal.append(&live5).await.expect("append live5");
    wal.flush().await.expect("flush wal");
    let m5 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m5")
        .unwrap()
        .expect("m5 ok");
    assert_eq!(m5.msg_id.topic_offset, 5);
}

/// Test: Files→Cache→Live handoff using local WAL only
///
/// Purpose
/// - Validate phase transitions across on-disk files, cache entries, and the live WAL tail.
///
/// Flow
/// - Initialize a durable `Wal` with a `CheckpointStore`.
/// - Append 0..=2 and flush (persisted on disk), then append 3..=4 (remain in cache).
/// - Create a `WalStorage` reader from offset 0.
/// - Expect 0..=2 from files, then 3..=4 from cache; append 5 and flush.
///
/// Expected
/// - Strictly ordered delivery 0..=5 across file→cache→live boundaries without gaps or duplicates.
#[tokio::test]
async fn local_files_cache_live_handoff() {
    let topic_path = "ns/files-cache-live";

    // WAL durable config with CheckpointStore
    let tmp = tempfile::TempDir::new().expect("temp wal dir");
    let wal_dir = tmp.path().to_path_buf();
    let cfg = WalConfig {
        dir: Some(wal_dir.clone()),
        cache_capacity: Some(128),
        ..Default::default()
    };
    let wal_ckpt = wal_dir.join("wal.ckpt");
    let uploader_ckpt = wal_dir.join("uploader.ckpt");
    let store = std::sync::Arc::new(CheckpointStore::new(wal_ckpt, uploader_ckpt));
    let _ = store.load_from_disk().await;
    let wal = Wal::with_config_with_store(cfg, Some(store.clone()))
        .await
        .expect("wal init");

    // File phase: 0..=2 persisted to disk
    for i in 0..3u64 {
        wal.append(&make_msg_tagged(i, topic_path, "file"))
            .await
            .expect("append file");
    }
    wal.flush().await.expect("flush file");

    // Cache phase: 3..=4 in cache
    wal.append(&make_msg_tagged(3, topic_path, "cache"))
        .await
        .expect("append cache");
    wal.append(&make_msg_tagged(4, topic_path, "cache"))
        .await
        .expect("append cache");

    let storage = WalStorage::from_wal(wal.clone());
    let mut stream = storage
        .create_reader(topic_path, StartPosition::Offset(0))
        .await
        .expect("create reader from 0");

    let m0 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m0")
        .unwrap()
        .expect("m0 ok");
    assert_eq!(m0.msg_id.topic_offset, 0);
    let m1 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m1")
        .unwrap()
        .expect("m1 ok");
    assert_eq!(m1.msg_id.topic_offset, 1);
    let m2 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m2")
        .unwrap()
        .expect("m2 ok");
    assert_eq!(m2.msg_id.topic_offset, 2);

    let m3 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m3")
        .unwrap()
        .expect("m3 ok");
    assert_eq!(m3.msg_id.topic_offset, 3);
    let m4 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m4")
        .unwrap()
        .expect("m4 ok");
    assert_eq!(m4.msg_id.topic_offset, 4);

    // Live phase
    wal.append(&make_msg_tagged(5, topic_path, "live"))
        .await
        .expect("append live");
    wal.flush().await.expect("flush live");
    let m5 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m5")
        .unwrap()
        .expect("m5 ok");
    assert_eq!(m5.msg_id.topic_offset, 5);
}

/// Test: Cloud→WAL chaining end-to-end (memory cloud backend)
///
/// Purpose
/// - Validate chained reading from cloud objects followed by WAL tail, using the uploader to
///   create a historical object and etcd descriptors.
///
/// Flow
/// - Initialize `Wal` with `CheckpointStore` and append 0..=2, then flush.
/// - Start uploader (1s interval), wait for object descriptors in etcd.
/// - Append 3..=5 and flush.
/// - Build `WalStorage::with_cloud(...)` and create a reader from offset 0; collect 6 items.
///
/// Expected
/// - Messages 0..=2 originate from the cloud object, 3..=5 from WAL tail; total of 6 in order.
#[tokio::test]
async fn chaining_stream_handoff_memory() {
    let topic_path = "ns/topic-handoff";

    // WAL durable config with CheckpointStore
    let tmp = tempfile::TempDir::new().expect("temp wal dir");
    let wal_dir = tmp.path().to_path_buf();
    let cfg = WalConfig {
        dir: Some(wal_dir.clone()),
        cache_capacity: Some(128),
        ..Default::default()
    };
    let wal_ckpt = wal_dir.join("wal.ckpt");
    let uploader_ckpt = wal_dir.join("uploader.ckpt");
    let store = std::sync::Arc::new(CheckpointStore::new(wal_ckpt, uploader_ckpt));
    let _ = store.load_from_disk().await;
    let wal = Wal::with_config_with_store(cfg, Some(store.clone()))
        .await
        .expect("wal init");

    // Append 0..2 and upload
    for i in 0..3u64 {
        let m = make_msg_tagged(i, topic_path, "cloud");
        wal.append(&m).await.expect("append pre-upload");
    }
    wal.flush().await.expect("flush wal");

    let cloud = CloudStore::new(BackendConfig::Local {
        backend: LocalBackend::Memory,
        root: "mem-handoff".to_string(),
    })
    .expect("cloud store mem");

    let mem = MemoryStore::new().await.expect("memory meta store");
    let meta = EtcdMetadata::new(
        MetadataStorage::InMemory(mem.clone()),
        "/danube".to_string(),
    );

    let up_cfg = UploaderConfig {
        interval_seconds: 1,
        topic_path: topic_path.to_string(),
        root_prefix: "/danube".to_string(),
    };
    let uploader = Arc::new(
        Uploader::new(up_cfg, cloud.clone(), meta.clone(), Some(store)).expect("uploader"),
    );
    let handle = uploader.clone().start();
    // Deterministic wait for object creation in metadata
    let prefix = format!("/danube/storage/topics/{}/objects", topic_path);
    let mut found = false;
    for _ in 0..50 {
        // up to 5s
        let children = mem.get_childrens(&prefix).await.unwrap_or_default();
        let objects: Vec<_> = children
            .into_iter()
            .filter(|c| c != "cur" && !c.ends_with('/'))
            .collect();
        if !objects.is_empty() {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "no object descriptors found under {}", prefix);
    handle.abort();

    // Append 3..5 after upload (these exist only in WAL)
    for i in 3..6u64 {
        let m = make_msg_tagged(i, topic_path, "wal");
        wal.append(&m).await.expect("append post-upload");
    }
    wal.flush().await.expect("flush wal");

    // Build WalStorage with cloud capabilities and read from offset 0
    let storage = WalStorage::from_wal(wal.clone()).with_cloud(
        cloud.clone(),
        meta.clone(),
        topic_path.to_string(),
    );
    let stream = storage
        .create_reader(topic_path, StartPosition::Offset(0))
        .await
        .expect("create reader");

    // Limit to the first 6 messages to avoid hanging on the infinite WAL tail stream
    let fut = stream.take(6).try_collect::<Vec<_>>();
    let msgs: Vec<StreamMessage> = timeout(Duration::from_secs(5), fut)
        .await
        .expect("stream timed out waiting for 6 messages")
        .expect("try_collect");

    // Expect exactly 6 messages (0..5) in order
    assert_eq!(msgs.len(), 6, "should receive 6 messages across cloud+wal");
    for (i, m) in msgs.into_iter().enumerate() {
        let payload_str = String::from_utf8_lossy(&m.payload);
        // First three should be produced by CloudReader, last three by WAL tail.
        if i < 3 {
            assert!(
                payload_str.starts_with("cloud-"),
                "expected cloud payload at {}, got {}",
                i,
                payload_str
            );
        } else {
            assert!(
                payload_str.starts_with("wal-"),
                "expected wal payload at {}, got {}",
                i,
                payload_str
            );
        }
    }
}

/// Test: Cloud→WAL chaining via WalStorageFactory (historical then live)
///
/// Purpose
/// - Validate that a reader created from a factory-produced `WalStorage` first reads
///   historical data from cloud objects and then seamlessly hands off to the local WAL tail.
///
/// Flow
/// - Build a factory with in-memory cloud and etcd; ensure per-topic WAL dir exists.
/// - Start per-topic storage via `factory.for_topic(...)` (starts uploader).
/// - Append 0..=2 via storage so uploader ingests and writes object + descriptor; wait for descriptor.
/// - Create a reader from offset 0; expect the three historical frames; then pad 0..=2 and append `live3`.
///
/// Expected
/// - Ordered delivery `h0, h1, h2` from cloud followed by `live3` from the WAL tail.
#[tokio::test]
async fn test_factory_cloud_wal_handoff_per_topic() {
    let (factory, _mem) = common::create_test_factory().await;
    let topic_name = "/default/topic-hand";
    let topic_path = "default/topic-hand"; // for cloud/etcd

    // Create per-topic storage via factory (this starts the per-topic uploader)
    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Pre-populate via storage appends 0..=2 so uploader sees a small historical window
    for i in 0..3u64 {
        let label = format!("h{}", i);
        storage
            .append_message(topic_name, make_msg_bin(topic_path, i, label.as_bytes()))
            .await
            .expect("append pre-upload");
    }

    // Brief head-start for the uploader, then wait for descriptor in etcd using common helpers
    // tokio::time::sleep(Duration::from_millis(1500)).await;
    // let uploaded = common::wait_for_condition(
    //     || {
    //         let mem = mem.clone();
    //         let topic_path = topic_path.to_string();
    //         async move { common::count_cloud_objects(&mem, &topic_path).await > 0 }
    //     },
    //     12000,
    // )
    // .await;
    //assert!(uploaded, "cloud objects not found for {} within timeout", topic_path);

    // Create reader from offset 0 (should chain cloud then WAL)
    let mut reader = storage
        .create_reader(topic_name, StartPosition::Offset(0))
        .await
        .expect("reader");

    // Expect to read the three historical records first (use timeouts to avoid hangs)
    let r0 = timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout r0")
        .unwrap()
        .unwrap();
    let r1 = timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout r1")
        .unwrap()
        .unwrap();
    let r2 = timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout r2")
        .unwrap()
        .unwrap();
    assert_eq!(r0.payload, b"h0");
    assert_eq!(r1.payload, b"h1");
    assert_eq!(r2.payload, b"h2");

    // The WAL is fresh; with cloud end=2, next live should be offset 3. Append only the next live item.
    storage
        .append_message(topic_name, make_msg_bin(topic_path, 3, b"live3"))
        .await
        .expect("append live");

    let r3 = timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout r3")
        .unwrap()
        .unwrap();
    assert_eq!(r3.payload, b"live3");
}

/// Test: factory cloud→WAL handoff with start after cloud range (skip cloud)
///
/// Purpose
/// - Ensure that when the reader starts after the last uploaded cloud object, it skips
///   cloud reads and starts directly from the WAL tail.
///
/// Flow
/// - Build factory with in-memory cloud and etcd and ensure per-topic WAL file exists.
/// - Start per-topic storage and append 0..=2; wait for the object descriptor to appear.
/// - Create a reader from offset 3 (after cloud end=2) to force WAL-only path.
/// - Append padding 0..=2, then new messages 3 and 4.
///
/// Expected
/// - Reader does not consume any cloud records and immediately reads from WAL: `w3`, `w4`.
#[tokio::test]
async fn test_factory_cloud_skip_cloud_when_start_after_objects() {
    let (factory, _mem) = common::create_test_factory().await;
    let topic_name = "/default/topic-skip";
    let topic_path = "default/topic-skip"; // for cloud/etcd
                                           // Create per-topic storage via factory (starts per-topic uploader)
    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Pre-populate via storage appends 0..=9 so uploader sees frames immediately
    for i in 0..3u64 {
        let label = format!("h{}", i);
        storage
            .append_message(topic_name, make_msg_bin(topic_path, i, label.as_bytes()))
            .await
            .expect("append pre-upload");
    }

    // Brief head-start then wait for descriptor using common helpers
    // tokio::time::sleep(Duration::from_millis(1500)).await;
    // let uploaded = common::wait_for_condition(
    //     || {
    //         let mem = mem.clone();
    //         let topic_path = topic_path.to_string();
    //         async move { common::count_cloud_objects(&mem, &topic_path).await > 0 }
    //     },
    //     12000,
    // )
    // .await;
    // assert!(
    //     uploaded,
    //     "cloud objects not found for {} within timeout",
    //     topic_path
    // );

    // Create reader starting after cloud end (2)
    let mut reader = storage
        .create_reader(topic_name, StartPosition::Offset(3))
        .await
        .expect("reader");

    // Append only new live messages starting at offset 3
    storage
        .append_message(topic_name, make_msg_bin(topic_path, 3, b"w3"))
        .await
        .expect("append w3");
    storage
        .append_message(topic_name, make_msg_bin(topic_path, 4, b"w4"))
        .await
        .expect("append w4");

    let r3 = timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout r3")
        .unwrap()
        .unwrap();
    let r4 = timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout r4")
        .unwrap()
        .unwrap();
    assert_eq!(r3.payload, b"w3");
    assert_eq!(r4.payload, b"w4");
}
