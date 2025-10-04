use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_metadata_store::{MemoryStore, MetadataStorage, MetadataStore};
use danube_persistent_storage::checkpoint::CheckpointStore;
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::{
    BackendConfig, CloudStore, EtcdMetadata, LocalBackend, Uploader, UploaderConfig, WalStorage,
};
use futures::{StreamExt, TryStreamExt};

fn make_msg(i: u64, topic: &str, tag: &str) -> StreamMessage {
    StreamMessage {
        request_id: i,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:8080".to_string(),
            segment_id: 0,
            segment_offset: i,
        },
        payload: format!("{}-{}", tag, i).into_bytes(),
        publish_time: 0,
        producer_name: "producer".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
    }
}

/// Test: local disk cache-only handoff (Cache→Live)
///
/// Purpose
/// - Validate that when starting inside the in-memory cache window, the reader skips the
///   on-disk file phase and seamlessly switches from cache replay to live tail.
///
/// Setup
/// - Initialize a durable `Wal` with a `CheckpointStore`.
/// - Append 0..=4, then flush to ensure they are durable and present in cache.
///
/// Steps
/// - Create a `WalStorage` and a reader from offset 3.
/// - Expect to receive 3 and 4 from cache.
/// - Append 5 and flush, then expect to receive 5 from the live tail.
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
        let m = make_msg(i, topic_path, "wal");
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
    assert_eq!(m3.msg_id.segment_offset, 3);
    let m4 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m4")
        .unwrap()
        .expect("m4 ok");
    assert_eq!(m4.msg_id.segment_offset, 4);

    // Append live 5 and expect it
    let live5 = make_msg(5, topic_path, "wal");
    wal.append(&live5).await.expect("append live5");
    wal.flush().await.expect("flush wal");
    let m5 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m5")
        .unwrap()
        .expect("m5 ok");
    assert_eq!(m5.msg_id.segment_offset, 5);
}

/// Test: local disk Files→Cache→Live
///
/// - Write 0..=2 and flush (persisted on disk)
/// - Then write 3..=4 (should be in cache)
/// - Create reader from 0 and expect 0,1,2 (file), then 3,4 (cache)
/// - Append 5 and expect it (live)
/// Test: Files→Cache→Live handoff using local WAL only
///
/// Purpose
/// - Validate phase transitions across on-disk files, cache entries, and the live WAL tail.
///
/// Setup
/// - Initialize a durable `Wal` with a `CheckpointStore`.
/// - Append 0..=2 and flush (persisted on disk), then append 3..=4 (remain in cache).
///
/// Steps
/// - Create a `WalStorage` and a reader from offset 0.
/// - Expect 0..=2 from files, then 3..=4 from cache.
/// - Append 5 and flush, then expect 5 from live tail.
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
        wal.append(&make_msg(i, topic_path, "file"))
            .await
            .expect("append file");
    }
    wal.flush().await.expect("flush file");

    // Cache phase: 3..=4 in cache (not strictly required to avoid disk, but ok)
    wal.append(&make_msg(3, topic_path, "cache"))
        .await
        .expect("append cache");
    wal.append(&make_msg(4, topic_path, "cache"))
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
    assert_eq!(m0.msg_id.segment_offset, 0);
    let m1 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m1")
        .unwrap()
        .expect("m1 ok");
    assert_eq!(m1.msg_id.segment_offset, 1);
    let m2 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m2")
        .unwrap()
        .expect("m2 ok");
    assert_eq!(m2.msg_id.segment_offset, 2);

    let m3 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m3")
        .unwrap()
        .expect("m3 ok");
    assert_eq!(m3.msg_id.segment_offset, 3);
    let m4 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m4")
        .unwrap()
        .expect("m4 ok");
    assert_eq!(m4.msg_id.segment_offset, 4);

    // Live phase
    wal.append(&make_msg(5, topic_path, "live"))
        .await
        .expect("append live");
    wal.flush().await.expect("flush live");
    let m5 = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout m5")
        .unwrap()
        .expect("m5 ok");
    assert_eq!(m5.msg_id.segment_offset, 5);
}

/// Test: Cloud→WAL chaining end-to-end (memory cloud backend)
///
/// Purpose
/// - Validate chained reading from cloud objects followed by WAL tail, using the uploader to
///   create a historical object and etcd descriptors.
///
/// Setup
/// - Initialize `Wal` with `CheckpointStore` and append 0..=2.
/// - Start uploader (1s interval), wait for object descriptors in etcd, then append 3..=5.
///
/// Steps
/// - Build `WalStorage::with_cloud(...)` and create a reader from offset 0.
/// - Collect 6 items with timeout from the stream.
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
        let m = make_msg(i, topic_path, "cloud");
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
        let m = make_msg(i, topic_path, "wal");
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
