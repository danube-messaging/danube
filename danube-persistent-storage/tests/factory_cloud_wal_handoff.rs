use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_metadata_store::{MemoryStore, MetadataStorage, MetadataStore};
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::CloudStore;
use danube_persistent_storage::EtcdMetadata;
use danube_persistent_storage::{
    BackendConfig, LocalBackend, UploaderBaseConfig, WalStorageFactory,
};
use tokio::time::{timeout, Duration};
use tokio_stream::StreamExt;

fn make_msg(topic: &str, off: u64, payload: &[u8]) -> StreamMessage {
    StreamMessage {
        request_id: off,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:6650".to_string(),
            segment_id: 0,
            segment_offset: off,
        },
        payload: payload.to_vec(),
        publish_time: 0,
        producer_name: "producer".to_string(),
        subscription_name: None,
        attributes: Default::default(),
    }
}

/// Test: Cloud→WAL chaining via WalStorageFactory (historical then live)
///
/// Purpose
/// - Validate that a reader created from a factory-produced `WalStorage` first reads
///   historical data from cloud objects and then seamlessly hands off to the local WAL tail.
///
/// Setup
/// - Build a factory with in-memory cloud and etcd; use fast uploader interval (1s).
/// - Ensure per-topic WAL dir exists so WAL writer can start cleanly.
/// - Start per-topic storage via `factory.for_topic(...)` which starts the uploader.
/// - Append offsets 0..=2 via `storage.append_message(...)` so the uploader ingests and
///   writes an object + descriptor to etcd.
///
/// Steps
/// - Wait briefly, then poll etcd for object descriptors under
///   `/danube/storage/topics/<ns/topic>/objects`.
/// - Create a reader from offset 0.
/// - Expect the first three messages (0..=2) from cloud; then pad 0..=2 and append `live3`.
/// - Expect `live3` from WAL tail.
///
/// Expected
/// - Ordered delivery `h0, h1, h2` from cloud followed by `live3` from WAL tail.
#[tokio::test]
#[ignore]
async fn test_factory_cloud_wal_handoff_per_topic() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().to_path_buf();

    // Build memory metadata store
    let mem = MemoryStore::new().await.expect("mem store");

    // Create shared cloud and etcd instances for both test setup and factory
    let cloud = CloudStore::new(BackendConfig::Local {
        backend: LocalBackend::Memory,
        root: "mem-prefix".to_string(),
    })
    .expect("cloud");
    let etcd = EtcdMetadata::new(
        MetadataStorage::InMemory(mem.clone()),
        "/danube".to_string(),
    );

    // Build factory using shared cloud and etcd instances
    let factory = WalStorageFactory::new_with_stores(
        WalConfig {
            dir: Some(wal_root.clone()),
            ..Default::default()
        },
        cloud.clone(),
        etcd.clone(),
        UploaderBaseConfig {
            interval_seconds: 1,
            ..Default::default()
        },
    );

    // Ensure per-topic WAL file exists at <root>/ns/topic/wal.log to avoid open errors on reader startup.
    let per_topic_dir = wal_root.join("default").join("topic-hand");
    tokio::fs::create_dir_all(&per_topic_dir)
        .await
        .expect("create per-topic wal dir");
    let wal_file = per_topic_dir.join("wal.log");
    if tokio::fs::metadata(&wal_file).await.is_err() {
        // create empty file; writer task will use/create as needed later
        let _ = tokio::fs::File::create(&wal_file)
            .await
            .expect("create wal.log");
    }

    let topic_name = "/default/topic-hand";
    let topic_path = "default/topic-hand"; // for cloud/etcd

    // Create per-topic storage via factory (this starts the per-topic uploader)
    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Pre-populate via storage appends 0..=2 so uploader sees frames immediately
    storage
        .append_message(topic_name, make_msg(topic_path, 0, b"h0"))
        .await
        .expect("append 0");
    storage
        .append_message(topic_name, make_msg(topic_path, 1, b"h1"))
        .await
        .expect("append 1");
    storage
        .append_message(topic_name, make_msg(topic_path, 2, b"h2"))
        .await
        .expect("append 2");

    // Brief head-start for the uploader, then wait for descriptor in etcd
    tokio::time::sleep(Duration::from_millis(500)).await;
    let prefix = format!("/danube/storage/topics/{}/objects", topic_path);
    let mut found = false;
    for _ in 0..50 {
        // up to ~5s
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

    // Create reader from offset 0 (should chain cloud [0..1] then tail WAL)
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

    // The WAL is fresh; with cloud end=2, next live should be 3. Append padding 0..2 so next is 3.
    storage
        .append_message(topic_name, make_msg(topic_name, 0, b"pad0"))
        .await
        .expect("append pad0");
    storage
        .append_message(topic_name, make_msg(topic_name, 1, b"pad1"))
        .await
        .expect("append pad1");
    storage
        .append_message(topic_name, make_msg(topic_name, 2, b"pad2"))
        .await
        .expect("append pad2");
    storage
        .append_message(topic_name, make_msg(topic_name, 3, b"live3"))
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
/// Setup
/// - Build factory with fast uploader interval (1s) and in-memory stores.
/// - Ensure per-topic dir and wal.log exist.
/// - Start per-topic storage and append 0..=2; wait for the object descriptor to appear.
///
/// Steps
/// - Create a reader from offset 3 (after cloud end=2) to force WAL-only path.
/// - Append pad 0..=2, then new messages 3 and 4.
/// - Expect `w3` and `w4` from the WAL tail.
///
/// Expected
/// - Reader does not consume any cloud records and immediately reads from WAL: `w3`, `w4`.
#[tokio::test]
#[ignore]
async fn test_factory_cloud_skip_cloud_when_start_after_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().to_path_buf();

    // Build memory metadata store
    let mem = MemoryStore::new().await.expect("mem store");

    // Create shared cloud and etcd instances for both test setup and factory
    let cloud = CloudStore::new(BackendConfig::Local {
        backend: LocalBackend::Memory,
        root: "mem-prefix-skip".to_string(),
    })
    .expect("cloud");
    let etcd = EtcdMetadata::new(
        MetadataStorage::InMemory(mem.clone()),
        "/danube".to_string(),
    );

    // Build factory using shared cloud and etcd instances
    let factory = WalStorageFactory::new_with_stores(
        WalConfig {
            dir: Some(wal_root.clone()),
            ..Default::default()
        },
        cloud.clone(),
        etcd.clone(),
        UploaderBaseConfig {
            interval_seconds: 1,
            ..Default::default()
        },
    );

    let topic_name = "/default/topic-skip";
    let topic_path = "default/topic-skip"; // for cloud/etcd

    // Ensure per-topic WAL file exists at <root>/ns/topic/wal.log to avoid open errors on reader startup.
    let per_topic_dir = wal_root.join("default").join("topic-skip");
    tokio::fs::create_dir_all(&per_topic_dir)
        .await
        .expect("create per-topic wal dir");
    let wal_file = per_topic_dir.join("wal.log");
    if tokio::fs::metadata(&wal_file).await.is_err() {
        // create empty file; writer task will use/create as needed later
        let _ = tokio::fs::File::create(&wal_file)
            .await
            .expect("create wal.log");
    }

    // Create per-topic storage via factory (starts per-topic uploader)
    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Pre-populate via storage appends 0..=2 so uploader sees frames immediately
    storage
        .append_message(topic_name, make_msg(topic_path, 0, b"h0"))
        .await
        .expect("append 0");
    storage
        .append_message(topic_name, make_msg(topic_path, 1, b"h1"))
        .await
        .expect("append 1");
    storage
        .append_message(topic_name, make_msg(topic_path, 2, b"h2"))
        .await
        .expect("append 2");

    // Brief head-start for uploader then wait for descriptor
    tokio::time::sleep(Duration::from_millis(500)).await;
    let prefix = format!("/danube/storage/topics/{}/objects", topic_path);
    let mut found = false;
    for _ in 0..50 {
        // up to ~5s
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

    // Create reader starting after cloud end (2)
    let mut reader = storage
        .create_reader(topic_name, StartPosition::Offset(3))
        .await
        .expect("reader");

    // Ensure WAL offsets align: append padding 0..2, then 3 and 4
    storage
        .append_message(topic_name, make_msg(topic_name, 0, b"pad0"))
        .await
        .expect("append pad0");
    storage
        .append_message(topic_name, make_msg(topic_name, 1, b"pad1"))
        .await
        .expect("append pad1");
    storage
        .append_message(topic_name, make_msg(topic_name, 2, b"pad2"))
        .await
        .expect("append pad2");
    storage
        .append_message(topic_name, make_msg(topic_name, 3, b"w3"))
        .await
        .expect("append w3");
    storage
        .append_message(topic_name, make_msg(topic_name, 4, b"w4"))
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
