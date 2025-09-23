//! Test: Cloud→WAL chaining using factory-produced WalStorage
//!
//! Goal: Validate that a reader created from WalStorageFactory-backed WalStorage
//! will first read historical data from Cloud (objects + descriptors), then
//! switch to WAL tailing for live messages.

use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_metadata_store::{MemoryStore, MetadataStorage};
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::CloudStore;
use danube_persistent_storage::{BackendConfig, LocalBackend, WalStorageFactory};
use danube_persistent_storage::{EtcdMetadata, ObjectDescriptor};
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

fn make_dnb1_object(records: &[(u64, StreamMessage)]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"DNB1");
    bytes.push(1u8);
    let count = records.len() as u32;
    bytes.extend_from_slice(&count.to_le_bytes());
    for (off, msg) in records.iter() {
        bytes.extend_from_slice(&off.to_le_bytes());
        let rec = bincode::serialize(msg).expect("serialize");
        let len = rec.len() as u32;
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(&rec);
    }
    bytes
}

#[tokio::test(flavor = "multi_thread")]
async fn test_factory_cloud_wal_handoff_per_topic() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().to_path_buf();

    // Build memory cloud and memory metadata
    let cloud = CloudStore::new(BackendConfig::Local {
        backend: LocalBackend::Memory,
        root: "mem-prefix".to_string(),
    })
    .expect("cloud");
    let mem = MemoryStore::new().await.expect("mem store");
    let etcd = EtcdMetadata::new(MetadataStorage::InMemory(mem), "/danube".to_string());

    // Build factory that uses our cloud/etcd so we can pre-populate history
    let factory = WalStorageFactory::new_with_config(
        WalConfig {
            dir: Some(wal_root.clone()),
            ..Default::default()
        },
        cloud.clone(),
        etcd.clone(),
    );

    let topic_name = "/default/topic-hand";
    let topic_path = "default/topic-hand"; // for cloud/etcd

    // Pre-populate cloud with an object covering offsets [0..1]
    let rec0 = make_msg(topic_path, 0, b"h0");
    let rec1 = make_msg(topic_path, 1, b"h1");
    let obj = make_dnb1_object(&[(0, rec0.clone()), (1, rec1.clone())]);
    let object_id = "data-0-1.dnb1";
    let object_path = format!("storage/topics/{}/objects/{}", topic_path, object_id);
    let meta = cloud
        .put_object_meta(&object_path, &obj)
        .await
        .expect("put object");

    // Write descriptor into etcd
    let desc = ObjectDescriptor {
        object_id: object_id.to_string(),
        start_offset: 0,
        end_offset: 1,
        size: obj.len() as u64,
        etag: meta.etag().map(|s| s.to_string()),
        created_at: chrono::Utc::now().timestamp() as u64,
        completed: true,
    };
    let start_padded = format!("{:020}", 0);
    etcd.put_object_descriptor(topic_path, &start_padded, &desc)
        .await
        .expect("put desc");

    // Create per-topic storage via factory
    let storage = factory.for_topic(topic_name);

    // Create reader from offset 0 (should chain cloud [0..1] then tail WAL)
    let mut reader = storage
        .create_reader(topic_name, StartPosition::Offset(0))
        .await
        .expect("reader");

    // Expect to read the two historical records first
    let r0 = reader.next().await.unwrap().unwrap();
    let r1 = reader.next().await.unwrap().unwrap();
    assert_eq!(r0.payload, b"h0");
    assert_eq!(r1.payload, b"h1");

    // The WAL is fresh; its offsets start at 0. Cloud handoff expects to switch at h = 2.
    // Append two padding messages (offsets 0 and 1) so the next append is offset 2.
    storage
        .append_message(topic_name, make_msg(topic_name, 0, b"pad0"))
        .await
        .expect("append pad0");
    storage
        .append_message(topic_name, make_msg(topic_name, 1, b"pad1"))
        .await
        .expect("append pad1");
    storage
        .append_message(topic_name, make_msg(topic_name, 2, b"live2"))
        .await
        .expect("append live");

    let r2 = reader.next().await.unwrap().unwrap();
    assert_eq!(r2.payload, b"live2");
}
