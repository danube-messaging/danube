//! Test: uploader_writes_object_and_manifest_memory
//!
//! Purpose
//! - Validate that the Uploader writes a batched object to the configured cloud backend
//!   (opendal `memory`) and records a per-object descriptor in the metadata store.
//! - Ensure key conventions and namespaces are respected: `storage/topics/<topic>/objects/<object_id>`.
//!
//! Inputs
//! - Appends three messages (offsets 0..2) into an in-memory WAL.
//! - Cloud backend: `LocalBackend::Memory` with root prefix (namespacing).
//! - Metadata: in-memory `MemoryStore` wrapped by `EtcdMetadata` (strict etcd-style writer).
//!
//! Expected Behavior
//! - After one uploader tick:
//!   - At least one object exists in the cloud backend under the expected namespace.
//!   - Metadata store contains a descriptor keyed by zero-padded `start_offset`.
//!   - Descriptor fields reflect the uploaded object (includes `completed = true`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use danube_core::message::{MessageID, StreamMessage};
use danube_metadata_store::{MemoryStore, MetadataStorage, MetadataStore};
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::{
    BackendConfig, CloudStore, EtcdMetadata, LocalBackend, Uploader, UploaderConfig,
};

fn make_msg(i: u64, topic: &str) -> StreamMessage {
    StreamMessage {
        request_id: i,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:8080".to_string(),
            segment_id: 0,
            segment_offset: i,
        },
        payload: format!("hello-{}", i).into_bytes(),
        publish_time: 0,
        producer_name: "producer-1".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
    }
}

#[tokio::test]
async fn uploader_writes_object_and_manifest_memory() {
    // WAL with small cache and no file persistence for the test
    let wal = Wal::with_config(WalConfig {
        cache_capacity: Some(128),
        ..Default::default()
    })
    .await
    .expect("wal init");

    let topic_path = "tenant/ns/topic-a";

    // Append a few messages
    let m0 = make_msg(0, topic_path);
    let m1 = make_msg(1, topic_path);
    let m2 = make_msg(2, topic_path);
    wal.append(&m0).await.expect("append m0");
    wal.append(&m1).await.expect("append m1");
    wal.append(&m2).await.expect("append m2");

    // CloudStore using in-memory backend
    let cloud = CloudStore::new(BackendConfig::Local {
        backend: LocalBackend::Memory,
        root: "mem-prefix".to_string(),
    })
    .expect("cloud store");

    // In-memory metadata store (keep a clone for direct reads)
    let mem = MemoryStore::new().await.expect("memory meta store");
    let mem_read = mem.clone();
    let meta = EtcdMetadata::new(MetadataStorage::InMemory(mem), "/danube".to_string());

    // Uploader config with short interval
    let up_cfg = UploaderConfig {
        interval_seconds: 1,
        max_batch_bytes: 8 * 1024 * 1024,
        topic_path: topic_path.to_string(),
        root_prefix: "/danube".to_string(),
    };
    let uploader = Arc::new(
        Uploader::new(up_cfg, wal.clone(), cloud.clone(), meta.clone()).expect("uploader"),
    );

    let handle = uploader.clone().start();

    // Wait for one tick to fire
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Stop background task (best-effort)
    handle.abort();

    // Discover the object by scanning in-memory manifest and use its descriptor
    let prefix = format!("/danube/storage/topics/{}/objects", topic_path);
    let mut children = mem_read.get_childrens(&prefix).await.expect("children");
    // Filter out the pointer key and any trailing slashes
    children.retain(|c| c != "cur" && !c.ends_with('/'));
    assert!(
        !children.is_empty(),
        "should contain at least one object descriptor key"
    );
    children.sort();
    let key = children[0].clone(); // get_childrens returns full paths already
    let val = mem_read
        .get(&key, danube_metadata_store::MetaOptions::None)
        .await
        .expect("get desc")
        .expect("desc present");
    let desc: danube_persistent_storage::ObjectDescriptor =
        serde_json::from_value(val).expect("parse desc");

    // Validate object exists in cloud using descriptor's object_id
    let object_path = format!("storage/topics/{}/objects/{}", topic_path, desc.object_id);
    let data = cloud
        .get_object(&object_path)
        .await
        .expect("read object from memory cloud");
    assert!(data.len() > 0, "uploaded data should not be empty");

    assert!(desc.completed, "descriptor should be completed");
}
