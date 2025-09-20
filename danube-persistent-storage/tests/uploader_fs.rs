//! Test: uploader_writes_object_and_manifest_fs
//!
//! Purpose
//! - Validate that the Uploader writes a batched object to the local filesystem backend
//!   (opendal `fs`) and records a per-object descriptor in the metadata store.
//! - Ensure file is materialized under the expected relative path rooted at the opendal FS root.
//!
//! Inputs
//! - Appends three messages (offsets 0..2) into a WAL (in-memory cache, no file backing required for the test).
//! - Cloud backend: `LocalBackend::Fs` with a temporary directory as root.
//! - Metadata: in-memory `MemoryStore` wrapped by `EtcdMetadata` (strict etcd-style writer).
//!
//! Expected Behavior
//! - After one uploader tick:
//!   - Descriptor exists in metadata under `/danube/storage/topics/<topic>/objects/<start_offset_padded>`.
//!   - A file exists on disk at `<tmp>/storage/topics/<topic>/objects/<object_id>` and is non-empty.
//!   - Descriptor has `completed = true`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use danube_core::message::{MessageID, StreamMessage};
use danube_metadata_store::{MemoryStore, MetadataStorage, MetadataStore};
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::{
    BackendConfig, CloudStore, EtcdMetadata, LocalBackend, Uploader, UploaderConfig,
};
use tempfile::TempDir;

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
        payload: format!("fs-hello-{}", i).into_bytes(),
        publish_time: 0,
        producer_name: "producer-fs".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
    }
}

#[tokio::test]
async fn uploader_writes_object_and_manifest_fs() {
    // Temp directory for local FS backend
    let tmp: TempDir = TempDir::new().expect("tmpdir");
    let fs_root: PathBuf = tmp.path().to_path_buf();

    // WAL with small cache and no file persistence for the test
    let wal = Wal::with_config(WalConfig {
        cache_capacity: Some(128),
        ..Default::default()
    })
    .await
    .expect("wal init");

    let topic_path = "tenant/ns/topic-fs";

    // Append a few messages
    for i in 0..3u64 {
        let m = make_msg(i, topic_path);
        wal.append(&m).await.expect("append");
    }

    // CloudStore using fs backend rooted at tmp dir
    let cloud = CloudStore::new(BackendConfig::Local {
        backend: LocalBackend::Fs,
        root: fs_root.to_string_lossy().to_string(),
    })
    .expect("cloud store fs");

    // In-memory metadata store
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

    // Discover descriptor key via in-memory store
    let prefix = format!("/danube/storage/topics/{}/objects", topic_path);
    let mut children = mem_read.get_childrens(&prefix).await.expect("children");
    children.retain(|c| c != "cur" && !c.ends_with('/'));
    assert!(!children.is_empty(), "descriptor keys should exist");
    children.sort();
    let key = children[0].clone();
    let val = mem_read
        .get(&key, danube_metadata_store::MetaOptions::None)
        .await
        .expect("get desc")
        .expect("desc present");
    let desc: danube_persistent_storage::ObjectDescriptor =
        serde_json::from_value(val).expect("parse desc");

    // Validate object exists on local filesystem
    let object_rel = format!("storage/topics/{}/objects/{}", topic_path, desc.object_id);
    let object_abs = fs_root.join(object_rel);
    let data = std::fs::read(&object_abs).expect("read uploaded object from fs");
    assert!(data.len() > 0, "uploaded fs data should not be empty");
    assert!(desc.completed, "descriptor should be completed");
}
