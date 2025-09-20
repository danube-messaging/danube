//! Test: uploader_resume_memory_from_checkpoint
//!
//! Purpose
//! - Validate that the Uploader resumes from `uploader.ckpt` across restarts and does not
//!   re-upload already committed ranges (no duplicate descriptors by start_offset).
//! - Exercise resume logic with opendal `memory` backend and in-memory metadata store.
//!
//! Inputs
//! - WAL configured with a temp directory to persist `uploader.ckpt`.
//! - Phase 1: append offsets 0..2 and run one uploader tick.
//! - Phase 2: append offsets 3..5 and start a new uploader instance (simulating restart).
//!
//! Expected Behavior
//! - After phase 1: at least one descriptor exists and `uploader.ckpt.last_committed_offset >= 2`.
//! - After phase 2: descriptor count is not reduced and descriptor `start_offset`s are unique.

use std::collections::HashMap;
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
        payload: format!("hello-{}", i).into_bytes(),
        publish_time: 0,
        producer_name: "producer-1".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
    }
}

#[tokio::test]
async fn uploader_resume_memory_from_checkpoint() {
    let tmp = TempDir::new().expect("tmpdir");
    let wal_dir = tmp.path().to_path_buf();

    // WAL with on-disk dir to enable uploader.ckpt persistence
    let wal = Wal::with_config(WalConfig {
        dir: Some(wal_dir.clone()),
        cache_capacity: Some(256),
        ..Default::default()
    })
    .await
    .expect("wal init");

    let topic_path = "tenant/ns/topic-memory-resume";

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

    // Phase 1: initial upload
    for i in 0..3u64 {
        wal.append(&make_msg(i, topic_path)).await.expect("append");
    }
    let uploader1 = Arc::new(
        Uploader::new(up_cfg.clone(), wal.clone(), cloud.clone(), meta.clone()).expect("uploader1"),
    );
    let handle1 = uploader1.clone().start();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    handle1.abort();

    // Verify a descriptor exists and checkpoint persisted
    let prefix = format!("/danube/storage/topics/{}/objects", topic_path);
    let mut children1 = mem_read.get_childrens(&prefix).await.expect("children1");
    children1.retain(|c| {
        if let Some(name) = c.rsplit('/').next() {
            name.len() == 20 && name.chars().all(|ch| ch.is_ascii_digit())
        } else {
            false
        }
    });
    assert!(
        !children1.is_empty(),
        "first upload should produce a descriptor"
    );

    let ckpt = wal
        .read_uploader_checkpoint()
        .await
        .expect("read ckpt")
        .expect("uploader.ckpt present after first upload");
    assert!(
        ckpt.last_committed_offset >= 2,
        "checkpoint should cover first batch"
    );

    // Phase 2: restart uploader and append more messages
    for i in 3..6u64 {
        wal.append(&make_msg(i, topic_path)).await.expect("append2");
    }
    let uploader2 = Arc::new(
        Uploader::new(up_cfg.clone(), wal.clone(), cloud.clone(), meta.clone()).expect("uploader2"),
    );
    let handle2 = uploader2.clone().start();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    handle2.abort();

    // Verify we have at least one new descriptor and no duplicates of the first start key
    let mut children2 = mem_read.get_childrens(&prefix).await.expect("children2");
    children2.retain(|c| {
        if let Some(name) = c.rsplit('/').next() {
            name.len() == 20 && name.chars().all(|ch| ch.is_ascii_digit())
        } else {
            false
        }
    });
    children2.sort();
    assert!(
        children2.len() >= children1.len(),
        "descriptor count should not decrease"
    );

    // Fetch descriptors and ensure start_offsets are unique (no duplicate re-uploads)
    use std::collections::HashSet;
    let mut starts = HashSet::new();
    for key in &children2 {
        let val = mem_read
            .get(key, danube_metadata_store::MetaOptions::None)
            .await
            .expect("get desc")
            .expect("desc present");
        let desc: danube_persistent_storage::ObjectDescriptor =
            serde_json::from_value(val).expect("parse desc");
        assert!(
            starts.insert(desc.start_offset),
            "duplicate start_offset found, resume likely re-uploaded a prior range"
        );
    }
}
