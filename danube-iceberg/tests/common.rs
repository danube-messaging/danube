//! # Shared Test Utilities
//!
//! This module provides helper functions used by all `danube-iceberg` integration
//! tests. It follows the same pattern as `danube-persistent-storage/tests/common.rs`.
//!
//! ## Provided utilities
//!
//! - **Message construction**: `make_test_message`, `make_json_message`,
//!   `make_rich_message` — create `StreamMessage` values with controlled fields
//! - **Serialization**: `serialize_message` — bincode-encode a `StreamMessage`
//!   (same format the WAL uses in production)
//! - **Segment construction**: `build_test_segment`, `build_segment_from_messages`
//!   — produce synthetic `.dnb1` segment byte arrays with valid WAL frame headers
//! - **Storage**: `build_local_storage`, `write_segment_to_storage` — create
//!   isolated `LocalFileSystem` object stores and write segments at the expected
//!   Danube path layout
//!
//! ## Why a shared module
//!
//! All integration tests need to build synthetic data in the same `.dnb1` format
//! that the broker writes. Centralizing this logic here avoids duplication and
//! ensures consistency across tests — if the frame format changes, only this
//! module needs updating.

use bytes::Bytes;
use danube_core::message::{MessageID, StreamMessage};
use danube_persistent_storage::frames::append_encoded_frame;
use object_store::local::LocalFileSystem;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Minimal re-export of the storage handle from the iceberg crate.
/// Integration tests can't import from `danube_iceberg` directly (it's a binary),
/// so we inline the essential types here.
pub struct TestStorageHandle {
    pub store: Arc<dyn object_store::ObjectStore>,
    pub root_prefix: String,
}

impl TestStorageHandle {
    /// Join the root prefix with a relative path.
    pub fn path(&self, relative: &str) -> object_store::path::Path {
        let key = if self.root_prefix.is_empty() {
            relative.to_string()
        } else {
            format!("{}/{}", self.root_prefix, relative)
        };
        object_store::path::Path::from(key)
    }
}

/// Creates a test `StreamMessage` with a given offset and raw payload bytes.
#[allow(dead_code)]
pub fn make_test_message(offset: u64, payload: &[u8]) -> StreamMessage {
    StreamMessage {
        request_id: offset,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: "default/test-topic".to_string(),
            broker_addr: "localhost:6650".to_string(),
            topic_offset: offset,
        },
        payload: Bytes::copy_from_slice(payload),
        publish_time: 1720000000 + offset,
        producer_name: "test-producer-1".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
        schema_id: None,
        schema_version: None,
        routing_key: None,
    }
}

/// Creates a test `StreamMessage` with a JSON object payload.
#[allow(dead_code)]
pub fn make_json_message(offset: u64, json: &serde_json::Value) -> StreamMessage {
    let payload = serde_json::to_vec(json).expect("serialize json");
    let mut msg = make_test_message(offset, &payload);
    // Add some attributes for testing
    if offset % 3 == 0 {
        msg.attributes
            .insert("source".to_string(), "sensor".to_string());
    }
    msg
}

/// Creates a test `StreamMessage` with a JSON object payload and optional routing_key + schema_id.
#[allow(dead_code)]
pub fn make_rich_message(
    offset: u64,
    payload: &[u8],
    routing_key: Option<&str>,
    schema_id: Option<u64>,
) -> StreamMessage {
    let mut msg = make_test_message(offset, payload);
    msg.routing_key = routing_key.map(|s| s.to_string());
    msg.schema_id = schema_id;
    msg.schema_version = schema_id.map(|id| id as u32);
    msg
}

/// Serialize a `StreamMessage` to bincode bytes (same encoding the WAL uses).
#[allow(dead_code)]
pub fn serialize_message(msg: &StreamMessage) -> Vec<u8> {
    let config = bincode::config::standard();
    bincode::serde::encode_to_vec(msg, config).expect("bincode encode")
}

/// Build a synthetic `.dnb1` segment from a list of (offset, StreamMessage) pairs.
///
/// Encodes each message via bincode, then wraps in WAL frames using
/// `append_encoded_frame` (the same format the broker writes).
#[allow(dead_code)]
pub fn build_test_segment(messages: &[(u64, StreamMessage)]) -> Vec<u8> {
    let mut segment = Vec::new();
    for (offset, msg) in messages {
        let payload = serialize_message(msg);
        append_encoded_frame(&mut segment, *offset, &payload);
    }
    segment
}

/// Convenience: build a segment from messages using their natural offsets.
#[allow(dead_code)]
pub fn build_segment_from_messages(messages: &[StreamMessage]) -> Vec<u8> {
    let pairs: Vec<(u64, StreamMessage)> = messages
        .iter()
        .map(|m| (m.msg_id.topic_offset, m.clone()))
        .collect();
    build_test_segment(&pairs)
}

/// Create a local filesystem `TestStorageHandle` in the given directory.
#[allow(dead_code)]
pub fn build_local_storage(root: &Path) -> TestStorageHandle {
    std::fs::create_dir_all(root).expect("create storage dir");
    let store = LocalFileSystem::new_with_prefix(root).expect("local fs");
    TestStorageHandle {
        store: Arc::new(store),
        root_prefix: String::new(),
    }
}

/// Write raw bytes as a segment object at the expected path.
///
/// Path: `storage/topics/{topic_path}/segments/{segment_id}`
#[allow(dead_code)]
pub async fn write_segment_to_storage(
    storage: &TestStorageHandle,
    topic_path: &str,
    segment_id: &str,
    data: &[u8],
) {
    let relative = format!("storage/topics/{}/segments/{}", topic_path, segment_id);
    let path = storage.path(&relative);
    let payload = object_store::PutPayload::from(Bytes::copy_from_slice(data));
    storage
        .store
        .put(&path, payload)
        .await
        .expect("write segment");
}
