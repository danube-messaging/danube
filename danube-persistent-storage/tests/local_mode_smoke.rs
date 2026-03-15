use danube_core::message::{MessageID, StreamMessage};
use danube_core::metadata::{MemoryStore, MetadataStore};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{StorageFactory, StorageFactoryConfig};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;

fn make_message(topic_name: &str, topic_offset: u64) -> StreamMessage {
    StreamMessage {
        request_id: topic_offset,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic_name.to_string(),
            broker_addr: "localhost:6650".to_string(),
            topic_offset,
        },
        payload: format!("msg-{}", topic_offset).into_bytes().into(),
        publish_time: topic_offset,
        producer_name: "producer-1".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
        schema_id: None,
        schema_version: None,
    }
}

#[tokio::test]
async fn local_mode_append_read_seal_reload_continuity() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let memory_store = Arc::new(MemoryStore::new().await.expect("memory store"));
    let metadata_store: Arc<dyn MetadataStore> = memory_store.clone();

    let factory = StorageFactory::new(
        StorageFactoryConfig::local(
            WalConfig {
                dir: Some(tmp.path().to_path_buf()),
                fsync_interval_ms: Some(5_000),
                ..Default::default()
            },
            "/danube",
        ),
        metadata_store,
    );

    let topic = "/default/local-smoke";
    let storage = factory.for_topic(topic).await.expect("create storage");

    for offset in 0..3u64 {
        let assigned = storage
            .append_message(topic, make_message(topic, offset))
            .await
            .expect("append message");
        assert_eq!(assigned, offset);
    }

    let commit_info = factory.commit_info(topic).await.expect("commit info");
    assert_eq!(commit_info.last_committed_offset, 2);

    let mut reader = storage
        .create_reader(topic, StartPosition::Offset(0))
        .await
        .expect("create reader");
    for expected in 0..3u64 {
        let msg = reader
            .next()
            .await
            .expect("reader item")
            .expect("reader result");
        assert_eq!(msg.payload.as_ref(), format!("msg-{}", expected).as_bytes());
        assert_eq!(msg.msg_id.topic_offset, expected);
    }

    let seal_info = factory.seal(topic, 7).await.expect("seal");
    assert_eq!(seal_info.last_committed_offset, 2);

    let storage = factory.for_topic(topic).await.expect("recreate storage");
    let assigned = storage
        .append_message(topic, make_message(topic, 3))
        .await
        .expect("append after reload");
    assert_eq!(assigned, 3);

    let mut reader = storage
        .create_reader(topic, StartPosition::Offset(0))
        .await
        .expect("create reader after reload");
    for expected in 0..4u64 {
        let msg = reader
            .next()
            .await
            .expect("reader item after reload")
            .expect("reader result after reload");
        assert_eq!(msg.payload.as_ref(), format!("msg-{}", expected).as_bytes());
        assert_eq!(msg.msg_id.topic_offset, expected);
    }
}
