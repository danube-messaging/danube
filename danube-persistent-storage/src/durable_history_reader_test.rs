#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::durable_history_reader::DurableHistoryReader;
    use crate::frames::append_encoded_frame;
    use crate::opendal::OpendalStore;
    use crate::metadata::{SegmentDescriptor, StorageMetadata};
    use crate::{BackendConfig, DurableStore, LocalBackend, OpendalDurableStore};
    use danube_core::message::{MessageID, StreamMessage};
    use danube_core::metadata::{MemoryStore, MetadataStore};
    use futures::TryStreamExt;

    fn make_msg(i: u64, topic: &str) -> StreamMessage {
        StreamMessage {
            request_id: i,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: topic.to_string(),
                broker_addr: "127.0.0.1:8080".to_string(),
                topic_offset: i,
            },
            payload: format!("cloud-{}", i).into_bytes().into(),
            publish_time: 0,
            producer_name: "producer-cloud".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
            schema_id: None,
            schema_version: None,
        }
    }

    fn encode_segment(messages: &[StreamMessage]) -> (Vec<u8>, Vec<(u64, u64)>) {
        let mut bytes = Vec::new();
        let mut index = Vec::new();
        for (msg_index, msg) in messages.iter().enumerate() {
            if msg_index % 1000 == 0 {
                index.push((msg.msg_id.topic_offset, bytes.len() as u64));
            }
            let payload =
                bincode::serde::encode_to_vec(msg, bincode::config::standard()).expect("serialize");
            append_encoded_frame(&mut bytes, msg.msg_id.topic_offset, &payload);
        }
        (bytes, index)
    }

    async fn seed_segment(
        durable_store: Arc<dyn DurableStore>,
        meta: &StorageMetadata,
        topic_path: &str,
        messages: Vec<StreamMessage>,
    ) {
        let (bytes, index) = encode_segment(&messages);
        let start_offset = messages.first().expect("segment start").msg_id.topic_offset;
        let end_offset = messages.last().expect("segment end").msg_id.topic_offset;
        let segment_id = format!("data-{}-seed.dnb1", start_offset);
        let object_path = format!("storage/topics/{}/segments/{}", topic_path, segment_id);
        let object_meta = durable_store
            .put_segment(&object_path, &bytes)
            .await
            .expect("put seeded segment");
        let desc = SegmentDescriptor {
            segment_id,
            start_offset,
            end_offset,
            size: bytes.len() as u64,
            etag: object_meta.etag().map(|etag| etag.to_string()),
            created_at: 1,
            completed: true,
            offset_index: Some(index),
        };
        let start_padded = format!("{:020}", start_offset);
        meta.put_segment_descriptor(topic_path, &start_padded, &desc)
            .await
            .expect("put seeded descriptor");
        meta.put_current_segment(topic_path, &start_padded)
            .await
            .expect("put seeded current segment");
    }

    /// Test: DurableHistoryReader uses sparse offset index for precise ranged reads
    ///
    /// Purpose
    /// - Ensure that with many messages per object, DurableHistoryReader starts near the requested
    ///   range using the sparse offset index written into the durable segment metadata.
    ///
    /// Flow
    /// - Seed a durable segment containing 3000 messages
    /// - Read a narrow range near the tail [2500, 2520] and validate output
    #[tokio::test]
    async fn test_durable_history_reader_sparse_index_seek() {
        let topic_path = "ns/topic-cloud";

        let durable_store = Arc::new(OpendalDurableStore::new(
            OpendalStore::new(BackendConfig::Local {
                backend: LocalBackend::Memory,
                root: "mem-cloud".to_string(),
            })
            .expect("cloud store mem"),
        )) as Arc<dyn DurableStore>;

        let mem = MemoryStore::new().await.expect("memory meta store");
        let mem_arc: Arc<dyn MetadataStore> = Arc::new(mem.clone());
        let meta = StorageMetadata::new(mem_arc, "/danube".to_string());
        let messages = (0..3000u64)
            .map(|i| make_msg(i, topic_path))
            .collect::<Vec<_>>();
        seed_segment(durable_store.clone(), &meta, topic_path, messages).await;

        // Reader should only return requested window
        let reader = DurableHistoryReader::new(
            durable_store.clone(),
            meta.clone(),
            topic_path.to_string(),
        );
        let stream = reader
            .read_range(2500, Some(2520))
            .await
            .expect("cloud read");
        let msgs: Vec<StreamMessage> = stream
            .try_collect::<Vec<StreamMessage>>()
            .await
            .expect("collect");
        assert_eq!(msgs.len(), 21);
        for (i, m) in msgs.into_iter().enumerate() {
            assert_eq!(m.payload, format!("cloud-{}", 2500 + i as u64).into_bytes());
        }
    }

    /// Test: DurableHistoryReader range reads from memory backend
    ///
    /// Purpose
    /// - Validate DurableHistoryReader can read historical messages from durable segments (memory backend)
    /// - Ensure raw WAL frame parsing works correctly for message reconstruction
    /// - Verify range filtering and message ordering from durable storage
    ///
    /// Flow
    /// - Seed one durable segment containing 3 messages (offsets 0-2)
    /// - Use DurableHistoryReader to read range [0, 2] and verify message content
    ///
    /// Expected
    /// - All messages are written to durable storage successfully
    /// - DurableHistoryReader can parse raw frame format and reconstruct original messages
    /// - Messages are returned in correct offset order with proper content
    #[tokio::test]
    async fn test_durable_history_reader_range_reads_memory() {
        let topic_path = "ns/topic-cloud";

        // OpendalStore memory
        let durable_store = Arc::new(OpendalDurableStore::new(
            OpendalStore::new(BackendConfig::Local {
                backend: LocalBackend::Memory,
                root: "mem-cloud".to_string(),
            })
            .expect("cloud store mem"),
        )) as Arc<dyn DurableStore>;

        // Metadata store
        let mem = MemoryStore::new().await.expect("memory meta store");
        let mem_arc: Arc<dyn MetadataStore> = Arc::new(mem.clone());
        let meta = StorageMetadata::new(mem_arc, "/danube".to_string());
        let messages = (0..3u64).map(|i| make_msg(i, topic_path)).collect::<Vec<_>>();
        seed_segment(durable_store.clone(), &meta, topic_path, messages).await;

        // DurableHistoryReader
        let reader = DurableHistoryReader::new(
            durable_store.clone(),
            meta.clone(),
            topic_path.to_string(),
        );
        let stream = reader.read_range(0, Some(2)).await.expect("cloud read");
        let msgs: Vec<StreamMessage> = stream
            .try_collect::<Vec<StreamMessage>>()
            .await
            .expect("try_collect");

        assert_eq!(msgs.len(), 3, "should read 3 messages from durable segments");
        for (i, m) in msgs.into_iter().enumerate() {
            assert_eq!(m.payload, format!("cloud-{}", i).into_bytes());
        }
    }
}
