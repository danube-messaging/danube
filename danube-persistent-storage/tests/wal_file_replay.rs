use danube_core::message::{MessageID, StreamMessage};
use danube_persistent_storage::wal::{Wal, WalConfig};
use tokio_stream::StreamExt;

// This test module validates WAL file-backed replay.
// Why: Readers starting behind the in-memory cache need to replay historical entries from the
// file-backed WAL. We verify that `tail_reader()` returns persisted messages and then tails live.
// Expected: All appended messages are replayed from file for Offset(0), and replay from a given
// offset returns only the newer subset before switching to live tailing.

fn make_msg(
    topic: &str,
    producer_id: u64,
    segment_id: u64,
    segment_offset: u64,
    payload: &[u8],
) -> StreamMessage {
    StreamMessage {
        request_id: producer_id,
        msg_id: MessageID {
            producer_id,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:6650".to_string(),
            segment_id,
            segment_offset,
        },
        payload: payload.to_vec(),
        publish_time: 0,
        producer_name: "test-producer".to_string(),
        subscription_name: None,
        attributes: Default::default(),
    }
}

#[tokio::test]
async fn test_wal_file_replay_all() {
    let tmp = tempfile::tempdir().unwrap();

    // Writer WAL: configure to flush on each append (very small batch)
    let writer = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(16),
        fsync_interval_ms: Some(1),
        max_batch_bytes: Some(1),
        ..Default::default()
    })
    .await
    .expect("create wal writer");

    // Append 3 messages
    for i in 0..3u8 {
        let msg = make_msg("/default/topic", 1, 0, i as u64, &[i]);
        writer.append(&msg).await.expect("append");
    }

    // Reader WAL: new instance pointing to the same file
    let reader = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(16),
        fsync_interval_ms: Some(5),
        max_batch_bytes: Some(4096),
        ..Default::default()
    })
    .await
    .expect("create wal reader");

    let mut stream = reader.tail_reader(0).await.expect("tail reader");
    let mut got = Vec::new();
    for _ in 0..3 {
        let item = stream.next().await.expect("stream item").expect("ok");
        got.push(item.payload);
    }
    // Expected: replay returns all three previously persisted payloads in order
    assert_eq!(got, vec![vec![0u8], vec![1u8], vec![2u8]]);
}

/// E2E: Verify WAL replay-from-offset returns cached messages then tails live
#[tokio::test]
async fn test_wal_file_replay_from_offset() {
    let tmp = tempfile::tempdir().unwrap();

    let writer = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(16),
        fsync_interval_ms: Some(1),
        max_batch_bytes: Some(1),
        ..Default::default()
    })
    .await
    .expect("create wal writer");

    for i in 0..5u8 {
        let msg = make_msg("/default/topic", 1, 0, i as u64, &[i]);
        writer.append(&msg).await.expect("append");
    }

    // Replay starting from offset 3 => expect two cached messages [3], [4]
    let reader = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(16),
        fsync_interval_ms: Some(5),
        max_batch_bytes: Some(4096),
        ..Default::default()
    })
    .await
    .expect("create wal reader");
    let mut stream = reader.tail_reader(3).await.expect("tail reader");

    let first = stream.next().await.expect("item1").expect("ok");
    let second = stream.next().await.expect("item2").expect("ok");
    // Expected: only offsets >= 3 are replayed before live
    assert_eq!(first.payload, vec![3u8]);
    assert_eq!(second.payload, vec![4u8]);
}
