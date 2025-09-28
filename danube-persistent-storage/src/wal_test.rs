#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use danube_core::message::{MessageID, StreamMessage};
    use danube_core::storage::PersistentStorageError;
    use crate::wal::{Wal, WalConfig};
    use tempfile::TempDir;
    use tokio_stream::StreamExt;

    fn make_message(i: u64) -> StreamMessage {
        StreamMessage {
            request_id: i,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "t".to_string(),
                broker_addr: "b".to_string(),
                segment_id: 0,
                segment_offset: 0,
            },
            payload: format!("msg-{i}").into_bytes(),
            publish_time: i,
            producer_name: "p".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
        }
    }

    async fn make_wal_with_dir(
        dir: &PathBuf,
        rotate_max_bytes: Option<u64>,
    ) -> Result<Wal, PersistentStorageError> {
        let cfg = WalConfig {
            dir: Some(dir.clone()),
            file_name: Some("wal.log".to_string()),
            cache_capacity: Some(1024),
            fsync_interval_ms: Some(1),
            fsync_max_batch_bytes: Some(1024),
            rotate_max_bytes,
            rotate_max_seconds: None,
        };
        Wal::with_config(cfg).await
    }

    /// Test: WAL file replay (fresh reader over existing WAL directory)
    ///
    /// Purpose
    /// - Validate that frames appended and flushed by a writer can be replayed by a fresh
    ///   WAL instance pointing to the same directory.
    /// - Ensures correctness of persisted on-disk format and the reader replay path.
    ///
    /// Flow
    /// - Create a writer WAL, append 3 messages, shutdown (flush to disk).
    /// - Create a new reader WAL over the same directory and call `tail_reader(0)`.
    ///
    /// Expected
    /// - The reader yields exactly the 3 payloads in order: msg-0, msg-1, msg-2, then tails live.
    #[tokio::test]
    async fn test_wal_file_replay_all() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let writer = make_wal_with_dir(&dir, None).await?;
        for i in 0..3u64 {
            writer.append(&make_message(i)).await?;
        }
        writer.shutdown().await;

        // New reader instance
        let reader = make_wal_with_dir(&dir, None).await?;
        let mut stream = reader.tail_reader(0).await?;
        let mut got = Vec::new();
        while let Some(item) = stream.next().await {
            let msg = item?;
            got.push(msg.payload);
            if got.len() == 3 {
                break;
            }
        }
        assert_eq!(
            got,
            vec![b"msg-0".to_vec(), b"msg-1".to_vec(), b"msg-2".to_vec()]
        );
        Ok(())
    }

    /// Test: WAL file replay from an offset
    ///
    /// Purpose
    /// - Validate that `tail_reader(from_offset)` replays only persisted/cached messages with
    ///   offsets >= `from_offset` before switching to live tailing.
    ///
    /// Flow
    /// - Create writer WAL, append 5 messages, shutdown (flush).
    /// - Create reader WAL over same directory, call `tail_reader(3)`.
    ///
    /// Expected
    /// - The first two items from the stream are payloads for offsets 3 and 4: msg-3, msg-4.
    ///   Afterwards the stream would switch to live (not asserted here).
    #[tokio::test]
    async fn test_wal_file_replay_from_offset() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let writer = make_wal_with_dir(&dir, None).await?;
        for i in 0..5u64 {
            writer.append(&make_message(i)).await?;
        }
        writer.shutdown().await;

        let reader = make_wal_with_dir(&dir, None).await?;
        let mut stream = reader.tail_reader(3).await?;
        let first = stream.next().await.unwrap()?;
        let second = stream.next().await.unwrap()?;
        assert_eq!(first.payload, b"msg-3".to_vec());
        assert_eq!(second.payload, b"msg-4".to_vec());
        Ok(())
    }

    /// Test: Append N messages and tail from offset 0
    ///
    /// Purpose
    /// - Validate end-to-end in-memory cache + file path by appending N messages
    ///   and reading them in order from offset 0.
    ///
    /// Flow
    /// - Create a WAL with a temp dir, append 20 messages, shutdown (flush),
    ///   then `tail_reader(0)` and collect exactly 20 messages.
    ///
    /// Expected
    /// - We receive 20 messages; each message payload is msg-i, and the injected
    ///   `msg_id.segment_offset` equals i (the WAL offset assigned during append).
    #[tokio::test]
    async fn test_append_and_tail_from_zero() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let wal = make_wal_with_dir(&dir, None).await?;

        let n = 20u64;
        for i in 0..n {
            let m = make_message(i);
            wal.append(&m).await?;
        }

        // Ensure data is flushed
        wal.shutdown().await;

        // Read from offset 0 and collect all
        let mut stream = wal.tail_reader(0).await?;
        let mut got: Vec<StreamMessage> = Vec::new();
        while let Some(item) = stream.next().await {
            let msg = item?;
            got.push(msg);
            if got.len() == n as usize {
                break;
            }
        }

        assert_eq!(got.len(), n as usize);
        for (i, msg) in got.into_iter().enumerate() {
            assert_eq!(msg.payload, format!("msg-{}", i).into_bytes());
            // segment_offset is set by reader to the WAL offset
            assert_eq!(msg.msg_id.segment_offset, i as u64);
        }
        Ok(())
    }

    /// Test: Rotation + ordered reads from a specific offset
    ///
    /// Purpose
    /// - Validate that when rotation occurs (size threshold), `tail_reader(from)` correctly
    ///   stitches replay from file + cache and yields ordered messages starting at `from`.
    ///
    /// Flow
    /// - Configure a small rotate threshold, append 50 messages (trigger rotation), shutdown.
    /// - Call `tail_reader(10)` and collect messages until offset < 25.
    ///
    /// Expected
    /// - We receive exactly 15 messages for offsets [10..25). Payloads match msg-<offset>,
    ///   and `msg_id.segment_offset` equals each expected offset.
    #[tokio::test]
    async fn test_rotation_and_tail_from_offset() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        // Force rotation after small number of bytes
        let wal = make_wal_with_dir(&dir, Some(512)).await?;

        let n = 50u64;
        for i in 0..n {
            let m = make_message(i);
            wal.append(&m).await?;
        }

        wal.shutdown().await;

        // Read from offset 10 and collect next 15
        let mut stream = wal.tail_reader(10).await?;
        let mut got: Vec<StreamMessage> = Vec::new();
        while let Some(item) = stream.next().await {
            let msg = item?;
            if msg.msg_id.segment_offset >= 10 && msg.msg_id.segment_offset < 25 {
                got.push(msg);
            }
            if got.len() == 15 {
                break;
            }
        }

        assert_eq!(got.len(), 15);
        for (idx, msg) in got.into_iter().enumerate() {
            let expected_off = 10 + idx as u64;
            assert_eq!(msg.payload, format!("msg-{}", expected_off).into_bytes());
            assert_eq!(msg.msg_id.segment_offset, expected_off);
        }

        Ok(())
    }
}
