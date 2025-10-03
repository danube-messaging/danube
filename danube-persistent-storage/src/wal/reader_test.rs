#[cfg(test)]
mod tests {
    use crate::wal::reader::build_tail_stream;
    use crate::wal::streaming_reader::stream_from_wal_files;
    use crate::checkpoint::WalCheckpoint;
    use crc32fast;
    use danube_core::message::{MessageID, StreamMessage};
    use danube_core::storage::PersistentStorageError;
    use std::collections::HashMap;
    use tempfile::TempDir;
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::broadcast;
    use tokio_stream::StreamExt;
    use tokio::time::{timeout, Duration};

    fn make_message(i: u64) -> StreamMessage {
        StreamMessage {
            request_id: i,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test".to_string(),
                broker_addr: "localhost:6650".to_string(),
                segment_id: 0,
                segment_offset: i,
            },
            payload: format!("msg-{}", i).into_bytes(),
            publish_time: i,
            producer_name: "test-producer".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
        }
    }

    async fn write_wal_frame(
        file: &mut tokio::fs::File,
        offset: u64,
        msg: &StreamMessage,
    ) -> Result<(), PersistentStorageError> {
        let bytes = bincode::serialize(msg)
            .map_err(|e| PersistentStorageError::Io(format!("serialize failed: {}", e)))?;
        let len = bytes.len() as u32;
        let crc = crc32fast::hash(&bytes);

        file.write_all(&offset.to_le_bytes())
            .await
            .map_err(|e| PersistentStorageError::Io(format!("write offset failed: {}", e)))?;
        file.write_all(&len.to_le_bytes())
            .await
            .map_err(|e| PersistentStorageError::Io(format!("write len failed: {}", e)))?;
        file.write_all(&crc.to_le_bytes())
            .await
            .map_err(|e| PersistentStorageError::Io(format!("write crc failed: {}", e)))?;
        file.write_all(&bytes)
            .await
            .map_err(|e| PersistentStorageError::Io(format!("write bytes failed: {}", e)))?;
        file.flush()
            .await
            .map_err(|e| PersistentStorageError::Io(format!("flush failed: {}", e)))?;

        Ok(())
    }

    /// Test: Streaming reader - single file basic
    ///
    /// - Writes 3 frames (0,1,2) in one file and streams from offset 0 using streaming_reader
    /// - Verifies all frames are yielded correctly
    #[tokio::test]
    async fn test_streaming_reader_single_file_basic() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let wal_path = tmp.path().join("test.wal");
        let mut file = OpenOptions::new().create(true).write(true).open(&wal_path).await?;
        for i in 0..3u64 {
            write_wal_frame(&mut file, i, &make_message(i)).await?;
        }
        drop(file);

        let ckpt = WalCheckpoint {
            start_offset: 0,
            last_offset: 2,
            file_seq: 0,
            file_path: wal_path.to_string_lossy().into_owned(),
            rotated_files: vec![],
            active_file_name: None,
            last_rotation_at: None,
        };

        let mut stream = stream_from_wal_files(&ckpt, 0, 4 * 1024 * 1024).await?;
        let mut offs = vec![];
        while let Some(item) = stream.next().await {
            let m = item?;
            offs.push(m.msg_id.segment_offset);
            if offs.len() == 3 { break; }
        }
        assert_eq!(offs, vec![0,1,2]);
        Ok(())
    }

    /// Test: Streaming reader - rotated files continuity
    ///
    /// - Create two WAL files: file0 with offsets 0..2, file1 with offsets 3..5
    /// - Put file0 in rotated_files and file1 as active in WalCheckpoint
    /// - Stream from offset 0 and verify we receive 0..5 in order
    #[tokio::test]
    async fn test_streaming_reader_rotated_files_continuity() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let file0 = tmp.path().join("wal.000");
        let file1 = tmp.path().join("wal.active");

        // Write 0..=2 in file0
        let mut f0 = OpenOptions::new().create(true).write(true).open(&file0).await?;
        for i in 0..3u64 { write_wal_frame(&mut f0, i, &make_message(i)).await?; }
        drop(f0);

        // Write 3..=5 in file1
        let mut f1 = OpenOptions::new().create(true).write(true).open(&file1).await?;
        for i in 3..6u64 { write_wal_frame(&mut f1, i, &make_message(i)).await?; }
        drop(f1);

        let ckpt = WalCheckpoint {
            start_offset: 0,
            last_offset: 5,
            file_seq: 1,
            file_path: file1.to_string_lossy().into_owned(),
            rotated_files: vec![(0, file0.clone())],
            active_file_name: None,
            last_rotation_at: None,
        };

        let mut stream = stream_from_wal_files(&ckpt, 0, 10 * 1024 * 1024).await?;
        let mut offs = Vec::new();
        while let Some(item) = timeout(Duration::from_secs(5), stream.next()).await.map_err(|_| "timeout")? {
            let m = item?;
            offs.push(m.msg_id.segment_offset);
            if offs.len() == 6 { break; }
        }
        assert_eq!(offs, vec![0,1,2,3,4,5]);
        Ok(())
    }

    /// Test: Streaming reader - CRC mismatch stops before corrupt frame
    ///
    /// - Write a good frame at 0 and a corrupt (wrong CRC) frame at 1
    /// - Stream from offset 0 and verify only offset 0 is received
    #[tokio::test]
    async fn test_streaming_reader_crc_mismatch_stops() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let wal_path = tmp.path().join("test_crc.wal");

        // Good frame at 0
        let mut file = OpenOptions::new().create(true).write(true).open(&wal_path).await?;
        write_wal_frame(&mut file, 0, &make_message(0)).await?;

        // Corrupt frame at 1 (wrong CRC)
        let msg = make_message(1);
        let bytes = bincode::serialize(&msg).unwrap();
        let len = bytes.len() as u32;
        let wrong_crc = 0xDEADBEEFu32;
        file.write_all(&1u64.to_le_bytes()).await?;
        file.write_all(&len.to_le_bytes()).await?;
        file.write_all(&wrong_crc.to_le_bytes()).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        drop(file);

        let ckpt = WalCheckpoint {
            start_offset: 0,
            last_offset: 1,
            file_seq: 0,
            file_path: wal_path.to_string_lossy().into_owned(),
            rotated_files: vec![],
            active_file_name: None,
            last_rotation_at: None,
        };

        let mut stream = stream_from_wal_files(&ckpt, 0, 10 * 1024 * 1024).await?;
        let first = timeout(Duration::from_secs(5), stream.next()).await.map_err(|_| "timeout msg0")?.unwrap()?;
        assert_eq!(first.msg_id.segment_offset, 0);

        // Next should be None or error due to CRC mismatch; we accept None (no more safe frames)
        let next = timeout(Duration::from_millis(500), stream.next()).await;
        // If timeout elapsed because stream keeps alive but has no more items, that's fine too; treat as success
        if let Ok(Some(Ok(m))) = next { panic!("unexpected extra message: {}", m.msg_id.segment_offset); }
        Ok(())
    }

    /// Test: Tail stream building - cache-only replay
    ///
    /// Purpose
    /// - Validate tail stream construction using only cache data (no file)
    /// - Ensure proper filtering and live message integration
    ///
    /// Flow
    /// - Create cache snapshot with messages at offsets 5, 7, 9
    /// - Build tail stream starting from offset 6
    /// - Send live message at offset 10
    ///
    /// Expected
    /// - Cache messages >= 6 are replayed (offsets 7, 9)
    /// - Live messages are received after cache replay
    /// - Message ordering is preserved across cache and live streams
    #[tokio::test]
    async fn test_build_tail_stream_cache_only() -> Result<(), Box<dyn std::error::Error>> {
        let (tx, rx) = broadcast::channel(16);

        // Cache snapshot with messages at offsets 5, 7, 9
        let cache_snapshot = vec![
            (5, make_message(5)),
            (7, make_message(7)),
            (9, make_message(9)),
        ];

        // Build stream starting from offset 6
        let mut stream = build_tail_stream(None, cache_snapshot, 6, rx).await?;

        // Should get messages at offsets 7, 9 from cache
        let msg1 = timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timeout waiting for msg1")?
            .unwrap()?;
        assert_eq!(msg1.msg_id.segment_offset, 7);
        assert_eq!(msg1.payload, b"msg-7");

        let msg2 = timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timeout waiting for msg2")?
            .unwrap()?;
        assert_eq!(msg2.msg_id.segment_offset, 9);
        assert_eq!(msg2.payload, b"msg-9");

        // Send live message
        tx.send((10, make_message(10)))?;
        let msg3 = timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timeout waiting for msg3")?
            .unwrap()?;
        assert_eq!(msg3.msg_id.segment_offset, 10);
        assert_eq!(msg3.payload, b"msg-10");

        Ok(())
    }

    /// Test: Tail stream building - file + cache + live
    ///
    /// Purpose
    /// - Validate complete tail stream with file replay, cache replay, and live messages
    /// - Ensure seamless transition between different data sources
    ///
    /// Flow
    /// - Write frames 0, 1, 2 to WAL file
    /// - Create cache snapshot with offsets 5, 7
    /// - Build tail stream from offset 1
    /// - Send live message at offset 8
    ///
    /// Expected
    /// - File replay: messages 1, 2
    /// - Cache replay: messages 5, 7
    /// - Live stream: message 8
    /// - All messages delivered in offset order
    #[tokio::test]
    async fn test_build_tail_stream_file_then_cache() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let wal_path = tmp.path().join("test.wal");

        // Write frames 0, 1, 2 to file
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&wal_path)
            .await?;

        for i in 0..3u64 {
            write_wal_frame(&mut file, i, &make_message(i)).await?;
        }
        drop(file);

        let (tx, rx) = broadcast::channel(16);

        // Cache snapshot with messages at offsets 5, 7
        let cache_snapshot = vec![(5, make_message(5)), (7, make_message(7))];

        // Build checkpoint to describe the single wal file
        let ckpt = WalCheckpoint {
            start_offset: 0,
            last_offset: 2,
            file_seq: 0,
            file_path: wal_path.to_string_lossy().into_owned(),
            rotated_files: vec![],
            active_file_name: None,
            last_rotation_at: None,
        };

        // Build stream starting from offset 1
        // Should read [1, 2] from file, then [5, 7] from cache
        let mut stream = build_tail_stream(Some(ckpt), cache_snapshot, 1, rx).await?;

        // File replay: offsets 1, 2
        let msg1 = timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timeout waiting for file msg1")?
            .unwrap()?;
        assert_eq!(msg1.msg_id.segment_offset, 1);

        let msg2 = timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timeout waiting for file msg2")?
            .unwrap()?;
        assert_eq!(msg2.msg_id.segment_offset, 2);

        // Cache replay: offsets 5, 7
        let msg3 = stream.next().await.unwrap()?;
        assert_eq!(msg3.msg_id.segment_offset, 5);

        let msg4 = timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timeout waiting for cache msg4")?
            .unwrap()?;
        assert_eq!(msg4.msg_id.segment_offset, 7);

        // Live message should start after offset 7
        tx.send((8, make_message(8)))?;
        let msg5 = timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timeout waiting for live msg5")?
            .unwrap()?;
        assert_eq!(msg5.msg_id.segment_offset, 8);

        Ok(())
    }

    /// Test: Tail stream live message filtering
    ///
    /// Purpose
    /// - Validate that live messages are filtered by start offset
    /// - Ensure only messages >= start_offset are delivered from live stream
    ///
    /// Flow
    /// - Build tail stream starting from offset 5 (no file, no cache)
    /// - Send live messages with offsets 3, 4, 5, 6
    /// - Verify filtering behavior
    ///
    /// Expected
    /// - Messages with offsets < 5 are filtered out (3, 4)
    /// - Messages with offsets >= 5 are delivered (5, 6)
    /// - Live stream respects start offset boundary
    #[tokio::test]
    async fn test_build_tail_stream_live_filtering() -> Result<(), Box<dyn std::error::Error>> {
        let (tx, rx) = broadcast::channel(16);

        // Empty cache
        let cache_snapshot = vec![];

        // Build stream starting from offset 5
        let mut stream = build_tail_stream(None, cache_snapshot, 5, rx).await?;

        // Send messages with offsets 3, 4, 5, 6
        // Should only receive 5, 6 (>= start_from_live which is 5)
        tx.send((3, make_message(3)))?;
        tx.send((4, make_message(4)))?;
        tx.send((5, make_message(5)))?;
        tx.send((6, make_message(6)))?;

        let msg1 = stream.next().await.unwrap()?;
        assert_eq!(msg1.msg_id.segment_offset, 5);

        let msg2 = stream.next().await.unwrap()?;
        assert_eq!(msg2.msg_id.segment_offset, 6);

        Ok(())
    }
}
