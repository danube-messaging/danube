#[cfg(test)]
mod tests {
    use crate::wal::reader::{build_tail_stream, read_file_range};
    use crc32fast;
    use danube_core::message::{MessageID, StreamMessage};
    use danube_core::storage::PersistentStorageError;
    use std::collections::HashMap;
    use tempfile::TempDir;
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::broadcast;
    use tokio_stream::StreamExt;

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

    /// Test: WAL file range reading - complete file
    ///
    /// Purpose
    /// - Validate reading all frames from a WAL file using range query
    /// - Ensure frame deserialization and offset extraction work correctly
    ///
    /// Flow
    /// - Write 3 frames with offsets 0, 1, 2 to WAL file
    /// - Read entire file using range [0, MAX]
    /// - Verify all frames are read in correct order
    ///
    /// Expected
    /// - All 3 frames are read successfully
    /// - Frame offsets and payloads match written data
    /// - No data corruption during file I/O
    #[tokio::test]
    async fn test_read_file_range_all() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let wal_path = tmp.path().join("test.wal");

        // Write 3 frames to file
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&wal_path)
            .await?;

        for i in 0..3u64 {
            write_wal_frame(&mut file, i, &make_message(i)).await?;
        }
        drop(file);

        // Read all frames
        let items = read_file_range(&wal_path, 0, u64::MAX).await?;
        assert_eq!(items.len(), 3);

        for (i, (offset, msg)) in items.into_iter().enumerate() {
            assert_eq!(offset, i as u64);
            assert_eq!(msg.payload, format!("msg-{}", i).into_bytes());
        }

        Ok(())
    }

    /// Test: WAL file range reading - partial range
    ///
    /// Purpose
    /// - Validate reading specific offset ranges from WAL file
    /// - Ensure range filtering works correctly for partial reads
    ///
    /// Flow
    /// - Write 5 frames with offsets 0-4 to WAL file
    /// - Read partial range [1, 3) expecting offsets 1, 2
    /// - Verify only requested frames are returned
    ///
    /// Expected
    /// - Only frames within specified range are read
    /// - Frame order and content are preserved
    /// - Range boundaries are respected (inclusive start, exclusive end)
    #[tokio::test]
    async fn test_read_file_range_partial() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let wal_path = tmp.path().join("test.wal");

        // Write 5 frames to file
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&wal_path)
            .await?;

        for i in 0..5u64 {
            write_wal_frame(&mut file, i, &make_message(i)).await?;
        }
        drop(file);

        // Read frames [1, 3) - should get offsets 1, 2
        let items = read_file_range(&wal_path, 1, 3).await?;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, 1);
        assert_eq!(items[1].0, 2);

        Ok(())
    }

    /// Test: WAL file reading with CRC corruption
    ///
    /// Purpose
    /// - Validate CRC integrity checking during WAL file reading
    /// - Ensure corrupted frames are detected and reading stops safely
    ///
    /// Flow
    /// - Write one valid frame followed by one frame with wrong CRC
    /// - Attempt to read entire file
    /// - Verify reading stops at corrupted frame
    ///
    /// Expected
    /// - Only valid frame before corruption is read
    /// - Reading stops at first CRC mismatch (no crash)
    /// - Data integrity is preserved for valid frames
    #[tokio::test]
    async fn test_read_file_range_crc_mismatch() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let wal_path = tmp.path().join("test.wal");

        // Write one good frame, then corrupt the next one
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&wal_path)
            .await?;

        // Good frame
        write_wal_frame(&mut file, 0, &make_message(0)).await?;

        // Corrupt frame - write wrong CRC
        let msg = make_message(1);
        let bytes = bincode::serialize(&msg).unwrap();
        let len = bytes.len() as u32;
        let wrong_crc = 0xDEADBEEFu32; // Wrong CRC

        file.write_all(&1u64.to_le_bytes()).await?;
        file.write_all(&len.to_le_bytes()).await?;
        file.write_all(&wrong_crc.to_le_bytes()).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        drop(file);

        // Should only read the first frame, stop on CRC mismatch
        let items = read_file_range(&wal_path, 0, u64::MAX).await?;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, 0);

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
        let msg1 = stream.next().await.unwrap()?;
        assert_eq!(msg1.msg_id.segment_offset, 7);
        assert_eq!(msg1.payload, b"msg-7");

        let msg2 = stream.next().await.unwrap()?;
        assert_eq!(msg2.msg_id.segment_offset, 9);
        assert_eq!(msg2.payload, b"msg-9");

        // Send live message
        tx.send((10, make_message(10)))?;
        let msg3 = stream.next().await.unwrap()?;
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

        // Build stream starting from offset 1
        // Should read [1, 2] from file, then [5, 7] from cache
        let mut stream = build_tail_stream(Some(wal_path), cache_snapshot, 1, rx).await?;

        // File replay: offsets 1, 2
        let msg1 = stream.next().await.unwrap()?;
        assert_eq!(msg1.msg_id.segment_offset, 1);

        let msg2 = stream.next().await.unwrap()?;
        assert_eq!(msg2.msg_id.segment_offset, 2);

        // Cache replay: offsets 5, 7
        let msg3 = stream.next().await.unwrap()?;
        assert_eq!(msg3.msg_id.segment_offset, 5);

        let msg4 = stream.next().await.unwrap()?;
        assert_eq!(msg4.msg_id.segment_offset, 7);

        // Live message should start after offset 7
        tx.send((8, make_message(8)))?;
        let msg5 = stream.next().await.unwrap()?;
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
