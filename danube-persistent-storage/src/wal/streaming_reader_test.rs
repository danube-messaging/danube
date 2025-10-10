#[cfg(test)]
mod tests {
    use crate::checkpoint::WalCheckpoint;
    use crate::wal::streaming_reader::stream_from_wal_files;
    use crc32fast;
    use danube_core::message::{MessageID, StreamMessage};
    use danube_core::storage::PersistentStorageError;
    use std::collections::HashMap;
    use tempfile::TempDir;
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;
    use tokio::time::{timeout, Duration};
    use tokio_stream::StreamExt;

    fn make_message(i: u64) -> StreamMessage {
        StreamMessage {
            request_id: i,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test".to_string(),
                broker_addr: "localhost:6650".to_string(),
                topic_offset: i,
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
    /// Purpose
    /// - Validate that `stream_from_wal_files` can read a single WAL file end-to-end
    ///   and yield frames in order with correct `topic_offset`.
    ///
    /// Flow
    /// - Create a single WAL file and write frames for offsets 0, 1, 2.
    /// - Build a `WalCheckpoint` referencing this file.
    /// - Call `stream_from_wal_files(ckpt, 0, chunk)` and collect outputs.
    ///
    /// Expected
    /// - The stream yields offsets [0, 1, 2] exactly once, in order.
    #[tokio::test]
    async fn test_streaming_reader_single_file_basic() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let wal_path = tmp.path().join("test.wal");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&wal_path)
            .await?;
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
            active_file_first_offset: Some(0),
        };

        let mut stream = stream_from_wal_files(&ckpt, 0, 4 * 1024 * 1024).await?;
        let mut offs = vec![];
        while let Some(item) = stream.next().await {
            let m = item?;
            offs.push(m.msg_id.topic_offset);
            if offs.len() == 3 {
                break;
            }
        }
        assert_eq!(offs, vec![0, 1, 2]);
        Ok(())
    }

    /// Test: Streaming reader - rotated files continuity
    ///
    /// Purpose
    /// - Validate continuity across rotated files: older frames in a rotated file and newer frames in
    ///   the active file are read seamlessly in order.
    ///
    /// Flow
    /// - Create two files: file0 with offsets 0..=2 and file1 with offsets 3..=5.
    /// - Build `WalCheckpoint` with file0 in `rotated_files` and file1 as the active `file_path`.
    /// - Call `stream_from_wal_files(ckpt, 0, chunk)`.
    ///
    /// Expected
    /// - The stream yields offsets [0, 1, 2, 3, 4, 5] in order.
    #[tokio::test]
    async fn test_streaming_reader_rotated_files_continuity(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let file0 = tmp.path().join("wal.000");
        let file1 = tmp.path().join("wal.active");

        // Write 0..=2 in file0
        let mut f0 = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file0)
            .await?;
        for i in 0..3u64 {
            write_wal_frame(&mut f0, i, &make_message(i)).await?;
        }
        drop(f0);

        // Write 3..=5 in file1
        let mut f1 = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file1)
            .await?;
        for i in 3..6u64 {
            write_wal_frame(&mut f1, i, &make_message(i)).await?;
        }
        drop(f1);

        let ckpt = WalCheckpoint {
            start_offset: 0,
            last_offset: 5,
            file_seq: 1,
            file_path: file1.to_string_lossy().into_owned(),
            rotated_files: vec![(0, file0.clone(), 0)],
            active_file_name: None,
            last_rotation_at: None,
            active_file_first_offset: Some(3),
        };

        let mut stream = stream_from_wal_files(&ckpt, 0, 10 * 1024 * 1024).await?;
        let mut offs = Vec::new();
        while let Some(item) = timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timeout")?
        {
            let m = item?;
            offs.push(m.msg_id.topic_offset);
            if offs.len() == 6 {
                break;
            }
        }
        assert_eq!(offs, vec![0, 1, 2, 3, 4, 5]);
        Ok(())
    }

    /// Test: Streaming reader - CRC mismatch stops before corrupt frame
    ///
    /// Purpose
    /// - Ensure the reader stops before a corrupt frame and does not yield invalid data.
    ///
    /// Flow
    /// - Write a valid frame at offset 0 and a corrupt frame (wrong CRC) at offset 1.
    /// - Build `WalCheckpoint` over the file and read from 0.
    ///
    /// Expected
    /// - The stream yields only offset 0 and then terminates or returns no further valid frames.
    #[tokio::test]
    async fn test_streaming_reader_crc_mismatch_stops() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let wal_path = tmp.path().join("test_crc.wal");

        // Good frame at 0
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&wal_path)
            .await?;
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
            active_file_first_offset: Some(0),
        };

        let mut stream = stream_from_wal_files(&ckpt, 0, 10 * 1024 * 1024).await?;
        let first = timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timeout msg0")?
            .unwrap()?;
        assert_eq!(first.msg_id.topic_offset, 0);

        // Next should be None or error due to CRC mismatch; accept None as success
        let next = timeout(Duration::from_millis(500), stream.next()).await;
        if let Ok(Some(Ok(m))) = next {
            panic!("unexpected extra message: {}", m.msg_id.topic_offset);
        }
        Ok(())
    }
}
