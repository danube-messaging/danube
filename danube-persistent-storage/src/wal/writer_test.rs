#[cfg(test)]
mod tests {
    use crate::wal::writer::{run, LogCommand, WriterInit};
    use danube_core::message::{MessageID, StreamMessage};
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;
    use tokio::sync::{mpsc, oneshot};

    fn make_test_message(offset: u64) -> StreamMessage {
        StreamMessage {
            request_id: offset,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test".to_string(),
                broker_addr: "localhost:6650".to_string(),
                segment_id: 0,
                segment_offset: offset,
            },
            payload: format!("test message {}", offset).into_bytes(),
            publish_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            producer_name: "test_producer".to_string(),
            subscription_name: None,
            attributes: std::collections::HashMap::new(),
        }
    }

    /// Test: WAL writer initialization
    ///
    /// Purpose
    /// - Validate WriterInit struct creation and configuration
    /// - Ensure writer can be initialized with proper settings
    ///
    /// Flow
    /// - Create temporary directory for WAL files
    /// - Initialize WriterInit with test configuration
    /// - Verify all configuration fields are set correctly
    ///
    /// Expected
    /// - WriterInit struct contains correct paths and settings
    /// - Configuration values match expected test parameters
    /// - No errors during initialization
    #[tokio::test]
    async fn test_writer_init_creation() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let wal_path = tmp.path().join("test.wal");
        let checkpoint_path = tmp.path().join("test.ckpt");

        let writer_init = WriterInit {
            wal_path: Some(wal_path.clone()),
            checkpoint_path: Some(checkpoint_path.clone()),
            fsync_interval_ms: 100,
            fsync_max_batch_bytes: 1024,
            rotate_max_bytes: Some(2048),
            rotate_max_seconds: Some(10),
            ckpt_store: None,
        };

        assert_eq!(writer_init.wal_path, Some(wal_path));
        assert_eq!(writer_init.checkpoint_path, Some(checkpoint_path));
        assert_eq!(writer_init.fsync_interval_ms, 100);
        assert_eq!(writer_init.fsync_max_batch_bytes, 1024);
        assert_eq!(writer_init.rotate_max_bytes, Some(2048));
        assert_eq!(writer_init.rotate_max_seconds, Some(10));

        Ok(())
    }

    /// Test: LogCommand enum variants
    ///
    /// Purpose
    /// - Validate LogCommand enum can be constructed with different variants
    /// - Ensure command types are properly defined for writer communication
    ///
    /// Flow
    /// - Create different LogCommand variants
    /// - Verify each variant can be constructed without errors
    ///
    /// Expected
    /// - All LogCommand variants can be created successfully
    /// - Commands contain expected data types and structures
    #[tokio::test]
    async fn test_log_command_variants() -> Result<(), Box<dyn std::error::Error>> {
        let test_message = make_test_message(1);

        // Test Write command
        let test_bytes = test_message.payload.clone();
        let write_cmd = LogCommand::Write {
            offset: 1,
            bytes: test_bytes.clone(),
        };
        match write_cmd {
            LogCommand::Write { offset, bytes } => {
                assert_eq!(offset, 1);
                assert_eq!(bytes, test_bytes);
            }
            _ => panic!("Expected Write command"),
        }

        // Test Flush command
        let flush_cmd = LogCommand::Flush;
        match flush_cmd {
            LogCommand::Flush => {
                // Successfully created flush command
            }
            _ => panic!("Expected Flush command"),
        }

        // Test Shutdown command
        let (tx, _rx) = oneshot::channel::<()>();
        let shutdown_cmd = LogCommand::Shutdown(tx);
        match shutdown_cmd {
            LogCommand::Shutdown(_) => {
                // Successfully created shutdown command
            }
            _ => panic!("Expected Shutdown command"),
        }

        Ok(())
    }

    // Tests targeting private WriterState internals were removed. We validate behavior
    // via public interfaces (writer::run through channels, frame format, etc.).

    /// Test: Writer task command handling
    ///
    /// Purpose
    /// - Validate the background writer task processes Write, Flush, and Shutdown commands
    /// - Ensure a file is created and contains written data before shutdown
    ///
    /// Flow
    /// - Start writer task with a temp WAL path
    /// - Send Write command with some bytes
    /// - Send Flush command to ensure data is persisted
    /// - Send Shutdown command and await acknowledgment
    /// - Verify the WAL file exists and has non-zero size
    ///
    /// Expected
    /// - Commands are accepted without errors
    /// - File size > 0 indicating data was written
    /// - Writer task terminates gracefully on Shutdown
    #[tokio::test]
    async fn test_writer_task_commands() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let wal_path = tmp.path().join("test.wal");

        let init = WriterInit {
            wal_path: Some(wal_path.clone()),
            checkpoint_path: None,
            fsync_interval_ms: 100,
            fsync_max_batch_bytes: 1024,
            rotate_max_bytes: None,
            rotate_max_seconds: None,
            ckpt_store: None,
        };

        let (cmd_tx, cmd_rx) = mpsc::channel(16);

        // Start writer task
        let writer_handle = tokio::spawn(async move {
            run(init, cmd_rx).await;
        });

        // Send write command
        let test_data = b"test data".to_vec();
        cmd_tx
            .send(LogCommand::Write {
                offset: 0,
                bytes: test_data,
            })
            .await?;

        // Send flush command
        cmd_tx.send(LogCommand::Flush).await?;

        // Send shutdown command
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        cmd_tx.send(LogCommand::Shutdown(shutdown_tx)).await?;

        // Wait for shutdown acknowledgment
        shutdown_rx.await?;

        // Wait for writer task to complete
        writer_handle.await?;

        // Verify file was created and has content
        assert!(tokio::fs::metadata(&wal_path).await.is_ok());
        let file_size = tokio::fs::metadata(&wal_path).await?.len();
        assert!(file_size > 0);

        Ok(())
    }

    /// Test: WAL frame on-disk format
    ///
    /// Purpose
    /// - Validate that the writer produces frames in the expected format:
    ///   [u64 offset][u32 len][u32 crc][bytes]
    /// - Ensure the stored CRC matches the computed CRC of the payload
    ///
    /// Flow
    /// - Start writer task with a temp WAL file
    /// - Send a single Write command for offset 123 and payload "hello"
    /// - Shutdown writer task
    /// - Open the file and sequentially read offset, len, crc, and data
    /// - Compare parsed values to expected and recomputed CRC
    ///
    /// Expected
    /// - Offset read back equals 123
    /// - Length equals payload length
    /// - CRC matches crc32fast::hash(payload)
    /// - Data matches the original payload
    #[tokio::test]
    async fn test_writer_frame_format() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let wal_path = tmp.path().join("test.wal");

        let init = WriterInit {
            wal_path: Some(wal_path.clone()),
            checkpoint_path: None,
            fsync_interval_ms: 10,
            fsync_max_batch_bytes: 64,
            rotate_max_bytes: None,
            rotate_max_seconds: None,
            ckpt_store: None,
        };

        let (cmd_tx, cmd_rx) = mpsc::channel(16);

        // Start writer task
        let writer_handle = tokio::spawn(async move {
            run(init, cmd_rx).await;
        });

        // Send write command
        let test_data = b"hello".to_vec();
        cmd_tx
            .send(LogCommand::Write {
                offset: 123,
                bytes: test_data.clone(),
            })
            .await?;

        // Wait a bit for write to be processed
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send shutdown
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        cmd_tx.send(LogCommand::Shutdown(shutdown_tx)).await?;
        shutdown_rx.await?;
        writer_handle.await?;

        // Read and verify frame format: [u64 offset][u32 len][u32 crc][bytes]
        let mut file = tokio::fs::File::open(&wal_path).await?;

        // Read offset
        let mut offset_bytes = [0u8; 8];
        file.read_exact(&mut offset_bytes).await?;
        let offset = u64::from_le_bytes(offset_bytes);
        assert_eq!(offset, 123);

        // Read length
        let mut len_bytes = [0u8; 4];
        file.read_exact(&mut len_bytes).await?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        assert_eq!(len, test_data.len());

        // Read CRC
        let mut crc_bytes = [0u8; 4];
        file.read_exact(&mut crc_bytes).await?;
        let stored_crc = u32::from_le_bytes(crc_bytes);

        // Read data
        let mut data = vec![0u8; len];
        file.read_exact(&mut data).await?;
        assert_eq!(data, test_data);

        // Verify CRC
        let computed_crc = crc32fast::hash(&data);
        assert_eq!(stored_crc, computed_crc);

        Ok(())
    }
}
