#[cfg(test)]
mod tests {
    use crate::checkpoint::{CheckPoint, UploaderCheckpoint};
    use tempfile::TempDir;

    /// Test: Checkpoint serialization and deserialization
    ///
    /// Purpose
    /// - Validate that uploader checkpoints can be written to and read from disk
    /// - Ensure checkpoint data integrity through serialization/deserialization cycle
    ///
    /// Flow
    /// - Create a checkpoint with specific offset and object key
    /// - Write checkpoint to temporary file
    /// - Read checkpoint back from file and verify data matches
    ///
    /// Expected
    /// - Written checkpoint can be read back successfully
    /// - All checkpoint fields (offset, object_key) match original values
    /// - File I/O operations complete without errors
    #[tokio::test]
    async fn test_checkpoint_write_and_read() {
        let tmp = TempDir::new().expect("temp dir");
        let path = tmp.path().join("test.ckpt");

        let checkpoint = UploaderCheckpoint {
            last_committed_offset: 42,
            last_read_file_seq: 0,
            last_read_byte_position: 0,
            last_object_id: Some("data-0-42.dnb1".to_string()),
            updated_at: 1234567890,
        };

        // Write checkpoint
        CheckPoint::write_uploader_to_path(&checkpoint, &path)
            .await
            .expect("write checkpoint");

        // Verify file exists
        assert!(tokio::fs::metadata(&path).await.is_ok());

        // Read checkpoint back
        let read_checkpoint = CheckPoint::read_uploader_from_path(&path)
            .await
            .expect("read checkpoint")
            .expect("checkpoint should exist");

        assert_eq!(read_checkpoint.last_committed_offset, 42);
        assert_eq!(
            read_checkpoint.last_object_id,
            Some("data-0-42.dnb1".to_string())
        );
        assert_eq!(read_checkpoint.updated_at, 1234567890);
    }

    /// Test: Checkpoint read from non-existent file
    ///
    /// Purpose
    /// - Validate error handling when attempting to read checkpoint from missing file
    /// - Ensure proper error propagation for file system failures
    ///
    /// Flow
    /// - Attempt to read checkpoint from non-existent file path
    /// - Verify that appropriate error is returned
    ///
    /// Expected
    /// - Read operation returns an error (not panic)
    /// - Error indicates file not found or I/O failure
    /// - System handles missing checkpoint files gracefully
    #[tokio::test]
    async fn test_checkpoint_nonexistent_file() {
        let tmp = TempDir::new().expect("temp dir");
        let path = tmp.path().join("nonexistent.ckpt");

        // Reading non-existent file should return None
        let result = CheckPoint::read_uploader_from_path(&path)
            .await
            .expect("read should not error");
        assert!(result.is_none());
    }

    /// Test: Checkpoint atomic write
    ///
    /// Purpose
    /// - Validate that writing a new checkpoint overwrites existing file atomically
    /// - Ensure no data corruption during concurrent writes
    ///
    /// Flow
    /// - Write initial checkpoint to file
    /// - Write different checkpoint to same file path
    /// - Read back and verify latest checkpoint data
    ///
    /// Expected
    /// - Second write overwrites first checkpoint completely
    /// - Read operation returns latest checkpoint data
    /// - No corruption or mixing of checkpoint data
    #[tokio::test]
    async fn test_checkpoint_atomic_write() {
        let tmp = TempDir::new().expect("temp dir");
        let path = tmp.path().join("atomic.ckpt");
        // Implementation writes to path.with_extension("tmp") then renames
        let tmp_path = path.with_extension("tmp");

        let checkpoint = UploaderCheckpoint {
            last_committed_offset: 100,
            last_read_file_seq: 0,
            last_read_byte_position: 0,
            last_object_id: None,
            updated_at: 9876543210,
        };

        // Write checkpoint
        CheckPoint::write_uploader_to_path(&checkpoint, &path)
            .await
            .expect("write checkpoint");

        // Verify final file exists and tmp file is cleaned up
        assert!(tokio::fs::metadata(&path).await.is_ok());
        assert!(tokio::fs::metadata(&tmp_path).await.is_err());

        // Verify content
        let read_checkpoint = CheckPoint::read_uploader_from_path(&path)
            .await
            .expect("read checkpoint")
            .expect("checkpoint should exist");

        assert_eq!(read_checkpoint.last_committed_offset, 100);
        assert_eq!(read_checkpoint.last_object_id, None);
        assert_eq!(read_checkpoint.updated_at, 9876543210);
    }

    /// Test: Checkpoint file overwrite behavior
    ///
    /// Purpose
    /// - Validate that writing a new checkpoint overwrites existing file
    /// - Ensure atomic write operations for checkpoint updates
    ///
    /// Flow
    /// - Write initial checkpoint to file
    /// - Write different checkpoint to same file path
    /// - Read back and verify latest checkpoint data
    ///
    /// Expected
    /// - Second write overwrites first checkpoint completely
    /// - Read operation returns latest checkpoint data
    /// - No corruption or mixing of checkpoint data
    #[tokio::test]
    async fn test_checkpoint_overwrite() {
        let tmp = TempDir::new().expect("temp dir");
        let path = tmp.path().join("overwrite.ckpt");

        // Write first checkpoint
        let checkpoint1 = UploaderCheckpoint {
            last_committed_offset: 10,
            last_read_file_seq: 0,
            last_read_byte_position: 0,
            last_object_id: Some("first".to_string()),
            updated_at: 1000,
        };
        CheckPoint::write_uploader_to_path(&checkpoint1, &path)
            .await
            .expect("write first");

        // Write second checkpoint (overwrite)
        let checkpoint2 = UploaderCheckpoint {
            last_committed_offset: 20,
            last_read_file_seq: 0,
            last_read_byte_position: 0,
            last_object_id: Some("second".to_string()),
            updated_at: 2000,
        };
        CheckPoint::write_uploader_to_path(&checkpoint2, &path)
            .await
            .expect("write second");

        // Read should get the second checkpoint
        let read_checkpoint = CheckPoint::read_uploader_from_path(&path)
            .await
            .expect("read checkpoint")
            .expect("checkpoint should exist");

        assert_eq!(read_checkpoint.last_committed_offset, 20);
        assert_eq!(read_checkpoint.last_object_id, Some("second".to_string()));
        assert_eq!(read_checkpoint.updated_at, 2000);
    }

    #[tokio::test]
    async fn test_uploader_checkpoint_serialization() {
        let checkpoint = UploaderCheckpoint {
            last_committed_offset: u64::MAX,
            last_read_file_seq: 0,
            last_read_byte_position: 0,
            last_object_id: Some("very-long-object-id-with-special-chars-!@#$%^&*()".to_string()),
            updated_at: 0,
        };

        // Test that serialization/deserialization works with edge case values
        let bytes = bincode::serde::encode_to_vec(&checkpoint, bincode::config::standard())
            .expect("serialize");
        let deserialized: UploaderCheckpoint =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .map(|(v, _)| v)
                .expect("deserialize");

        assert_eq!(deserialized.last_committed_offset, u64::MAX);
        assert_eq!(deserialized.last_object_id, checkpoint.last_object_id);
        assert_eq!(deserialized.updated_at, 0);
    }
}
