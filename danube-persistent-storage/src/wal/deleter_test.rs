#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use crate::checkpoint::{CheckpointStore, UploaderCheckpoint, WalCheckpoint};
    use crate::wal::deleter::{Deleter, DeleterConfig};

    // Helper to create a small file and return its PathBuf and size
    async fn touch_file(dir: &PathBuf, name: &str, bytes: &[u8]) -> PathBuf {
        let p = dir.join(name);
        tokio::fs::write(&p, bytes).await.expect("write file");
        p
    }

    fn default_uploader_ckpt(seq: u64, committed: u64) -> UploaderCheckpoint {
        UploaderCheckpoint {
            last_committed_offset: committed,
            last_read_file_seq: seq,
            last_read_byte_position: 0,
            last_object_id: None,
            updated_at: 0,
        }
    }

    /// Test: Deleter respects uploader safety watermark
    ///
    /// Purpose
    /// - Ensure the deleter never deletes files at or after the uploader's current file
    ///   (`seq >= last_read_file_seq`).
    ///
    /// Flow
    /// - Create rotated files with seq=1 and seq=2, and an active file seq=3.
    /// - Set uploader watermark `last_read_file_seq=2` (so only seq 1 is strictly older).
    /// - Enable time-based retention to trigger deletion and run one deleter cycle.
    ///
    /// Expected
    /// - Only seq 1 is deleted; seq 2 remains.
    /// - `start_offset` advances to the `first_offset` of the oldest remaining file (seq 2).
    #[tokio::test]
    async fn test_deleter_respects_safety_seq_watermark() {
        // Setup temp directory and checkpoint store
        let tmp = TempDir::new().expect("temp dir");
        let root = tmp.path().to_path_buf();
        let wal_ckpt_path = root.join("wal.ckpt");
        let up_ckpt_path = root.join("uploader.ckpt");
        let store = std::sync::Arc::new(CheckpointStore::new(wal_ckpt_path, up_ckpt_path));

        // Create two rotated files: seq 1 and seq 2, active file seq 3
        let f1 = touch_file(&root, "wal.1.log", b"a").await;
        let f2 = touch_file(&root, "wal.2.log", b"b").await;
        let active = touch_file(&root, "wal.3.log", b"c").await;

        let wal_ckpt = WalCheckpoint {
            start_offset: 0,
            last_offset: 0,
            file_seq: 3,
            file_path: active.to_string_lossy().into_owned(),
            rotated_files: vec![(1, f1.clone(), 0), (2, f2.clone(), 10)],
            active_file_name: Some("wal.3.log".into()),
            last_rotation_at: None,
            active_file_first_offset: Some(20),
        };
        store.update_wal(&wal_ckpt).await.expect("update wal");

        // Set uploader watermark to seq=2 (meaning seq < 2 can be deleted; seq 2 cannot)
        let up = default_uploader_ckpt(2, 100);
        store.update_uploader(&up).await.expect("update up");

        // Configure deleter with time-based retention (0 minutes) to trigger deletion of eligible files
        let cfg = DeleterConfig {
            check_interval_minutes: 5,
            retention_time_minutes: Some(0),
            retention_size_mb: None,
        };
        let deleter = std::sync::Arc::new(Deleter::new("ns/topic".into(), store.clone(), cfg));

        // Run cycle: only seq 1 is eligible (seq < uploader.last_read_file_seq)
        deleter.run_cycle().await.expect("run cycle");

        // Reload wal checkpoint and assert that seq 1 was removed, seq 2 remains
        let wal_after = store.get_wal().await.expect("wal after");
        assert_eq!(wal_after.rotated_files.len(), 1);
        assert_eq!(wal_after.rotated_files[0].0, 2);
        assert_eq!(
            wal_after.start_offset, 10,
            "start_offset should advance to first_offset of seq 2"
        );
    }

    /// Test: Deleter size-based pruning removes rotated files deterministically
    ///
    /// Purpose
    /// - Validate size-based retention deletes oldest rotated files until the total
    ///   rotated size is within the configured limit.
    ///
    /// Flow
    /// - Create rotated files seq 1 (2 bytes) and seq 2 (3 bytes); active seq 3.
    /// - Set uploader watermark beyond both files to make them eligible.
    /// - Configure size limit to 0 MB (0 bytes) to force pruning of all rotated files.
    /// - Run one deleter cycle.
    ///
    /// Expected
    /// - All rotated files are deleted.
    /// - `start_offset` falls back to `active_file_first_offset` when no rotated files remain.
    #[tokio::test]
    async fn test_deleter_size_based_prunes_oldest_until_under_limit() {
        let tmp = TempDir::new().expect("temp dir");
        let root = tmp.path().to_path_buf();
        let store = std::sync::Arc::new(CheckpointStore::new(
            root.join("wal.ckpt"),
            root.join("uploader.ckpt"),
        ));

        // Rotated: seq 1 (2 bytes), seq 2 (3 bytes), active seq 3
        let f1 = touch_file(&root, "wal.1.log", b"aa").await; // 2 bytes
        let f2 = touch_file(&root, "wal.2.log", b"bbb").await; // 3 bytes
        let active = touch_file(&root, "wal.3.log", b"cccc").await;

        store
            .update_wal(&WalCheckpoint {
                start_offset: 0,
                last_offset: 0,
                file_seq: 3,
                file_path: active.to_string_lossy().into_owned(),
                rotated_files: vec![(1, f1.clone(), 0), (2, f2.clone(), 5)],
                active_file_name: Some("wal.3.log".into()),
                last_rotation_at: None,
                active_file_first_offset: Some(8),
            })
            .await
            .expect("update wal");
        // Uploader watermark beyond both rotated files
        store
            .update_uploader(&default_uploader_ckpt(99, 999))
            .await
            .expect("up");

        // Force pruning of all rotated files by setting retention_size_mb to 0 MB (0 bytes).
        let cfg = DeleterConfig {
            check_interval_minutes: 5,
            retention_time_minutes: None,
            retention_size_mb: Some(0),
        };
        let deleter = std::sync::Arc::new(Deleter::new("ns/topic".into(), store.clone(), cfg));
        deleter.run_cycle().await.expect("run");

        let after = store.get_wal().await.expect("wal after");
        assert_eq!(
            after.rotated_files.len(),
            0,
            "all rotated files pruned due to size limit 0 MB"
        );
        assert_eq!(
            after.start_offset, 8,
            "falls back to active_file_first_offset when no rotated files remain"
        );
    }

    /// Test: Deleter time-based pruning removes rotated files older than threshold
    ///
    /// Purpose
    /// - Validate that time-based retention prunes files whose age exceeds the threshold.
    ///
    /// Flow
    /// - Create a rotated file (seq 1) and an active file (seq 2).
    /// - Set uploader watermark to make the rotated file eligible.
    /// - Configure `retention_time_minutes = 0` so any candidate is considered old.
    /// - Run one deleter cycle.
    ///
    /// Expected
    /// - Rotated files are deleted.
    /// - `start_offset` falls back to the active file's first offset when no rotated remain.
    #[tokio::test]
    async fn test_deleter_time_based_prunes_when_age_exceeds() {
        let tmp = TempDir::new().expect("temp dir");
        let root = tmp.path().to_path_buf();
        let store = std::sync::Arc::new(CheckpointStore::new(
            root.join("wal.ckpt"),
            root.join("uploader.ckpt"),
        ));

        let f1 = touch_file(&root, "wal.1.log", b"x").await;
        let active = touch_file(&root, "wal.2.log", b"y").await;

        store
            .update_wal(&WalCheckpoint {
                start_offset: 0,
                last_offset: 0,
                file_seq: 2,
                file_path: active.to_string_lossy().into_owned(),
                rotated_files: vec![(1, f1.clone(), 0)],
                active_file_name: Some("wal.2.log".into()),
                last_rotation_at: None,
                active_file_first_offset: Some(10),
            })
            .await
            .expect("wal");
        store
            .update_uploader(&default_uploader_ckpt(99, 999))
            .await
            .expect("up");

        // Set retention_time_minutes = 0 to treat any file as older than threshold
        let cfg = DeleterConfig {
            check_interval_minutes: 5,
            retention_time_minutes: Some(0),
            retention_size_mb: None,
        };
        let deleter = std::sync::Arc::new(Deleter::new("ns/topic".into(), store.clone(), cfg));
        deleter.run_cycle().await.expect("run");

        let after = store.get_wal().await.expect("after");
        assert!(
            after.rotated_files.is_empty(),
            "rotated should be pruned by time"
        );
        assert_eq!(
            after.start_offset, 10,
            "start_offset uses active file when no rotated"
        );
    }

    /// Test: Deleter handles file system errors gracefully (missing file)
    ///
    /// Purpose
    /// - Validate that if a rotated file is missing on disk, the deleter does not crash and does not corrupt checkpoints.
    ///
    /// Flow
    /// - Create a WAL checkpoint that references a rotated file path that does not exist.
    /// - Set uploader watermark and an aggressive time-based retention to attempt deletion.
    /// - Run one deleter cycle.
    ///
    /// Expected
    /// - Checkpoint remains unchanged (rotated file still referenced) since deletion failed.
    /// - No panic occurs; the operation completes with Ok(()) and logs an error internally.
    #[tokio::test]
    async fn test_deleter_handles_missing_file_gracefully() {
        let tmp = TempDir::new().expect("temp dir");
        let root = tmp.path().to_path_buf();
        let store = std::sync::Arc::new(CheckpointStore::new(
            root.join("wal.ckpt"),
            root.join("uploader.ckpt"),
        ));

        // Reference a non-existent rotated file (seq 1)
        let missing_path = root.join("wal.1.log");
        let active = touch_file(&root, "wal.2.log", b"active").await;

        store
            .update_wal(&WalCheckpoint {
                start_offset: 0,
                last_offset: 0,
                file_seq: 2,
                file_path: active.to_string_lossy().into_owned(),
                rotated_files: vec![(1, missing_path.clone(), 0)],
                active_file_name: Some("wal.2.log".into()),
                last_rotation_at: None,
                active_file_first_offset: Some(10),
            })
            .await
            .expect("wal");
        // Watermark allows deletion of seq 1
        store
            .update_uploader(&default_uploader_ckpt(99, 999))
            .await
            .expect("up");

        let cfg = DeleterConfig {
            check_interval_minutes: 5,
            retention_time_minutes: Some(0),
            retention_size_mb: None,
        };
        let deleter = std::sync::Arc::new(Deleter::new("ns/topic".into(), store.clone(), cfg));
        // Should not panic; deletion of missing file should be handled gracefully
        deleter.run_cycle().await.expect("run");

        // Rotated file remains referenced since deletion failed; start_offset unchanged
        let after = store.get_wal().await.expect("after");
        assert_eq!(after.rotated_files.len(), 1, "missing file should not be removed from checkpoint");
        assert_eq!(after.rotated_files[0].0, 1);
        assert_eq!(after.start_offset, 0, "start_offset should not advance on failed deletion");
    }
}
