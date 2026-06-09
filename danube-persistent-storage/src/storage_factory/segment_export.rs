use super::StorageFactory;
use crate::durable_store::segment_object_path;
use crate::frames::{decode_next_frame, extract_offsets, scan_safe_frame_boundary};
use crate::metadata::SegmentDescriptor;
use crate::wal::Wal;
use danube_core::storage::PersistentStorageError;
use std::collections::HashSet;
use std::path::PathBuf;

impl StorageFactory {
    /// Rotate the active WAL file and export any newly sealed segments.
    ///
    /// This is the periodic export path used by background exporters. Rotation happens first so the
    /// exporter works on immutable sealed files instead of racing the actively written file.
    pub(super) async fn cut_and_export_topic_segments(
        &self,
        topic_path: &str,
        wal: &Wal,
    ) -> Result<(), PersistentStorageError> {
        if !self.should_start_background_export() {
            return Ok(());
        }
        wal.rotate().await?;
        self.export_topic_segments(topic_path, wal, false).await
    }

    /// Export eligible local WAL files into durable segment objects.
    ///
    /// Functional flow
    /// - Load the current WAL checkpoint to discover rotated files and, optionally, the active file.
    /// - Query the segment catalog to avoid re-exporting a segment whose `start_offset` is already
    ///   published.
    /// - Read each candidate file, trim it to the last safe frame boundary, and derive its start/end
    ///   offsets from the intact frame sequence.
    /// - Upload the safe byte prefix as a durable segment and publish the descriptor into metadata.
    ///
    /// Safety notes
    /// - Only the safe prefix up to `scan_safe_frame_boundary()` is exported, so partially written
    ///   trailing frames are never published as durable history.
    /// - `include_active_file` is reserved for explicit seal/export flows; the periodic exporter
    ///   normally works on rotated files only.
    pub(super) async fn export_topic_segments(
        &self,
        topic_path: &str,
        wal: &Wal,
        include_active_file: bool,
    ) -> Result<(), PersistentStorageError> {
        if !self.uses_sealed_segment_export() {
            return Ok(());
        }
        let durable_store = match self.durable_store.clone() {
            Some(store) => store,
            None if self.mode.requires_separate_durable_backend() => {
                return Err(PersistentStorageError::Other(
                    "storage mode requires separate durable backend for segment export".to_string(),
                ))
            }
            None => return Ok(()),
        };
        let wal_ckpt = match wal.current_wal_checkpoint().await {
            Some(ckpt) => ckpt,
            None => return Ok(()),
        };
        let existing_starts: HashSet<u64> = self
            .segment_catalog
            .list_segments(topic_path)
            .await?
            .into_iter()
            .map(|desc| desc.start_offset)
            .collect();

        let mut files: Vec<(u64, PathBuf)> = wal_ckpt
            .rotated_files
            .iter()
            .map(|(_, path, first_offset)| (*first_offset, path.clone()))
            .collect();
        if include_active_file {
            if let Some(first_offset) = wal_ckpt.active_file_first_offset {
                if !wal_ckpt.file_path.is_empty() {
                    files.push((first_offset, PathBuf::from(wal_ckpt.file_path.clone())));
                }
            }
        }
        files.sort_by_key(|(first_offset, _)| *first_offset);

        for (file_first_offset, path) in files {
            if existing_starts.contains(&file_first_offset) {
                continue;
            }
            let file_bytes = match tokio::fs::read(&path).await {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(PersistentStorageError::Io(format!(
                        "read local wal file {} failed: {}",
                        path.display(),
                        e
                    )))
                }
            };
            if file_bytes.is_empty() {
                continue;
            }

            let safe_len = scan_safe_frame_boundary(&file_bytes);
            if safe_len == 0 {
                continue;
            }
            let safe_bytes = &file_bytes[..safe_len];
            let (start_offset, end_offset) = match extract_offsets(safe_bytes) {
                (Some(start), Some(end)) => (start, end),
                _ => continue,
            };
            let desc = self
                .persist_durable_segment(
                    &durable_store,
                    topic_path,
                    safe_bytes,
                    start_offset,
                    end_offset,
                )
                .await?;
            self.publish_durable_segment(topic_path, &desc).await?;

            // Clean up exported offsets from Valkey active_segment and
            // promote the segment for warm caching.
            self.valkey_post_export_cleanup(topic_path, start_offset, end_offset, &desc.segment_id)
                .await;
        }

        Ok(())
    }

    /// Post-export Valkey housekeeping: clean up exported offsets and
    /// optionally promote the segment for warm caching.
    ///
    /// Best-effort: Valkey errors are logged but don't fail the export.
    async fn valkey_post_export_cleanup(
        &self,
        topic_path: &str,
        start_offset: u64,
        end_offset: u64,
        segment_id: &str,
    ) {
        let (client, config) = match (&self.write_buffer, self.valkey_client.get()) {
            (Some(cfg), Some(client)) => (client.clone(), cfg),
            _ => return, // no write buffer configured or not connected
        };

        // Remove exported offsets from active_segment — keeps only un-exported
        // messages that arrived after the WAL rotation.
        crate::valkey::segment_lifecycle::cleanup_exported_offsets(
            &client,
            topic_path,
            start_offset,
            end_offset,
        )
        .await;

        // Promote active_segment → named segment for warm caching
        crate::valkey::segment_lifecycle::promote_segment(
            &client,
            config,
            topic_path,
            segment_id,
        )
        .await;
    }

    pub(super) async fn clear_topic_wal_state(
        &self,
        topic_path: &str,
    ) -> Result<(), PersistentStorageError> {
        if let Some(dir) = self.topic_wal_dir(topic_path) {
            match tokio::fs::remove_dir_all(&dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(PersistentStorageError::Io(format!(
                        "remove local wal dir {} failed: {}",
                        dir.display(),
                        e
                    )))
                }
            }
        }
        Ok(())
    }

    pub(super) async fn delete_topic_durable_segments(
        &self,
        topic_path: &str,
    ) -> Result<(), PersistentStorageError> {
        let durable_store = match self.durable_store.clone() {
            Some(store) => store,
            None if self.mode.requires_separate_durable_backend() => {
                return Err(PersistentStorageError::Other(
                    "storage mode requires separate durable backend for segment deletion".to_string(),
                ))
            }
            None => return Ok(()),
        };
        let segments = self.segment_catalog.list_segments(topic_path).await?;
        for segment in segments {
            let object_path = segment_object_path(topic_path, &segment.segment_id);
            durable_store.delete_segment(&object_path).await?;
        }

        // Clean up Valkey keys for this topic (best-effort)
        self.clear_valkey_topic_keys(topic_path).await;

        Ok(())
    }

    /// Remove all Valkey keys associated with a deleted topic.
    async fn clear_valkey_topic_keys(&self, topic_path: &str) {
        let client = match self.valkey_client.get() {
            Some(client) => client.clone(),
            None => return,
        };

        let active_key = format!("/topics/{}/active_segment", topic_path);
        let cached_list_key = format!("/topics/{}/cached_segments", topic_path);

        // Delete the active segment hash
        let _ = client.del(&active_key).await;

        // Pop and delete all cached segment hashes
        loop {
            match client.lpop(&cached_list_key).await {
                Ok(Some(segment_id)) => {
                    let seg_key = format!("/topics/{}/segments/{}", topic_path, segment_id);
                    let _ = client.del(&seg_key).await;
                }
                _ => break,
            }
        }

        // Delete the cached segments list itself
        let _ = client.del(&cached_list_key).await;
    }

    pub(super) async fn stop_topic_background_tasks(&self, topic_path: &str) {
        if let Some((_, cancel)) = self.segment_exporter_tokens.remove(topic_path) {
            cancel.cancel();
        }
        if let Some((_, handle)) = self.segment_exporters.remove(topic_path) {
            let _ = handle.await;
        }
        if let Some((_, cancel)) = self.deleter_tokens.remove(topic_path) {
            cancel.cancel();
        }
        if let Some((_, handle)) = self.deleters.remove(topic_path) {
            let _ = handle.await;
        }
    }

    pub(super) fn uses_sealed_segment_export(&self) -> bool {
        self.mode.requires_separate_durable_backend()
    }

    /// Persist one sealed WAL byte range as a durable segment object and construct its descriptor.
    ///
    /// The descriptor records the durable object identity, inclusive offset bounds, object size,
    /// optional ETag, and a sparse offset index that accelerates historical reads.
    async fn persist_durable_segment(
        &self,
        durable_store: &std::sync::Arc<dyn crate::durable_store::DurableStore>,
        topic_path: &str,
        bytes: &[u8],
        start_offset: u64,
        end_offset: u64,
    ) -> Result<SegmentDescriptor, PersistentStorageError> {
        let segment_id = format!(
            "data-{}-{}.dnb1",
            start_offset,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        let object_path = segment_object_path(topic_path, &segment_id);
        let meta = durable_store.put_segment(&object_path, bytes).await?;
        Ok(SegmentDescriptor {
            segment_id,
            start_offset,
            end_offset,
            size: bytes.len() as u64,
            etag: meta.etag().map(|s| s.to_string()),
            created_at: chrono::Utc::now().timestamp() as u64,
            completed: true,
            offset_index: build_offset_index(bytes),
        })
    }

    /// Publish a durable segment descriptor and advance the current-segment pointer.
    ///
    /// The descriptor is written under its padded `start_offset` key first, then the `cur` pointer
    /// is updated to reference that same key so readers can discover the latest durable frontier.
    async fn publish_durable_segment(
        &self,
        topic_path: &str,
        desc: &SegmentDescriptor,
    ) -> Result<(), PersistentStorageError> {
        let start_padded = format!("{:020}", desc.start_offset);
        self.segment_catalog
            .put_segment(topic_path, &start_padded, desc)
            .await?;
        self.segment_catalog
            .set_current_segment(topic_path, &start_padded)
            .await
    }
}

/// Build a sparse `(offset, byte_position)` index over a sealed WAL byte slice.
///
/// The index samples every 1000th message frame rather than every message to keep metadata small
/// while still giving historical readers periodic seek anchors into the segment payload.
fn build_offset_index(bytes: &[u8]) -> Option<Vec<(u64, u64)>> {
    let mut idx = 0usize;
    let mut offsets = Vec::new();
    let mut msgs_since_index = 0usize;
    while idx < bytes.len() {
        let frame = match decode_next_frame(&bytes[idx..]) {
            Ok(Some(frame)) => frame,
            Ok(None) | Err(_) => break,
        };
        if msgs_since_index == 0 {
            offsets.push((frame.offset, idx as u64));
        }
        msgs_since_index = (msgs_since_index + 1) % 1000;
        idx += frame.frame_len;
    }
    if offsets.is_empty() {
        None
    } else {
        Some(offsets)
    }
}
