use super::StorageFactory;
use crate::frames::{decode_next_frame, extract_offsets, scan_safe_frame_boundary};
use crate::hot_log::HotLog;
use crate::metadata::SegmentDescriptor;
use danube_core::storage::PersistentStorageError;
use std::collections::HashSet;
use std::path::PathBuf;

impl StorageFactory {
    pub(super) async fn cut_and_export_topic_segments(
        &self,
        topic_path: &str,
        hot_log: &HotLog,
    ) -> Result<(), PersistentStorageError> {
        if !self.mode.is_cloud_native() {
            return Ok(());
        }
        hot_log.rotate().await?;
        self.export_topic_segments(topic_path, hot_log, false).await
    }

    pub(super) async fn export_topic_segments(
        &self,
        topic_path: &str,
        hot_log: &HotLog,
        include_active_file: bool,
    ) -> Result<(), PersistentStorageError> {
        if !self.uses_sealed_segment_export() {
            return Ok(());
        }
        let durable_store = match self.durable_store.clone() {
            Some(store) => store,
            None if self.mode.requires_durable_backend() => {
                return Err(PersistentStorageError::Other(
                    "durable storage mode requires durable backend for segment export".to_string(),
                ))
            }
            None => return Ok(()),
        };
        let wal_ckpt = match hot_log.current_wal_checkpoint().await {
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
            let segment_id = format!(
                "data-{}-{}.dnb1",
                start_offset,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            );
            let object_path = format!("storage/topics/{}/segments/{}", topic_path, segment_id);
            let meta = durable_store.put_segment(&object_path, safe_bytes).await?;
            let desc = SegmentDescriptor {
                segment_id: segment_id.clone(),
                start_offset,
                end_offset,
                size: safe_len as u64,
                etag: meta.etag().map(|s| s.to_string()),
                created_at: chrono::Utc::now().timestamp() as u64,
                completed: true,
                offset_index: build_offset_index(safe_bytes),
            };
            let start_padded = format!("{:020}", start_offset);
            self.segment_catalog
                .put_segment(topic_path, &start_padded, &desc)
                .await?;
            self.segment_catalog
                .set_current_segment(topic_path, &start_padded)
                .await?;
        }

        Ok(())
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
            None if self.mode.requires_durable_backend() => {
                return Err(PersistentStorageError::Other(
                    "durable storage mode requires durable backend for segment deletion".to_string(),
                ))
            }
            None => return Ok(()),
        };
        let segments = self.segment_catalog.list_segments(topic_path).await?;
        for segment in segments {
            let object_path = format!("storage/topics/{}/segments/{}", topic_path, segment.segment_id);
            durable_store.delete_segment(&object_path).await?;
        }
        Ok(())
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
        self.mode.is_local() || self.mode.requires_durable_backend()
    }
}

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
