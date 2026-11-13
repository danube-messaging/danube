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
        if !self.should_start_background_export()
            && !self.cloud_tiered_topics.contains(topic_path)
        {
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
        let durable_store = match self.current_durable_store() {
            Some(store) => store,
            None if self.current_mode().requires_separate_durable_backend() => {
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
        let durable_store = match self.current_durable_store() {
            Some(store) => store,
            None if self.current_mode().requires_separate_durable_backend() => {
                return Err(PersistentStorageError::Other(
                    "storage mode requires separate durable backend for segment deletion".to_string(),
                ))
            }
            None => return Ok(()),
        };
        delete_topic_segments_with_refcount(
            &self.segment_catalog,
            durable_store.as_ref(),
            topic_path,
        )
        .await?;

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

        let stream_key = format!("/topics/{}/stream", topic_path);
        let _ = client.del(&stream_key).await;
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
        self.current_mode().requires_separate_durable_backend()
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
            ref_count: 1,
            object_topic: None,
            max_valid_offset: None,
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

pub(crate) async fn delete_topic_segments_with_refcount(
    segment_catalog: &crate::metadata::SegmentCatalog,
    durable_store: &dyn crate::durable_store::DurableStore,
    topic_path: &str,
) -> Result<(), PersistentStorageError> {
    let segments = segment_catalog.list_segments(topic_path).await?;
    let meta = segment_catalog.metadata();
    for segment in segments {
        let effective_topic = segment.object_topic.as_deref().unwrap_or(topic_path);
        let remaining_refs = meta
            .list_other_segment_references(effective_topic, &segment.segment_id, topic_path)
            .await?;

        if remaining_refs.is_empty() {
            let object_path = segment_object_path(effective_topic, &segment.segment_id);
            durable_store.delete_segment(&object_path).await?;
        } else {
            let next_rc = remaining_refs.len() as u32;
            for (other_topic, start_padded, mut other_desc) in remaining_refs {
                if other_desc.ref_count != next_rc {
                    other_desc.ref_count = next_rc;
                    let _ = segment_catalog
                        .put_segment(&other_topic, &start_padded, &other_desc)
                        .await;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::delete_topic_segments_with_refcount;
    use crate::durable_store::{segment_object_path, DurableStore};
    use crate::metadata::{SegmentCatalog, SegmentDescriptor, StorageMetadata};
    use crate::object_store_backend::{BackendConfig, ObjStore};
    use crate::ObjStoreDurable;
    use danube_core::metadata::{MemoryStore, MetadataStore};
    use std::sync::Arc;

    #[tokio::test]
    async fn branched_segment_refcount_preserves_object_until_last_topic_deleted() {
        let mem_meta: Arc<dyn MetadataStore> =
            Arc::new(MemoryStore::new().await.expect("memory store"));
        let storage_meta = StorageMetadata::new(mem_meta, "/danube".to_string());
        let catalog = SegmentCatalog::new(storage_meta.clone());

        let obj_store = ObjStore::new(BackendConfig::Memory {
            root: "mem-branch-test".to_string(),
        })
        .expect("mem obj store");
        let durable = ObjStoreDurable::new(obj_store);

        let seg_id = "data-0-100.dnb1".to_string();
        let obj_path = segment_object_path("ns/src", &seg_id);
        durable
            .put_segment(&obj_path, b"segment-payload")
            .await
            .expect("put segment");

        let desc = SegmentDescriptor {
            segment_id: seg_id,
            start_offset: 0,
            end_offset: 9,
            size: 15,
            etag: None,
            created_at: 1,
            completed: true,
            offset_index: None,
            ref_count: 1,
            object_topic: None,
            max_valid_offset: None,
        };
        let start_padded = format!("{:020}", 0);
        catalog
            .put_segment("ns/src", &start_padded, &desc)
            .await
            .expect("put descriptor");

        let copied = storage_meta
            .branch_topic_segments("ns/src", "ns/branch", None)
            .await
            .expect("branch segments");
        assert_eq!(copied, 1);

        let src_desc = storage_meta
            .get_segment_descriptor("ns/src", &start_padded)
            .await
            .expect("get src")
            .expect("src exists");
        let br_desc = storage_meta
            .get_segment_descriptor("ns/branch", &start_padded)
            .await
            .expect("get branch")
            .expect("branch exists");
        assert_eq!(src_desc.ref_count, 2);
        assert_eq!(br_desc.ref_count, 2);
        assert_eq!(br_desc.object_topic.as_deref(), Some("ns/src"));

        // Deleting the branch while source still holds a reference skips physical deletion
        // and decrements source ref_count back to 1.
        delete_topic_segments_with_refcount(&catalog, &durable, "ns/branch")
            .await
            .expect("delete branch segments");
        catalog.delete_topic("ns/branch").await.expect("delete branch meta");

        assert!(durable.open_segment_reader(&obj_path, 0).await.is_ok());

        let updated_src = storage_meta
            .get_segment_descriptor("ns/src", &start_padded)
            .await
            .expect("get src after branch delete")
            .expect("src still exists");
        assert_eq!(updated_src.ref_count, 1);

        // Now deleting source (ref_count == 1) physically deletes the durable object.
        delete_topic_segments_with_refcount(&catalog, &durable, "ns/src")
            .await
            .expect("delete source segments");
        assert!(durable.open_segment_reader(&obj_path, 0).await.is_err());
    }

    #[tokio::test]
    async fn deleting_parent_before_branch_preserves_object_until_branch_deleted() {
        let mem_meta: Arc<dyn MetadataStore> =
            Arc::new(MemoryStore::new().await.expect("memory store"));
        let storage_meta = StorageMetadata::new(mem_meta, "/danube".to_string());
        let catalog = SegmentCatalog::new(storage_meta.clone());

        let obj_store = ObjStore::new(BackendConfig::Memory {
            root: "mem-parent-first-test".to_string(),
        })
        .expect("mem obj store");
        let durable = ObjStoreDurable::new(obj_store);

        let seg_id = "data-0-parent-first.dnb1".to_string();
        let obj_path = segment_object_path("ns/parent", &seg_id);
        durable
            .put_segment(&obj_path, b"parent-payload")
            .await
            .expect("put segment");

        let desc = SegmentDescriptor {
            segment_id: seg_id,
            start_offset: 0,
            end_offset: 9,
            size: 14,
            etag: None,
            created_at: 1,
            completed: true,
            offset_index: None,
            ref_count: 1,
            object_topic: None,
            max_valid_offset: None,
        };
        let start_padded = format!("{:020}", 0);
        catalog
            .put_segment("ns/parent", &start_padded, &desc)
            .await
            .expect("put parent descriptor");

        let copied = storage_meta
            .branch_topic_segments("ns/parent", "ns/child", Some(4))
            .await
            .expect("branch segments");
        assert_eq!(copied, 1);

        let child_desc = storage_meta
            .get_segment_descriptor("ns/child", &start_padded)
            .await
            .expect("get child")
            .expect("child exists");
        assert_eq!(child_desc.max_valid_offset, Some(4));

        // Delete parent FIRST: object must stay intact and child ref_count must drop to 1.
        delete_topic_segments_with_refcount(&catalog, &durable, "ns/parent")
            .await
            .expect("delete parent segments");
        catalog.delete_topic("ns/parent").await.expect("delete parent meta");
        assert!(durable.open_segment_reader(&obj_path, 0).await.is_ok());

        let updated_child = storage_meta
            .get_segment_descriptor("ns/child", &start_padded)
            .await
            .expect("get child after parent delete")
            .expect("child still exists");
        assert_eq!(updated_child.ref_count, 1);

        // Deleting child last physically removes the shared segment object.
        delete_topic_segments_with_refcount(&catalog, &durable, "ns/child")
            .await
            .expect("delete child segments");
        assert!(durable.open_segment_reader(&obj_path, 0).await.is_err());
    }
}
