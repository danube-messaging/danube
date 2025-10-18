use crate::checkpoint::WalCheckpoint;
use crate::cloud::CloudStore;
use crate::cloud::CloudWriter;
use crate::frames::{
    extract_offsets_in_prefix, scan_safe_frame_boundary_with_crc, FRAME_HEADER_SIZE,
};
use danube_core::storage::PersistentStorageError;
use tokio::io::AsyncReadExt;

use std::path::PathBuf;
use tokio::io::{AsyncSeekExt, SeekFrom};

/// Internal mutable state for a single streaming cycle.
struct UploadState {
    cloud_writer: Option<CloudWriter>,
    object_id: Option<String>,
    first_offset: Option<u64>,
    last_offset: Option<u64>,
    msgs_since_index: usize,
    total_bytes_uploaded: u64,
    index_entries: Vec<(u64, u64)>,
    next_seq_out: u64,
    next_pos_out: u64,
}

impl UploadState {
    fn new(start_seq: u64, start_pos: u64) -> Self {
        Self {
            cloud_writer: None,
            object_id: None,
            first_offset: None,
            last_offset: None,
            msgs_since_index: 0,
            total_bytes_uploaded: 0,
            index_entries: Vec::new(),
            next_seq_out: start_seq,
            next_pos_out: start_pos,
        }
    }
}

/// Stream frames from a precise WAL resume position to cloud storage.
///
/// Overview of how this works (single cycle):
/// - Reads the WAL starting at `(start_seq, start_pos)` file-by-file, but uploads from at most
///   the first file that yields at least one complete frame (one-file-per-cycle behavior).
/// - Uses a `carry` buffer to stitch frames across chunk boundaries so we never split frames.
/// - For each file chunk, we compute a "safe prefix" length via `scan_safe_frame_boundary_with_crc`:
///   this finds the largest prefix that ends exactly on a full frame while validating CRC per frame.
/// - When we first see a complete frame, we lazily open the cloud streaming writer using a
///   pending object name `data-<start>-pending.dnb1` derived from the first frame's offset.
/// - Over the safe prefix, we do two additional lightweight scans in memory:
///   1) `extract_offsets_in_prefix` to determine `first_offset`/`last_offset` (first time sets
///      both; subsequent times updates `last_offset`).
///   2) A frame-walking loop to build a sparse `(offset -> byte_pos)` index every N messages
///      (currently every 1000), used later by readers to seek efficiently.
/// - We upload the safe prefix bytes, drain them from `carry`, and update `next_seq_out`/`next_pos_out`.
/// - After finishing the first file that produced frames, we close the writer and finalize by copying
///   the pending key to the final key `data-<start>-<end>.dnb1` (with a read+write fallback) and
///   delete the pending key.
///
/// Notes:
/// - The three scans (boundary/CRC, offsets, sparse index) are over the same in-memory safe prefix;
///   there is no re-reading of the file. The dominant cost remains I/O, not these CPU passes.
/// - Defensive CRC policy: any CRC mismatch in the safe-prefix scan halts the prefix at the last
///   known-good frame, preventing corrupt data from being uploaded.
///
/// Returns `None` if no complete frame is available in this cycle; otherwise returns the final
/// object id, `(start_offset, end_offset)`, the next `(seq, pos)` resume point, cloud metadata,
/// and the sparse index entries.
#[allow(clippy::too_many_arguments)]
pub async fn stream_frames_to_cloud(
    cloud: &CloudStore,
    topic_path: &str,
    wal_ckpt: &WalCheckpoint,
    start_seq: u64,
    start_pos: u64,
    max_object_mb: Option<u64>,
) -> Result<
    Option<(
        String,            // final_object_id
        u64,               // start_offset
        u64,               // end_offset
        u64,               // next_seq_out
        u64,               // next_pos_out
        opendal::Metadata, // meta
        Vec<(u64, u64)>,   // sparse index entries (offset -> byte_pos)
    )>,
    PersistentStorageError,
> {
    // 1) Build ordered list of WAL files to read from (including active file)
    let files = build_ordered_wal_files(wal_ckpt);

    // 2) Initialize streaming state
    let mut state = UploadState::new(start_seq, start_pos);
    let mut carry: Vec<u8> = Vec::new();

    // 3) Process files starting from the resume point; continue across files while under cap
    let cap_bytes: Option<u64> = max_object_mb.map(|mb| mb.saturating_mul(1024 * 1024));
    for (seq, path) in files.into_iter().filter(|(s, _)| *s >= start_seq) {
        process_file_and_upload_once(
            cloud, topic_path, seq, &path, start_seq, start_pos, &mut carry, &mut state,
        )
        .await?;

        // Stop if we have started and hit size cap
        if state.first_offset.is_some() {
            if let Some(cap) = cap_bytes {
                if state.total_bytes_uploaded >= cap {
                    break;
                }
            }
        }
    }

    // If we didn't upload anything, return None
    let first_offset_val = match state.first_offset {
        Some(start) => start,
        None => return Ok(None),
    };

    // 4) Finalize uploaded object and return
    let (final_object_id, end, meta) =
        finalize_uploaded_object(cloud, topic_path, &mut state).await?;

    Ok(Some((
        final_object_id,
        first_offset_val,
        end,
        state.next_seq_out,
        state.next_pos_out,
        meta,
        state.index_entries,
    )))
}

fn build_ordered_wal_files(wal_ckpt: &WalCheckpoint) -> Vec<(u64, PathBuf)> {
    // Map rotated_files (seq, path, first_offset) -> (seq, path) for streaming
    let mut files: Vec<(u64, PathBuf)> = wal_ckpt
        .rotated_files
        .iter()
        .map(|(seq, path, _first)| (*seq, path.clone()))
        .collect();
    files.push((wal_ckpt.file_seq, PathBuf::from(&wal_ckpt.file_path)));
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

async fn process_file_and_upload_once(
    cloud: &CloudStore,
    topic_path: &str,
    seq: u64,
    path: &PathBuf,
    start_seq: u64,
    start_pos: u64,
    carry: &mut Vec<u8>,
    state: &mut UploadState,
) -> Result<(), PersistentStorageError> {
    // Only process valid paths
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    let mut file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(_) => return Ok(()),
    };
    if seq == start_seq && start_pos > 0 {
        file.seek(SeekFrom::Start(start_pos))
            .await
            .map_err(|_| PersistentStorageError::Other("seek failed".into()))?;
    }

    let chunk_size = 50 * 1024 * 1024; // 50 MiB file read buffer
    let mut buf = vec![0u8; chunk_size];

    loop {
        let n = file
            .read(&mut buf)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("read wal: {}", e)))?;
        if n == 0 {
            break;
        }
        // Append to carry and parse frame headers/payload boundaries to avoid cutting frames.
        carry.extend_from_slice(&buf[..n]);

        // Determine how many complete bytes we can safely upload (ending at frame boundary)
        let safe_len = scan_safe_frame_boundary_with_crc(&carry);
        if safe_len > 0 {
            // Initialize writer and object name on first complete frame
            if state.first_offset.is_none() {
                let (first, last) = extract_offsets_in_prefix(&carry[..safe_len]);
                state.first_offset = first;
                state.last_offset = last;
                if let Some(s) = state.first_offset {
                    let obj = format!("data-{}-pending.dnb1", s);
                    let path = format!("storage/topics/{}/objects/{}", topic_path, obj);
                    let writer = cloud
                        .open_streaming_writer(&path, 8 * 1024 * 1024, 4)
                        .await?;
                    state.cloud_writer = Some(writer);
                    state.object_id = Some(obj);
                }
            } else {
                // Update last_offset as we extend
                let (_first, last) = extract_offsets_in_prefix(&carry[..safe_len]);
                if let Some(l) = last {
                    state.last_offset = Some(l);
                }
            }

            // Build sparse index entries within this safe prefix based on message count.
            let mut idx_scan: usize = 0;
            while idx_scan + FRAME_HEADER_SIZE <= safe_len {
                let off = u64::from_le_bytes(carry[idx_scan..idx_scan + 8].try_into().unwrap());
                let len = u32::from_le_bytes(carry[idx_scan + 8..idx_scan + 12].try_into().unwrap())
                    as usize;
                let next = idx_scan + FRAME_HEADER_SIZE + len;
                if next > safe_len {
                    break;
                }
                if state.msgs_since_index == 0 {
                    // record index entry at this frame boundary
                    state
                        .index_entries
                        .push((off, state.total_bytes_uploaded + idx_scan as u64));
                }
                state.msgs_since_index = (state.msgs_since_index + 1) % 1000; // INDEX_EVERY_MSGS
                idx_scan = next;
            }
            if let Some(w) = state.cloud_writer.as_mut() {
                w.write(&carry[..safe_len]).await?;
            }
            // Drain uploaded bytes from carry
            carry.drain(0..safe_len);
            state.total_bytes_uploaded += safe_len as u64;
        }
        // Update next position in this file
        state.next_seq_out = seq;
        state.next_pos_out = file.stream_position().await.unwrap_or(0);
    }
    Ok(())
}

async fn finalize_uploaded_object(
    cloud: &CloudStore,
    topic_path: &str,
    state: &mut UploadState,
) -> Result<(String, u64, opendal::Metadata), PersistentStorageError> {
    let mut cw = state
        .cloud_writer
        .take()
        .expect("writer must exist for finalize");
    let meta = cw.close().await?;
    let first_offset = state.first_offset.expect("first_offset must be set");
    let end = state.last_offset.unwrap_or(first_offset);
    let final_object_id = format!("data-{}-{}.dnb1", first_offset, end);
    // Perform server-side copy from pending key to final key and delete pending key
    let pending_path = format!(
        "storage/topics/{}/objects/{}",
        topic_path,
        format!("data-{}-pending.dnb1", first_offset)
    );
    let final_path = format!("storage/topics/{}/objects/{}", topic_path, final_object_id);
    match cloud.copy_object(&pending_path, &final_path).await {
        Ok(()) => {
            let _ = cloud.delete_object(&pending_path).await;
        }
        Err(_e) => {
            if let Ok(bytes) = cloud.get_object(&pending_path).await {
                let _ = cloud.put_object_meta(&final_path, &bytes).await;
            }
            let _ = cloud.delete_object(&pending_path).await;
        }
    }
    Ok((final_object_id, end, meta))
}
