use crate::checkpoint::WalCheckpoint;
use crate::cloud::CloudStore;
use crate::frames::{
    extract_offsets_in_prefix, scan_safe_frame_boundary_with_crc, FRAME_HEADER_SIZE,
};
use danube_core::storage::PersistentStorageError;
use tokio::io::AsyncReadExt;

use crate::persistent_metrics::{
    CLOUD_UPLOAD_BYTES_TOTAL, CLOUD_UPLOAD_LATENCY_MS, CLOUD_UPLOAD_OBJECTS_TOTAL,
};
use metrics::{counter, histogram};
use std::path::PathBuf;
use std::time::Instant;
use tokio::io::{AsyncSeekExt, SeekFrom};

/// Internal mutable state for a single streaming cycle.
struct UploadState {
    segment_bytes: Vec<u8>,
    segment_id: Option<String>,
    first_offset: Option<u64>,
    last_offset: Option<u64>,
    msgs_since_index: usize,
    total_bytes_uploaded: u64,
    index_entries: Vec<(u64, u64)>,
    next_seq_out: u64,
    next_pos_out: u64,
    start_instant: Option<Instant>,
}

impl UploadState {
    fn new(start_seq: u64, start_pos: u64) -> Self {
        Self {
            segment_bytes: Vec::new(),
            segment_id: None,
            first_offset: None,
            last_offset: None,
            msgs_since_index: 0,
            total_bytes_uploaded: 0,
            index_entries: Vec::new(),
            next_seq_out: start_seq,
            next_pos_out: start_pos,
            start_instant: None,
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
///   timestamp-based final object name `data-<start>-<timestamp>.dnb1` for uniqueness.
///   The end_offset is stored in the metadata store, not the object name.
/// - Over the safe prefix, we do two additional lightweight scans in memory:
///   1) `extract_offsets_in_prefix` to determine `first_offset`/`last_offset` (first time sets
///      both; subsequent times updates `last_offset`).
///   2) A frame-walking loop to build a sparse `(offset -> byte_pos)` index every N messages
///      (currently every 1000), used later by readers to seek efficiently.
/// - We upload the safe prefix bytes, drain them from `carry`, and update `next_seq_out`/`next_pos_out`.
/// - After finishing the first file that produced frames, we close the writer and return the
///   final object metadata (no copy/rename needed).
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
        String,
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
            seq, &path, start_seq, start_pos, &mut carry, &mut state,
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
    let (final_segment_id, end, meta) = finalize_uploaded_segment(cloud, topic_path, &mut state).await?;
    // Metrics: upload latency, bytes, objects (ok)
    let provider = cloud.provider().to_string();

    if let Some(start) = state.start_instant.take() {
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        histogram!(CLOUD_UPLOAD_LATENCY_MS.name, "provider"=> provider.clone()).record(elapsed_ms);
    }

    // Normalize topic label to include leading slash to align with other topic metrics
    let topic_label = if topic_path.starts_with('/') {
        topic_path.to_string()
    } else {
        format!("/{}", topic_path)
    };
    counter!(CLOUD_UPLOAD_OBJECTS_TOTAL.name, "topic"=> topic_label.clone(), "provider"=> provider.clone(), "result"=> "ok").increment(1);
    counter!(CLOUD_UPLOAD_BYTES_TOTAL.name, "topic"=> topic_label, "provider"=> provider)
        .increment(meta.content_length());

    Ok(Some((
        final_segment_id,
        first_offset_val,
        end,
        state.next_seq_out,
        state.next_pos_out,
        meta,
        state.index_entries,
    )))
}

pub async fn resume_position_from_uploaded_offset(
    wal_ckpt: &WalCheckpoint,
    last_committed_offset: u64,
) -> Result<Option<(u64, u64)>, PersistentStorageError> {
    if wal_ckpt.last_offset <= last_committed_offset {
        return Ok(None);
    }

    let files = build_ordered_wal_files_with_offsets(wal_ckpt);
    for (idx, (seq, path, first_offset)) in files.iter().enumerate() {
        let first_offset = match first_offset {
            Some(first_offset) => *first_offset,
            None => continue,
        };
        if last_committed_offset < first_offset {
            return Ok(Some((*seq, 0)));
        }

        let next_first_offset = files
            .iter()
            .skip(idx + 1)
            .find_map(|(_, _, next_first)| *next_first)
            .unwrap_or_else(|| wal_ckpt.last_offset.saturating_add(1));
        if next_first_offset <= first_offset {
            continue;
        }
        let end_offset = next_first_offset.saturating_sub(1);
        if last_committed_offset > end_offset {
            continue;
        }

        let resume_pos =
            find_resume_position_in_file(path, last_committed_offset).await?;
        return Ok(Some((*seq, resume_pos)));
    }

    Ok(None)
}

pub fn initial_resume_position(wal_ckpt: &WalCheckpoint) -> Option<(u64, u64)> {
    build_ordered_wal_files_with_offsets(wal_ckpt)
        .into_iter()
        .find_map(|(seq, _path, first_offset)| first_offset.map(|_| (seq, 0)))
}

pub async fn upload_contiguous_rotated_file_as_segment(
    cloud: &CloudStore,
    topic_path: &str,
    wal_ckpt: &WalCheckpoint,
    last_committed_offset: u64,
    max_object_mb: Option<u64>,
) -> Result<Option<(String, u64, u64, opendal::Metadata, Vec<(u64, u64)>)>, PersistentStorageError>
{
    let next_expected_offset = last_committed_offset.saturating_add(1);
    let mut rotated_files: Vec<(u64, PathBuf)> = wal_ckpt
        .rotated_files
        .iter()
        .map(|(_, path, first_offset)| (*first_offset, path.clone()))
        .collect();
    rotated_files.sort_by_key(|(first_offset, _)| *first_offset);

    let path = match rotated_files
        .into_iter()
        .find(|(first_offset, _)| *first_offset == next_expected_offset)
    {
        Some((_, path)) => path,
        None => return Ok(None),
    };

    let file_bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(PersistentStorageError::Io(format!(
                "read wal for rotated upload failed: {}",
                e
            )))
        }
    };
    if file_bytes.is_empty() {
        return Ok(None);
    }

    let safe_len = scan_safe_frame_boundary_with_crc(&file_bytes);
    if safe_len == 0 {
        return Ok(None);
    }

    if let Some(cap_bytes) = max_object_mb.map(|mb| mb.saturating_mul(1024 * 1024)) {
        if safe_len as u64 > cap_bytes {
            return Ok(None);
        }
    }

    let safe_bytes = &file_bytes[..safe_len];
    let (start_offset, end_offset) = match extract_offsets_in_prefix(safe_bytes) {
        (Some(start_offset), Some(end_offset)) => (start_offset, end_offset),
        _ => return Ok(None),
    };
    if start_offset != next_expected_offset {
        return Ok(None);
    }

    let started_at = Instant::now();
    let segment_id = format!(
        "data-{}-{}.dnb1",
        start_offset,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    let object_path = format!("storage/topics/{}/segments/{}", topic_path, segment_id);
    let meta = cloud.put_object_meta(&object_path, safe_bytes).await?;

    let provider = cloud.provider().to_string();
    let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
    histogram!(CLOUD_UPLOAD_LATENCY_MS.name, "provider"=> provider.clone()).record(elapsed_ms);
    let topic_label = if topic_path.starts_with('/') {
        topic_path.to_string()
    } else {
        format!("/{}", topic_path)
    };
    counter!(CLOUD_UPLOAD_OBJECTS_TOTAL.name, "topic"=> topic_label.clone(), "provider"=> provider.clone(), "result"=> "ok").increment(1);
    counter!(CLOUD_UPLOAD_BYTES_TOTAL.name, "topic"=> topic_label, "provider"=> provider)
        .increment(meta.content_length());

    Ok(Some((
        segment_id,
        start_offset,
        end_offset,
        meta,
        build_offset_index_entries(safe_bytes),
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

fn build_ordered_wal_files_with_offsets(wal_ckpt: &WalCheckpoint) -> Vec<(u64, PathBuf, Option<u64>)> {
    let mut files: Vec<(u64, PathBuf, Option<u64>)> = wal_ckpt
        .rotated_files
        .iter()
        .map(|(seq, path, first_offset)| (*seq, path.clone(), Some(*first_offset)))
        .collect();
    if !wal_ckpt.file_path.is_empty() {
        files.push((
            wal_ckpt.file_seq,
            PathBuf::from(&wal_ckpt.file_path),
            wal_ckpt.active_file_first_offset,
        ));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

async fn find_resume_position_in_file(
    path: &PathBuf,
    last_committed_offset: u64,
) -> Result<u64, PersistentStorageError> {
    let file_bytes = tokio::fs::read(path)
        .await
        .map_err(|e| PersistentStorageError::Other(format!("read wal for resume: {}", e)))?;
    let safe_len = scan_safe_frame_boundary_with_crc(&file_bytes);
    let mut pos = 0usize;
    while pos + FRAME_HEADER_SIZE <= safe_len {
        let offset = u64::from_le_bytes(file_bytes[pos..pos + 8].try_into().unwrap());
        let payload_len =
            u32::from_le_bytes(file_bytes[pos + 8..pos + 12].try_into().unwrap()) as usize;
        let next = pos + FRAME_HEADER_SIZE + payload_len;
        if next > safe_len {
            break;
        }
        if offset > last_committed_offset {
            return Ok(pos as u64);
        }
        pos = next;
    }
    Ok(safe_len as u64)
}

async fn process_file_and_upload_once(
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
                    // Use timestamp-based naming for uniqueness (eliminates copy/rename)
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let obj = format!("data-{}-{}.dnb1", s, timestamp);
                    state.segment_id = Some(obj);
                    state.start_instant = Some(Instant::now());
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
            state.segment_bytes.extend_from_slice(&carry[..safe_len]);
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

async fn finalize_uploaded_segment(
    cloud: &CloudStore,
    topic_path: &str,
    state: &mut UploadState,
) -> Result<(String, u64, opendal::Metadata), PersistentStorageError> {
    let first_offset = state.first_offset.expect("first_offset must be set");
    let end = state.last_offset.unwrap_or(first_offset);
    let final_segment_id = state.segment_id.take().expect("segment_id must be set");
    let object_path = format!("storage/topics/{}/segments/{}", topic_path, final_segment_id);
    let meta = cloud.put_object_meta(&object_path, &state.segment_bytes).await?;
    Ok((final_segment_id, end, meta))
}

fn build_offset_index_entries(bytes: &[u8]) -> Vec<(u64, u64)> {
    let mut idx = 0usize;
    let mut offsets = Vec::new();
    let mut msgs_since_index = 0usize;
    while idx + FRAME_HEADER_SIZE <= bytes.len() {
        let offset = u64::from_le_bytes(bytes[idx..idx + 8].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[idx + 8..idx + 12].try_into().unwrap()) as usize;
        let next = idx + FRAME_HEADER_SIZE + len;
        if next > bytes.len() {
            break;
        }
        if msgs_since_index == 0 {
            offsets.push((offset, idx as u64));
        }
        msgs_since_index = (msgs_since_index + 1) % 1000;
        idx = next;
    }
    offsets
}
