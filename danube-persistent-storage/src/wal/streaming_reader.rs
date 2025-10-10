use std::path::PathBuf;

use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::checkpoint::WalCheckpoint;
use tracing::info;
use crate::frames::FRAME_HEADER_SIZE;
use crate::frames::scan_safe_frame_boundary_with_crc;

/// Build ordered list of WAL files from checkpoint: rotated files + active file, sorted by seq.
fn build_ordered_wal_files(wal_ckpt: &WalCheckpoint) -> Vec<(u64, PathBuf)> {
    // Map rotated_files (seq, path, first_offset) -> (seq, path) for streaming
    let mut files: Vec<(u64, PathBuf)> = wal_ckpt
        .rotated_files
        .iter()
        .map(|(seq, path, _first)| (*seq, path.clone()))
        .collect();
    if !wal_ckpt.file_path.is_empty() {
        files.push((wal_ckpt.file_seq, PathBuf::from(&wal_ckpt.file_path)));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

/// Stream frames from local WAL files starting at `from_offset`, parsing incrementally in chunks.
/// Returns a stream of `StreamMessage` items (boxed) with per-item `Result`.
pub(crate) async fn stream_from_wal_files(
    checkpoint: &WalCheckpoint,
    from_offset: u64,
    chunk_size: usize,
) -> Result<TopicStream, PersistentStorageError> {
    let files = build_ordered_wal_files(checkpoint);

    // Channel to deliver parsed messages to the caller as a stream.
    let (tx, rx) = mpsc::channel::<Result<StreamMessage, PersistentStorageError>>(1024);

    // Spawn a background task to perform the I/O and parsing.
    tokio::spawn(async move {
        let mut carry: Vec<u8> = Vec::new();
        let mut buf: Vec<u8> = vec![0u8; chunk_size];

        'outer: for (seq, path) in files.into_iter() {
            if path.as_os_str().is_empty() {
                continue;
            }
            info!(target = "wal_reader", seq, file = %path.display(), "opening wal file for streaming");
            let mut file = match tokio::fs::File::open(&path).await {
                Ok(f) => f,
                Err(_) => continue, // skip missing files
            };

            // We don't know the byte position of `from_offset` in this file; we'll parse frames and skip by offset.
            loop {
                let n = match file.read(&mut buf).await {
                    Ok(0) => break, // EOF
                    Ok(n) => n,
                    Err(e) => {
                        let _ = tx
                            .send(Err(PersistentStorageError::Other(format!(
                                "read wal: {}",
                                e
                            ))))
                            .await;
                        break 'outer;
                    }
                };

                carry.extend_from_slice(&buf[..n]);

                // Parse as many complete frames as possible from the carry buffer.
                let mut idx = 0usize;
                let safe_len = scan_safe_frame_boundary_with_crc(&carry);
                while idx + FRAME_HEADER_SIZE <= safe_len {
                    let off = u64::from_le_bytes(carry[idx..idx + 8].try_into().unwrap());
                    let len = u32::from_le_bytes(
                        carry[idx + 8..idx + 12].try_into().unwrap(),
                    ) as usize;
                    let frame_end = idx + FRAME_HEADER_SIZE + len;
                    if frame_end > safe_len {
                        break;
                    }
                    if off >= from_offset {
                        let payload = &carry[idx + FRAME_HEADER_SIZE..frame_end];
                        match bincode::deserialize::<StreamMessage>(payload) {
                            Ok(mut msg) => {
                                msg.msg_id.topic_offset = off;
                                if tx.send(Ok(msg)).await.is_err() {
                                    // receiver dropped; stop work
                                    break 'outer;
                                }
                            }
                            Err(e) => {
                                let _ = tx
                                    .send(Err(PersistentStorageError::Io(format!(
                                        "bincode deserialize failed: {}",
                                        e
                                    ))))
                                    .await;
                                break 'outer;
                            }
                        }
                    }
                    idx = frame_end;
                }

                // Drain the parsed bytes from the carry buffer to keep memory bounded.
                if safe_len > 0 {
                    carry.drain(0..safe_len);
                }
            }
        }
        // Optional: drop tx to close the stream gracefully.
    });

    Ok(Box::pin(ReceiverStream::new(rx)))
}
