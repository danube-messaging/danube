use std::path::PathBuf;

use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::checkpoint::WalCheckpoint;
use crate::frames::{decode_next_frame, scan_safe_frame_boundary};
use tracing::info;

/// Build ordered list of WAL files from checkpoint: rotated files + active file, sorted by seq.
///
/// The checkpoint already records replay order, so this helper simply reconstructs the linear file
/// sequence the reader should walk from oldest rotated file to current active file.
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
///
/// Functional behavior
/// - Walk files in checkpoint order, oldest first.
/// - Read bytes in fixed-size chunks and append them into a carry buffer.
/// - Parse only up to the last safe frame boundary so partial trailing frames stay in `carry`
///   until the next read completes them.
/// - Skip frames below `from_offset` and restamp yielded messages with the WAL frame offset.
///
/// Failure model
/// - Missing files are skipped so replay can continue if older files were concurrently pruned.
/// - Read or decode failures are surfaced through the stream and terminate replay.
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
                let safe_len = scan_safe_frame_boundary(&carry);
                while idx < safe_len {
                    let frame = match decode_next_frame(&carry[idx..safe_len]) {
                        Ok(Some(frame)) => frame,
                        Ok(None) | Err(_) => break,
                    };
                    if frame.offset >= from_offset {
                        match bincode::serde::decode_from_slice::<StreamMessage, _>(
                            frame.payload,
                            bincode::config::standard(),
                        )
                        .map(|(v, _)| v)
                        {
                            Ok(mut msg) => {
                                msg.msg_id.topic_offset = frame.offset;
                                if tx.send(Ok(msg)).await.is_err() {
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
                    idx += frame.frame_len;
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
