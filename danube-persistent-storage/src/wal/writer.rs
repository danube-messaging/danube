use super::WalCheckpoint;
use bincode;
use crc32fast;
use danube_core::storage::PersistentStorageError;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, warn};

/// Commands sent from `Wal::append()` (and friends) to the background writer task.
///
/// Rationale
/// - Keep the hot path (`append`) non-blocking by enqueueing a lightweight command.
/// - The writer task owns all I/O state and applies batching, fsync, rotation, and checkpoints.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum LogCommand {
    Write {
        offset: u64,
        bytes: Vec<u8>,
    },
    #[allow(dead_code)]
    Flush,
    #[allow(dead_code)]
    Rotate,
    Shutdown(oneshot::Sender<()>),
}

/// Init parameters for the writer task captured at WAL startup.
///
/// These are copied into `WriterState` and remain constant for the lifetime of the writer task.
pub(crate) struct WriterInit {
    pub wal_path: Option<PathBuf>,
    pub checkpoint_path: Option<PathBuf>,
    pub fsync_interval_ms: u64,
    pub fsync_max_batch_bytes: usize,
    pub rotate_max_bytes: Option<u64>,
    pub rotate_max_seconds: Option<u64>,
}

/// Writer-owned state (no locking). Lives entirely inside the writer task.
///
/// Responsibilities
/// - Buffer frames and periodically flush to disk according to `fsync_interval_ms` and `max_batch_bytes`.
/// - Rotate WAL files based on size/time thresholds (`rotate_max_bytes` / `rotate_max_seconds`).
/// - Persist `WalCheckpoint` atomically on flush to record `last_offset`, `file_seq`, and current file path.
struct WriterState {
    writer: Option<BufWriter<tokio::fs::File>>,
    write_buf: Vec<u8>,
    last_flush: std::time::Instant,
    bytes_in_file: u64,
    file_started: std::time::Instant,
    file_seq: u64,
    wal_path: Option<PathBuf>,
    checkpoint_path: Option<PathBuf>,
    fsync_interval_ms: u64,
    fsync_max_batch_bytes: usize,
    rotate_max_bytes: Option<u64>,
    rotate_max_seconds: Option<u64>,
}

impl WriterState {
    /// Handle a single `Write` command by framing and appending into the in-memory buffer;
    /// flush (write + fsync) if batch/time thresholds are exceeded and write a checkpoint.
    async fn process_write(
        &mut self,
        offset: u64,
        bytes: &[u8],
    ) -> Result<(), PersistentStorageError> {
        self.rotate_if_needed().await?;
        if let Some(writer) = self.writer.as_mut() {
            let len = bytes.len() as u32;
            let crc = crc32fast::hash(bytes);
            self.write_buf.extend_from_slice(&offset.to_le_bytes());
            self.write_buf.extend_from_slice(&len.to_le_bytes());
            self.write_buf.extend_from_slice(&crc.to_le_bytes());
            self.write_buf.extend_from_slice(bytes);

            self.bytes_in_file += (8 + 4 + 4 + bytes.len()) as u64;
            let should_flush_by_bytes = self.write_buf.len() >= self.fsync_max_batch_bytes;
            let should_flush_by_time = self.last_flush.elapsed()
                >= std::time::Duration::from_millis(self.fsync_interval_ms);
            if should_flush_by_bytes || should_flush_by_time {
                writer
                    .write_all(&self.write_buf)
                    .await
                    .map_err(|e| PersistentStorageError::Io(format!("wal write failed: {}", e)))?;
                writer
                    .flush()
                    .await
                    .map_err(|e| PersistentStorageError::Io(format!("wal flush failed: {}", e)))?;
                self.write_buf.clear();
                self.last_flush = std::time::Instant::now();
                self.write_checkpoint(offset).await.ok();
            }
        }
        Ok(())
    }

    /// Force a flush of any buffered frames to disk and update internal timers.
    async fn process_flush(&mut self) -> Result<(), PersistentStorageError> {
        if let Some(writer) = self.writer.as_mut() {
            if !self.write_buf.is_empty() {
                writer
                    .write_all(&self.write_buf)
                    .await
                    .map_err(|e| PersistentStorageError::Io(format!("wal write failed: {}", e)))?;
                writer
                    .flush()
                    .await
                    .map_err(|e| PersistentStorageError::Io(format!("wal flush failed: {}", e)))?;
                self.write_buf.clear();
                self.last_flush = std::time::Instant::now();
            }
        }
        Ok(())
    }

    /// Check rotate thresholds and rotate to a new `wal.<seq>.log` if needed.
    async fn rotate_if_needed(&mut self) -> Result<(), PersistentStorageError> {
        if let Some(max_bytes) = self.rotate_max_bytes {
            if self.bytes_in_file >= max_bytes {
                self.rotate_file().await?;
                return Ok(());
            }
        }
        if let Some(max_secs) = self.rotate_max_seconds {
            if self.file_started.elapsed() >= std::time::Duration::from_secs(max_secs) {
                self.rotate_file().await?;
                return Ok(());
            }
        }
        Ok(())
    }

    /// Open a new rotated file, update path, reset counters and timers.
    async fn rotate_file(&mut self) -> Result<(), PersistentStorageError> {
        self.file_seq += 1;
        let new_name = format!("wal.{}.log", self.file_seq);
        if let Some(mut dirp) = self
            .wal_path
            .as_ref()
            .and_then(|p| p.parent().map(|pp| pp.to_path_buf()))
        {
            dirp.push(&new_name);
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&dirp)
                .await
                .map_err(|e| {
                    PersistentStorageError::Io(format!("open rotated wal file failed: {}", e))
                })?;
            self.writer = Some(BufWriter::new(f));
            self.wal_path = Some(dirp);
            self.bytes_in_file = 0;
            self.file_started = std::time::Instant::now();
            if let Some(ref p) = self.wal_path {
                debug!(target = "wal", seq = self.file_seq, file = %p.display(), "rotated wal file");
            }
        }
        Ok(())
    }

    /// Atomically write `WalCheckpoint` with `bincode` into `<dir>/wal.ckpt` via tmp+rename.
    async fn write_checkpoint(&self, last_offset: u64) -> Result<(), PersistentStorageError> {
        let ckpt_path = match &self.checkpoint_path {
            Some(p) => p.clone(),
            None => return Ok(()),
        };
        let file_path = self
            .wal_path
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        let ckpt = WalCheckpoint {
            last_offset,
            file_seq: self.file_seq,
            file_path,
        };
        let bytes = bincode::serialize(&ckpt).map_err(|e| {
            PersistentStorageError::Io(format!("checkpoint serialize failed: {}", e))
        })?;
        let tmp = ckpt_path.with_extension("ckpt.tmp");
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .await
            .map_err(|e| {
                PersistentStorageError::Io(format!("open checkpoint tmp failed: {}", e))
            })?;
        f.write_all(&bytes)
            .await
            .map_err(|e| PersistentStorageError::Io(format!("write checkpoint failed: {}", e)))?;
        f.flush()
            .await
            .map_err(|e| PersistentStorageError::Io(format!("flush checkpoint failed: {}", e)))?;
        tokio::fs::rename(&tmp, &ckpt_path)
            .await
            .map_err(|e| PersistentStorageError::Io(format!("rename checkpoint failed: {}", e)))?;
        debug!(target = "wal", last_offset, file_seq = self.file_seq, path = %ckpt_path.display(), "wrote wal checkpoint");
        Ok(())
    }
}

/// Background writer task entrypoint.
///
/// Lifecycle
/// - Initializes `WriterState` from `WriterInit` (including opening the active file, if any).
/// - Processes commands until `Shutdown`, flushing on demand and writing checkpoints.
pub(crate) async fn run(init: WriterInit, mut rx: mpsc::Receiver<LogCommand>) {
    let writer = if let Some(p) = &init.wal_path {
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
            .await
            .ok();
        f.map(BufWriter::new)
    } else {
        None
    };

    // Capture fields that may be moved so we can still log derived values
    let has_file = init.wal_path.is_some();

    let mut state = WriterState {
        writer,
        write_buf: Vec::with_capacity(init.fsync_max_batch_bytes),
        last_flush: std::time::Instant::now(),
        bytes_in_file: 0,
        file_started: std::time::Instant::now(),
        file_seq: 0,
        wal_path: init.wal_path,
        checkpoint_path: init.checkpoint_path,
        fsync_interval_ms: init.fsync_interval_ms,
        fsync_max_batch_bytes: init.fsync_max_batch_bytes,
        rotate_max_bytes: init.rotate_max_bytes,
        rotate_max_seconds: init.rotate_max_seconds,
    };

    debug!(target = "wal", has_file = has_file, fsync_ms = init.fsync_interval_ms, max_batch = init.fsync_max_batch_bytes, rotate_bytes = ?init.rotate_max_bytes, rotate_secs = ?init.rotate_max_seconds, "writer task started");
    while let Some(cmd) = rx.recv().await {
        let res = match cmd {
            LogCommand::Write { offset, bytes } => state.process_write(offset, &bytes).await,
            LogCommand::Flush => state.process_flush().await,
            LogCommand::Rotate => state.rotate_if_needed().await,
            LogCommand::Shutdown(ack_tx) => {
                if let Err(e) = state.process_flush().await {
                    warn!(target = "wal", error = ?e, "flush on shutdown failed");
                }
                let _ = ack_tx.send(());
                debug!(target = "wal", "writer task shutting down");
                break;
            }
        };
        if let Err(e) = res {
            warn!(target = "wal", error = ?e, "background writer command failed");
        }
    }
}
