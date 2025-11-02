use crate::checkpoint::{CheckPoint, CheckpointStore, WalCheckpoint};
use crc32fast;
use danube_core::storage::PersistentStorageError;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, warn};
use metrics::{counter, histogram};
use crate::persistent_metrics::{
    WAL_FLUSH_LATENCY_MS,
    WAL_FSYNC_TOTAL,
    WAL_FILE_ROTATE_TOTAL,
};
use std::time::Instant;

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
    /// Set the topic name used for labeling writer metrics (optional).
    SetTopic(String),
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
    pub ckpt_store: Option<Arc<CheckpointStore>>,
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
    fsync_max_batch_bytes: usize,
    rotate_max_bytes: Option<u64>,
    rotate_max_seconds: Option<u64>,
    rotated_files: Vec<(u64, PathBuf, u64)>,
    ckpt_store: Option<Arc<CheckpointStore>>,
    // Track the last offset written since last checkpoint
    last_offset_written: Option<u64>,
    // Track the first offset written in the current active file
    current_file_first_offset: Option<u64>,
    // Optional topic name for metrics labeling
    topic_name: Option<String>,
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
        // Record first offset for the current active file if not recorded yet
        if self.current_file_first_offset.is_none() {
            self.current_file_first_offset = Some(offset);
        }
        // Frame and buffer the entry. We avoid borrowing the writer here so we can call
        // `process_flush()` (which accesses `self.writer`) without overlapping borrows.
        let len = bytes.len() as u32;
        let crc = crc32fast::hash(bytes);
        self.write_buf.extend_from_slice(&offset.to_le_bytes());
        self.write_buf.extend_from_slice(&len.to_le_bytes());
        self.write_buf.extend_from_slice(&crc.to_le_bytes());
        self.write_buf.extend_from_slice(bytes);

        self.bytes_in_file += (8 + 4 + 4 + bytes.len()) as u64;
        // Remember last offset appended
        self.last_offset_written = Some(offset);

        // Time-based flushing is handled by the periodic ticker in run(); only enforce byte threshold here
        let should_flush_by_bytes = self.write_buf.len() >= self.fsync_max_batch_bytes;
        if should_flush_by_bytes {
            // This will write the buffer, fsync, clear, update timers, and persist a checkpoint
            // using `last_offset_written`.
            self.process_flush().await?;
        }
        Ok(())
    }

    /// Force a flush of any buffered frames to disk and update internal timers.
    async fn process_flush(&mut self) -> Result<(), PersistentStorageError> {
        if let Some(writer) = self.writer.as_mut() {
            if !self.write_buf.is_empty() {
                let start = Instant::now();
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
                // Metrics: flush latency and fsync count
                let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                if let Some(t) = &self.topic_name {
                    histogram!(WAL_FLUSH_LATENCY_MS.name, "topic"=> t.clone()).record(elapsed_ms);
                    counter!(WAL_FSYNC_TOTAL.name, "topic"=> t.clone()).increment(1);
                } else {
                    histogram!(WAL_FLUSH_LATENCY_MS.name).record(elapsed_ms);
                    counter!(WAL_FSYNC_TOTAL.name).increment(1);
                }
                // On explicit flush, also persist a checkpoint if we know a last offset
                if let Some(off) = self.last_offset_written {
                    self.write_checkpoint(off).await.ok();
                }
            }
        }
        Ok(())
    }

    /// Check rotate thresholds and rotate to a new `wal.<seq>.log` if needed.
    async fn rotate_if_needed(&mut self) -> Result<(), PersistentStorageError> {
        if let Some(max_bytes) = self.rotate_max_bytes {
            if self.bytes_in_file >= max_bytes {
                if let Some(t) = &self.topic_name {
                    counter!(WAL_FILE_ROTATE_TOTAL.name, "topic"=> t.clone(), "reason"=> "size").increment(1);
                } else {
                    counter!(WAL_FILE_ROTATE_TOTAL.name, "reason"=> "size").increment(1);
                }
                self.rotate_file().await?;
                return Ok(());
            }
        }
        if let Some(max_secs) = self.rotate_max_seconds {
            if self.file_started.elapsed() >= std::time::Duration::from_secs(max_secs) {
                if let Some(t) = &self.topic_name {
                    counter!(WAL_FILE_ROTATE_TOTAL.name, "topic"=> t.clone(), "reason"=> "time").increment(1);
                } else {
                    counter!(WAL_FILE_ROTATE_TOTAL.name, "reason"=> "time").increment(1);
                }
                self.rotate_file().await?;
                return Ok(());
            }
        }
        Ok(())
    }

    /// Open a new rotated file, update path, reset counters and timers.
    async fn rotate_file(&mut self) -> Result<(), PersistentStorageError> {
        // Record the previous file into rotation history before switching
        if let Some(prev_path) = self.wal_path.clone() {
            if let Some(first_off) = self.current_file_first_offset {
                self.rotated_files.push((self.file_seq, prev_path, first_off));
            } else {
                // If no writes happened, treat first_offset as last_offset_written or 0
                let first_off = self.last_offset_written.unwrap_or(0);
                self.rotated_files.push((self.file_seq, prev_path, first_off));
            }
        }
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
            // Reset first offset for the new active file; will be set on first write
            self.current_file_first_offset = None;
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
            start_offset: 0, // TODO: This should be updated when pruning is implemented
            last_offset,
            file_seq: self.file_seq,
            file_path,
            rotated_files: self.rotated_files.clone(),
            active_file_name: self
                .wal_path
                .as_ref()
                .and_then(|p| p.file_name().map(|s| s.to_string_lossy().into_owned())),
            last_rotation_at: None,
            active_file_first_offset: self.current_file_first_offset,
        };
        if let Some(store) = &self.ckpt_store {
            // Update cache and persist via store
            store.update_wal(&ckpt).await?;
        } else {
            // Fallback to direct file write
            CheckPoint::write_wal_to_path(&ckpt, &ckpt_path).await?;
        }
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
        fsync_max_batch_bytes: init.fsync_max_batch_bytes,
        rotate_max_bytes: init.rotate_max_bytes,
        rotate_max_seconds: init.rotate_max_seconds,
        rotated_files: Vec::new(),
        ckpt_store: init.ckpt_store,
        last_offset_written: None,
        current_file_first_offset: None,
        topic_name: None,
    };

    debug!(target = "wal", has_file = has_file, fsync_ms = init.fsync_interval_ms, max_batch = init.fsync_max_batch_bytes, rotate_bytes = ?init.rotate_max_bytes, rotate_secs = ?init.rotate_max_seconds, "writer task started");
    let mut ticker =
        tokio::time::interval(std::time::Duration::from_millis(init.fsync_interval_ms));
    loop {
        tokio::select! {
            maybe_cmd = rx.recv() => {
                match maybe_cmd {
                    Some(cmd) => {
                        let res = match cmd {
                            LogCommand::Write { offset, bytes } => state.process_write(offset, &bytes).await,
                            LogCommand::SetTopic(topic) => { state.topic_name = Some(topic); Ok(()) },
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
                    None => {
                        // Sender dropped; perform a final flush and exit
                        if let Err(e) = state.process_flush().await {
                            warn!(target = "wal", error = ?e, "final flush failed after channel closed");
                        }
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                // Time-based flush to ensure checkpoints advance even without new writes
                if let Err(e) = state.process_flush().await {
                    warn!(target = "wal", error = ?e, "periodic flush failed");
                }
            }
        }
    }
}
