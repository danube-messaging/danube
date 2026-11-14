use crate::checkpoint::{CheckpointStore, WalCheckpoint};
use crate::frames::append_encoded_frame;
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
    WAL_FLUSH_TOTAL,
    WAL_FILE_ROTATE_TOTAL,
};
use std::time::Instant;

/// Controls fsync / flush guarantees on `Wal::append` and `Wal::append_batch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum WalSyncMode {
    /// Enqueue writes to the background writer buffer and flush periodically.
    Async,
    /// Group-commit flush to the OS page cache (`BufWriter::flush()`) before acking writes.
    #[default]
    OsCacheFlush,
    /// Group-commit flush + `file.sync_data()` (`fdatasync`) before acking writes.
    FsyncBatch,
}

/// Commands sent from `Wal::append()` (and friends) to the background writer task.
///
/// Rationale
/// - Keep the hot path (`append`) responsive while coordinating all disk I/O in a single task.
/// - Batching in `writer::run` groups concurrent `Write`s into one flush/fdatasync syscall.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum LogCommand {
    Write {
        offset: u64,
        bytes: Vec<u8>,
        ack_tx: Option<oneshot::Sender<Result<(), String>>>,
    },
    /// Batch write: pre-encoded frames for efficient replication ingestion.
    /// The `frames` buffer contains already-framed data (header + payload for each message).
    WriteBatch {
        first_offset: u64,
        last_offset: u64,
        frames: Vec<u8>,
        ack_tx: Option<oneshot::Sender<Result<(), String>>>,
    },
    /// Remove pruned rotated WAL files and advance `start_offset` atomically under writer ownership
    /// so subsequent writer checkpoints never resurrect deleted files or reset `start_offset`.
    PruneRotatedFiles {
        pruned_seqs: Vec<u64>,
        new_start_offset: u64,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Set the topic name used for labeling writer metrics (optional).
    SetTopic(String),
    #[allow(dead_code)]
    Flush(oneshot::Sender<Result<(), String>>),
    #[allow(dead_code)]
    Rotate(oneshot::Sender<Result<(), String>>),
    Shutdown(oneshot::Sender<Result<(), String>>),
}

/// Notification emitted when the WAL writer rotates the active file.
///
/// Consumers (e.g. Valkey cleanup) can subscribe to these events to learn
/// which offset range just moved from "active" to "sealed on disk".
#[derive(Debug, Clone)]
pub struct RotationEvent {
    /// First offset in the rotated (now sealed) file.
    pub start_offset: u64,
    /// Last offset in the rotated (now sealed) file.
    pub end_offset: u64,
}

/// Init parameters for the writer task captured at WAL startup.
///
/// These are copied into `WriterState` and remain constant for the lifetime of the writer task.
pub(crate) struct WriterInit {
    pub wal_path: Option<PathBuf>,
    pub checkpoint_path: Option<PathBuf>,
    pub flush_interval_ms: u64,
    pub flush_max_batch_bytes: usize,
    pub rotate_max_bytes: Option<u64>,
    pub rotate_max_seconds: Option<u64>,
    pub sync_mode: WalSyncMode,
    pub ckpt_store: Option<Arc<CheckpointStore>>,
    /// Optional channel to notify external systems when the active WAL file rotates.
    pub rotation_tx: Option<mpsc::UnboundedSender<RotationEvent>>,
}

/// Writer-owned state (no locking). Lives entirely inside the writer task.
///
/// Responsibilities
/// - Buffer frames and periodically flush to disk according to `flush_interval_ms` and `flush_max_batch_bytes`.
/// - Perform group-commit `flush` / `sync_data` for writers waiting on `ack_tx`.
/// - Rotate WAL files based on size thresholds and time thresholds checked on writes.
/// - Persist `WalCheckpoint` atomically on flush to record `last_offset`, `file_seq`, and current file path.
struct WriterState {
    writer: Option<BufWriter<tokio::fs::File>>,
    write_buf: Vec<u8>,
    last_flush: std::time::Instant,
    bytes_in_file: u64,
    file_started: std::time::Instant,
    file_seq: u64,
    start_offset: u64,
    wal_path: Option<PathBuf>,
    checkpoint_path: Option<PathBuf>,
    flush_max_batch_bytes: usize,
    rotate_max_bytes: Option<u64>,
    rotate_max_seconds: Option<u64>,
    sync_mode: WalSyncMode,
    rotated_files: Vec<(u64, PathBuf, u64)>,
    ckpt_store: Option<Arc<CheckpointStore>>,
    // Track the last offset written since last checkpoint
    last_offset_written: Option<u64>,
    // Track the first offset written in the current active file
    current_file_first_offset: Option<u64>,
    // Optional topic name for metrics labeling
    topic_name: Option<String>,
    // Optional channel to notify external systems on WAL file rotation
    rotation_tx: Option<mpsc::UnboundedSender<RotationEvent>>,
}

impl WriterState {
    /// Handle a `Write` command by framing and appending into the in-memory buffer.
    /// Does not immediately flush on its own so the batch loop in `run()` can group writes.
    async fn buffer_write(
        &mut self,
        offset: u64,
        bytes: &[u8],
    ) -> Result<(), PersistentStorageError> {
        self.rotate_if_needed().await?;
        if self.current_file_first_offset.is_none() {
            self.current_file_first_offset = Some(offset);
        }
        append_encoded_frame(&mut self.write_buf, offset, bytes);
        self.bytes_in_file += (crate::frames::FRAME_HEADER_SIZE + bytes.len()) as u64;
        self.last_offset_written = Some(offset);
        Ok(())
    }

    /// Handle a `WriteBatch` command by appending pre-encoded frames into the buffer.
    async fn buffer_write_batch(
        &mut self,
        first_offset: u64,
        last_offset: u64,
        frames: &[u8],
    ) -> Result<(), PersistentStorageError> {
        self.rotate_if_needed().await?;
        if self.current_file_first_offset.is_none() {
            self.current_file_first_offset = Some(first_offset);
        }
        self.write_buf.extend_from_slice(frames);
        self.bytes_in_file += frames.len() as u64;
        self.last_offset_written = Some(last_offset);
        Ok(())
    }

    /// Force a flush of any buffered frames to disk (and `sync_data` when configured) and update internal timers.
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
                if self.sync_mode == WalSyncMode::FsyncBatch {
                    writer.get_ref().sync_data().await.map_err(|e| {
                        PersistentStorageError::Io(format!("wal sync_data failed: {}", e))
                    })?;
                }
                self.write_buf.clear();
                self.last_flush = std::time::Instant::now();
                // Metrics: flush latency and flush count
                let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                if let Some(t) = &self.topic_name {
                    histogram!(WAL_FLUSH_LATENCY_MS.name, "topic"=> t.clone()).record(elapsed_ms);
                    counter!(WAL_FLUSH_TOTAL.name, "topic"=> t.clone()).increment(1);
                } else {
                    histogram!(WAL_FLUSH_LATENCY_MS.name).record(elapsed_ms);
                    counter!(WAL_FLUSH_TOTAL.name).increment(1);
                }
                // On explicit flush, also persist a checkpoint if we know a last offset
                if let Some(off) = self.last_offset_written {
                    self.write_checkpoint(off).await.ok();
                }
            }
        }
        Ok(())
    }

    /// Remove pruned rotated files from writer state and advance `start_offset` atomically.
    async fn prune_rotated_files(
        &mut self,
        pruned_seqs: &[u64],
        new_start_offset: u64,
    ) -> Result<(), PersistentStorageError> {
        if !pruned_seqs.is_empty() {
            self.rotated_files
                .retain(|(seq, _path, _first)| !pruned_seqs.contains(seq));
        }
        self.start_offset = self.start_offset.max(new_start_offset);
        if let Some(off) = self.last_offset_written {
            self.write_checkpoint(off).await?;
        } else if let Some(store) = &self.ckpt_store {
            if let Some(mut ckpt) = store.get_wal().await {
                ckpt.rotated_files = self.rotated_files.clone();
                ckpt.start_offset = self.start_offset;
                store.update_wal(&ckpt).await?;
            }
        }
        Ok(())
    }

    /// Force the current buffered state into a sealed rotated file.
    ///
    /// Functional behavior
    /// - Flushes any buffered frames first so the rotated file is complete on disk.
    /// - Skips rotation entirely if the active file never accepted a write.
    /// - Persists a checkpoint after rotation so external readers/exporters can observe the new
    ///   active file boundary.
    async fn force_rotate(&mut self) -> Result<(), PersistentStorageError> {
        self.process_flush().await?;
        if self.current_file_first_offset.is_none() {
            return Ok(());
        }
        self.rotate_file().await?;
        if let Some(off) = self.last_offset_written {
            self.write_checkpoint(off).await?;
        }
        Ok(())
    }

    /// Check rotate thresholds and rotate to a new `wal.<seq>.log` if needed.
    ///
    /// Functional note
    /// - Size rotation is evaluated against bytes accepted into the current active file.
    /// - Time rotation is evaluated only when a new write arrives; there is no separate rotation
    ///   timer that rotates idle files in the background.
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
    ///
    /// The previous active file is first recorded into `rotated_files` together with the first
    /// offset known to belong to that file. That metadata is later used for replay ordering,
    /// retention eligibility, and checkpoint reconstruction.
    async fn rotate_file(&mut self) -> Result<(), PersistentStorageError> {
        // Capture the offset range of the file being rotated BEFORE resetting
        let rotated_start = self.current_file_first_offset;
        let rotated_end = self.last_offset_written;

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

        // Notify external systems (e.g. Valkey cleanup) about the rotation
        if let (Some(start), Some(end)) = (rotated_start, rotated_end) {
            if let Some(tx) = &self.rotation_tx {
                let _ = tx.send(RotationEvent {
                    start_offset: start,
                    end_offset: end,
                });
            }
        }

        Ok(())
    }

    /// Atomically write `WalCheckpoint` with `bincode` into `<dir>/wal.ckpt` via tmp+rename.
    ///
    /// The checkpoint is the writer's summary of local WAL topology: last written offset, active
    /// file identity, and the ordered list of rotated files. Recovery and retention both rely on
    /// this view rather than rescanning the directory structure on every operation.
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
            start_offset: self.start_offset,
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
            return Err(PersistentStorageError::Other(format!(
                "file-backed wal missing checkpoint store for {}",
                ckpt_path.display()
            )));
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
/// - Uses a periodic ticker only for buffer flush/checkpoint advancement; rotation is still checked
///   on writes via `process_write()`.
///
/// Event-loop model
/// - `Write` appends into the in-memory buffer and may trigger immediate flush/rotation by size.
/// - `Flush`, `Rotate`, and `Shutdown` are acknowledged explicitly so callers can observe failure.
/// - Channel closure triggers one final best-effort flush before exit.
pub(crate) async fn run(init: WriterInit, mut rx: mpsc::Receiver<LogCommand>) {
    let writer = if let Some(p) = &init.wal_path {
        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
            .await
        {
            Ok(f) => Some(BufWriter::new(f)),
            Err(e) => {
                warn!(target = "wal", file = %p.display(), error = %e, "failed to open configured wal file; writer task exiting");
                return;
            }
        }
    } else {
        None
    };

    // Capture fields that may be moved so we can still log derived values
    let has_file = init.wal_path.is_some();

    let (initial_start_offset, initial_rotated_files) = match &init.ckpt_store {
        Some(store) => match store.get_wal().await {
            Some(ckpt) => (ckpt.start_offset, ckpt.rotated_files),
            None => (0, Vec::new()),
        },
        None => (0, Vec::new()),
    };

    let mut state = WriterState {
        writer,
        write_buf: Vec::with_capacity(init.flush_max_batch_bytes),
        last_flush: std::time::Instant::now(),
        bytes_in_file: 0,
        file_started: std::time::Instant::now(),
        file_seq: 0,
        start_offset: initial_start_offset,
        wal_path: init.wal_path,
        checkpoint_path: init.checkpoint_path,
        flush_max_batch_bytes: init.flush_max_batch_bytes,
        rotate_max_bytes: init.rotate_max_bytes,
        rotate_max_seconds: init.rotate_max_seconds,
        sync_mode: init.sync_mode,
        rotated_files: initial_rotated_files,
        ckpt_store: init.ckpt_store,
        last_offset_written: None,
        current_file_first_offset: None,
        topic_name: None,
        rotation_tx: init.rotation_tx,
    };

    debug!(target = "wal", has_file = has_file, flush_ms = init.flush_interval_ms, max_batch = init.flush_max_batch_bytes, rotate_bytes = ?init.rotate_max_bytes, rotate_secs = ?init.rotate_max_seconds, "writer task started");
    let mut ticker =
        tokio::time::interval(std::time::Duration::from_millis(init.flush_interval_ms));
    loop {
        tokio::select! {
            maybe_cmd = rx.recv() => {
                match maybe_cmd {
                    Some(first_cmd) => {
                        let mut pending_acks: Vec<oneshot::Sender<Result<(), String>>> = Vec::new();
                        let mut deferred_cmds: Vec<LogCommand> = Vec::new();

                        // Handle first command; if it's a write, greedily batch subsequent queued writes
                        let mut shutdown_requested = false;
                        let mut batch_cmds = vec![first_cmd];
                        while batch_cmds.len() < 256 {
                            match rx.try_recv() {
                                Ok(next_cmd) => batch_cmds.push(next_cmd),
                                Err(_) => break,
                            }
                        }

                        for cmd in batch_cmds {
                            match cmd {
                                LogCommand::Write { offset, bytes, ack_tx } => {
                                    if let Err(e) = state.buffer_write(offset, &bytes).await {
                                        if let Some(tx) = ack_tx {
                                            let _ = tx.send(Err(e.to_string()));
                                        }
                                        warn!(target = "wal", error = %e, "background writer write failed");
                                    } else if let Some(tx) = ack_tx {
                                        pending_acks.push(tx);
                                    }
                                }
                                LogCommand::WriteBatch { first_offset, last_offset, frames, ack_tx } => {
                                    if let Err(e) = state.buffer_write_batch(first_offset, last_offset, &frames).await {
                                        if let Some(tx) = ack_tx {
                                            let _ = tx.send(Err(e.to_string()));
                                        }
                                        warn!(target = "wal", error = %e, "background writer write_batch failed");
                                    } else if let Some(tx) = ack_tx {
                                        pending_acks.push(tx);
                                    }
                                }
                                other => deferred_cmds.push(other),
                            }
                        }

                        // Group-commit flush if any caller awaits durability OR batch threshold reached
                        if !pending_acks.is_empty() || state.write_buf.len() >= state.flush_max_batch_bytes {
                            let flush_res = state.process_flush().await;
                            match flush_res {
                                Ok(()) => {
                                    for tx in pending_acks.drain(..) {
                                        let _ = tx.send(Ok(()));
                                    }
                                }
                                Err(e) => {
                                    warn!(target = "wal", error = %e, "group commit flush failed");
                                    let msg = e.to_string();
                                    for tx in pending_acks.drain(..) {
                                        let _ = tx.send(Err(msg.clone()));
                                    }
                                }
                            }
                        }

                        // Process non-write control commands in order
                        for cmd in deferred_cmds {
                            let res = match cmd {
                                LogCommand::SetTopic(topic) => { state.topic_name = Some(topic); Ok(()) }
                                LogCommand::PruneRotatedFiles { pruned_seqs, new_start_offset, reply_tx } => {
                                    let res = state.prune_rotated_files(&pruned_seqs, new_start_offset).await;
                                    let _ = reply_tx.send(res.as_ref().map(|_| ()).map_err(|e| e.to_string()));
                                    res
                                }
                                LogCommand::Flush(ack_tx) => {
                                    let res = state.process_flush().await;
                                    let ack_res = res.as_ref().map(|_| ()).map_err(|e| e.to_string());
                                    let _ = ack_tx.send(ack_res);
                                    res
                                }
                                LogCommand::Rotate(ack_tx) => {
                                    let res = state.force_rotate().await;
                                    let ack_res = res.as_ref().map(|_| ()).map_err(|e| e.to_string());
                                    let _ = ack_tx.send(ack_res);
                                    res
                                }
                                LogCommand::Shutdown(ack_tx) => {
                                    let res = state.process_flush().await;
                                    if let Err(e) = &res {
                                        warn!(target = "wal", error = ?e, "flush on shutdown failed");
                                    }
                                    let ack_res = res.as_ref().map(|_| ()).map_err(|e| e.to_string());
                                    let _ = ack_tx.send(ack_res);
                                    debug!(target = "wal", "writer task shutting down");
                                    shutdown_requested = true;
                                    res
                                }
                                LogCommand::Write { .. } | LogCommand::WriteBatch { .. } => Ok(()),
                            };
                            if let Err(e) = res {
                                warn!(target = "wal", error = ?e, "background writer command failed");
                            }
                            if shutdown_requested {
                                break;
                            }
                        }
                        if shutdown_requested {
                            break;
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
