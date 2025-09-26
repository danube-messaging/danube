use bincode;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};
// serde_json no longer used for checkpoints; using bincode for compactness
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};
use tracing::{info, warn};

// Submodules for writer and reader paths
mod cache;
mod checkpoints;
mod reader;
mod writer;
use cache::Cache;
use writer::{LogCommand, WriterInit};

// Re-export for external users: crate::wal::{Wal, UploaderCheckpoint}
pub use checkpoints::UploaderCheckpoint;

/// Write-Ahead Log (WAL) with:
/// - In-memory ordered replay cache
/// - CRC32-protected frames `[u64 offset][u32 len][u32 crc][bytes]`
/// - Batched writes with periodic fsync
/// - Optional rotation by size/time and durable checkpoints
/// - Replay from file + cache and live tail via broadcast channel
///
/// Cloning `Wal` is cheap; all state is held in `Arc<WalInner>`.
#[derive(Debug, Clone)]
pub struct Wal {
    inner: Arc<WalInner>,
}

#[derive(Debug)]
struct WalInner {
    next_offset: AtomicU64,
    tx: broadcast::Sender<(u64, StreamMessage)>,
    wal_path: Mutex<Option<PathBuf>>,
    // In-memory ordered cache for replay (offset -> message)
    cache: Mutex<Cache>,
    cache_capacity: usize,
    fsync_interval_ms: u64,
    fsync_max_batch_bytes: usize,
    // Rotation state
    rotate_max_bytes: Option<u64>,
    rotate_max_seconds: Option<u64>,
    // Checkpoint path
    checkpoint_path: Option<PathBuf>,
    // Background writer command channel (hot path enqueues; background task performs IO)
    cmd_tx: mpsc::Sender<LogCommand>,
}

#[derive(Debug, Clone, Default)]
pub struct WalConfig {
    /// Root directory for the WAL. When `None`, the WAL operates in memory-only mode
    /// (no file durability and no checkpoint files).
    ///
    /// Default: `None` (in-memory only)
    pub dir: Option<PathBuf>,

    /// Base file name for the active WAL when rotation is disabled. Combined with `dir`
    /// to form `<dir>/<file_name>`. Ignored for rotated files (which use `wal.<seq>.log`).
    ///
    /// Default when `None`: `"wal.log"`
    pub file_name: Option<String>,

    /// Maximum number of recent messages to retain in the in-memory replay cache.
    /// The cache is ordered by offset and older entries are evicted first when capacity
    /// is exceeded.
    ///
    /// Default when `None`: `1024` messages
    pub cache_capacity: Option<usize>,

    /// Maximum time between flushes (ms) for the background writer. A flush is triggered
    /// if either this interval elapses or `fsync_max_batch_bytes` is reached, whichever comes first.
    ///
    /// Default when `None`: `1_000` ms (1 s)
    pub fsync_interval_ms: Option<u64>,

    /// Maximum buffered bytes in the writer before forcing a flush. This bounds write latency
    /// and memory usage for the write buffer.
    ///
    /// Default when `None`: `10 * 1024 * 1024` bytes (10 MiB)
    pub fsync_max_batch_bytes: Option<usize>,

    /// Size-based rotation threshold in bytes. When set, the writer rotates to a new
    /// `wal.<seq>.log` file after at least this many bytes have been written to the current file.
    ///
    /// Default when `None`: rotation by size is disabled
    pub rotate_max_bytes: Option<u64>,

    /// Time-based rotation threshold in seconds. When set, the writer rotates to a new
    /// `wal.<seq>.log` if the current file has been open longer than this duration, even if
    /// the size threshold hasn't been reached (useful for low-traffic topics and operational hygiene).
    ///
    /// Default when `None`: rotation by time is disabled
    pub rotate_max_seconds: Option<u64>,
}

impl WalConfig {
    /// Resolve full path to the active WAL file (e.g., `<dir>/<file_name>`), if a directory is configured.
    fn wal_file_path(&self) -> Option<PathBuf> {
        let dir = self.dir.as_ref()?;
        let name = self
            .file_name
            .clone()
            .unwrap_or_else(|| "wal.log".to_string());
        Some(dir.join(name))
    }
    /// Return the configured WAL directory, if any.
    fn wal_dir(&self) -> Option<PathBuf> {
        self.dir.clone()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(super) struct WalCheckpoint {
    last_offset: u64,
    file_seq: u64,
    file_path: String,
}

impl Default for Wal {
    fn default() -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        let (cmd_tx, cmd_rx) = mpsc::channel(8192);
        let wal = Self {
            inner: Arc::new(WalInner {
                next_offset: AtomicU64::new(0),
                tx,
                wal_path: Mutex::new(None),
                cache: Mutex::new(Cache::new()),
                cache_capacity: 1024,
                fsync_interval_ms: 1_000, // 1s default flush interval
                fsync_max_batch_bytes: 10 * 1024 * 1024, // 10 MiB default batch
                rotate_max_bytes: None,
                rotate_max_seconds: None,
                checkpoint_path: None,
                cmd_tx,
            }),
        };
        // Spawn background writer task
        let init = WriterInit {
            wal_path: None,
            checkpoint_path: None,
            fsync_interval_ms: wal.inner.fsync_interval_ms,
            fsync_max_batch_bytes: wal.inner.fsync_max_batch_bytes,
            rotate_max_bytes: wal.inner.rotate_max_bytes,
            rotate_max_seconds: wal.inner.rotate_max_seconds,
        };
        tokio::spawn(async move {
            writer::run(init, cmd_rx).await;
        });
        wal
    }
}

impl Wal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a WAL using an optional file for durability.
    ///
    /// Behavior
    /// - If `cfg.dir` is set, ensures the directory exists and prepares the active file path.
    /// - Spawns a background writer task (`writer::run`) that owns I/O state and services `LogCommand`s.
    /// - Initializes in-memory cache and broadcast channel for live tailing.
    ///
    /// Returns
    /// - `Ok(Wal)` ready for `append()` and `tail_reader()`; I/O happens in the background task.
    pub async fn with_config(cfg: WalConfig) -> Result<Self, PersistentStorageError> {
        let (tx, _rx) = broadcast::channel(1024);
        let (cmd_tx, cmd_rx) = mpsc::channel(8192);
        // Build optional file and wal_path without moving the Option twice
        let wal_path_opt = if let Some(path) = cfg.wal_file_path() {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    PersistentStorageError::Io(format!("create wal dir failed: {}", e))
                })?;
            }
            info!(
                target = "wal",
                wal_file = %path.display(),
                "initialized WAL file"
            );
            Some(path)
        } else {
            warn!(
                target = "wal",
                "WAL configured without a directory: operating in memory-only mode (no durability)"
            );
            None
        };

        let capacity = cfg.cache_capacity.unwrap_or(1024);
        let fsync_interval_ms = cfg.fsync_interval_ms.unwrap_or(1_000); // 1s default
        let fsync_max_batch_bytes = cfg.fsync_max_batch_bytes.unwrap_or(10 * 1024 * 1024); // 10 MiB default
        let rotate_max_bytes = cfg.rotate_max_bytes;
        let rotate_max_seconds = cfg.rotate_max_seconds;
        let checkpoint_path = cfg.wal_dir().map(|mut d| {
            d.push("wal.ckpt");
            d
        });

        // Log effective configuration for visibility
        if let Some(dir) = cfg.wal_dir() {
            info!(
                target = "wal",
                wal_dir = %dir.display(),
                cache_capacity = capacity,
                fsync_interval_ms,
                fsync_max_batch_bytes,
                rotate_max_bytes = rotate_max_bytes.unwrap_or(0),
                rotate_max_seconds = rotate_max_seconds.unwrap_or(0),
                checkpoint = %checkpoint_path.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "<none>".to_string()),
                "WAL configuration applied"
            );
        } else {
            info!(
                target = "wal",
                cache_capacity = capacity,
                fsync_interval_ms,
                fsync_max_batch_bytes,
                "WAL configuration applied (no dir)"
            );
        }

        let wal = Self {
            inner: Arc::new(WalInner {
                next_offset: AtomicU64::new(0),
                tx,
                wal_path: Mutex::new(wal_path_opt),
                cache: Mutex::new(Cache::new()),
                cache_capacity: capacity,
                fsync_interval_ms,
                fsync_max_batch_bytes,
                rotate_max_bytes,
                rotate_max_seconds,
                checkpoint_path,
                cmd_tx,
            }),
        };
        // Spawn background writer task
        let wal_path_for_init = wal.inner.wal_path.lock().await.clone();
        let init = WriterInit {
            wal_path: wal_path_for_init,
            checkpoint_path: wal.inner.checkpoint_path.clone(),
            fsync_interval_ms,
            fsync_max_batch_bytes,
            rotate_max_bytes,
            rotate_max_seconds,
        };
        tokio::spawn(async move {
            writer::run(init, cmd_rx).await;
        });
        Ok(wal)
    }

    /// Append a message and return the assigned offset.
    ///
    /// What happens
    /// - Atomically assigns the next offset and inserts the message into the in-memory cache (evicting if needed).
    /// - Enqueues a `LogCommand::Write { offset, bytes }` to the background writer (non-blocking hot path).
    /// - Broadcasts `(offset, message)` to live tailing readers.
    ///
    /// Durability
    /// - The background writer batches frames and fsyncs periodically; rotation/checkpointing handled there.
    /// - On-disk frame layout: `[u64 offset][u32 len][u32 crc][bytes]` with CRC32 over `bytes`.
    pub async fn append(&self, msg: &StreamMessage) -> Result<u64, PersistentStorageError> {
        let offset = self.inner.next_offset.fetch_add(1, Ordering::AcqRel);
        // Serialize the full message for durability and enqueue to background writer
        let bytes = bincode::serialize(msg)
            .map_err(|e| PersistentStorageError::Io(format!("bincode serialize failed: {}", e)))?;
        // Update in-memory cache with single lock and evict oldest if over capacity
        {
            let mut cache = self.inner.cache.lock().await;
            cache.insert(offset, msg.clone());
            cache.evict_to(self.inner.cache_capacity);
        }

        // Enqueue write command (non-blocking I/O path)
        if let Err(_e) = self
            .inner
            .cmd_tx
            .send(LogCommand::Write { offset, bytes })
            .await
        {
            return Err(PersistentStorageError::Other(
                "wal writer channel closed".to_string(),
            ));
        }

        // Notify tailing readers
        if let Err(e) = self.inner.tx.send((offset, msg.clone())) {
            warn!(
                target = "wal",
                offset = offset,
                error = %e,
                "failed to broadcast message to live readers; a consumer may be lagging"
            );
        }
        Ok(offset)
    }

    /// Create a reader stream starting from a given offset.
    ///
    /// Replay semantics
    /// - Replays any persisted (file) and cached messages with offsets `>= from_offset` (already ordered, no dedupe needed).
    /// - Then switches to live tail using the internal broadcast channel.
    ///
    /// Implementation note
    /// - This is a thin wrapper that snapshots inputs and delegates to `wal/reader.rs::build_tail_stream`.
    pub async fn tail_reader(
        &self,
        from_offset: u64,
    ) -> Result<TopicStream, PersistentStorageError> {
        // Thin wrapper: snapshot inputs and delegate heavy lifting to reader::build_tail_stream
        let wal_path_opt = self.inner.wal_path.lock().await.clone();
        let cache_snapshot: Vec<(u64, StreamMessage)> = {
            let cache = self.inner.cache.lock().await;
            cache
                .range_from(0)
                .map(|(off, msg)| (off, msg.clone()))
                .collect()
        };
        let rx = self.inner.tx.subscribe();
        reader::build_tail_stream(wal_path_opt, cache_snapshot, from_offset, rx).await
    }

    /// Snapshot cached messages with offsets `>= after_offset`.
    ///
    /// Returns
    /// - `(items, watermark)` where `items` are `(offset, message)` pairs and `watermark` is the highest offset seen.
    pub async fn read_cached_since(
        &self,
        after_offset: u64,
    ) -> Result<(Vec<(u64, StreamMessage)>, u64), PersistentStorageError> {
        let cache = self.inner.cache.lock().await;
        let mut items = Vec::new();
        let mut watermark = after_offset;
        for (off, msg) in cache.range_from(after_offset) {
            items.push((off, msg.clone()));
            if off > watermark {
                watermark = off;
            }
        }
        Ok((items, watermark))
    }

    /// Return the next offset that will be assigned on append (i.e., current tip + 1).
    pub fn current_offset(&self) -> u64 {
        self.inner.next_offset.load(Ordering::Acquire)
    }

    // Reader helpers moved to wal/reader.rs

    // Writer rotation and checkpoints are handled inside WriterState; no-op stubs retained for compatibility

    /// Compute the uploader checkpoint path if WAL checkpoints are enabled.
    async fn uploader_checkpoint_path(&self) -> Option<PathBuf> {
        let ckpt = self.inner.checkpoint_path.clone()?;
        let parent = ckpt.parent()?.to_path_buf();
        Some(parent.join("uploader.ckpt"))
    }

    /// Persist uploader checkpoint to `uploader.ckpt`.
    pub async fn write_uploader_checkpoint(
        &self,
        ckpt: &UploaderCheckpoint,
    ) -> Result<(), PersistentStorageError> {
        let path = match self.uploader_checkpoint_path().await {
            Some(p) => p,
            None => return Ok(()),
        };
        ckpt.write_to_path(&path).await
    }

    /// Read uploader checkpoint from `uploader.ckpt` if present.
    pub async fn read_uploader_checkpoint(
        &self,
    ) -> Result<Option<UploaderCheckpoint>, PersistentStorageError> {
        let path = match self.uploader_checkpoint_path().await {
            Some(p) => p,
            None => return Ok(None),
        };
        UploaderCheckpoint::read_from_path(&path).await
    }

    // Writer task implemented in wal/writer.rs (spawned from with_config/default)

    /// Graceful shutdown: flush pending buffered data and stop the background writer task.
    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        // Ignore send error if writer already stopped
        let _ = self.inner.cmd_tx.send(LogCommand::Shutdown(tx)).await;
        // Await ack; ignore error if task already gone
        let _ = rx.await;
    }
}
