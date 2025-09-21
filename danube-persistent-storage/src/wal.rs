use bincode;
use crc32fast;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};
use serde_json;
use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

/// Minimal WAL with in-memory replay cache, CRC32C-protected frames, batched fsync, file replay,
/// rotation and checkpoints.
#[derive(Debug, Clone)]
pub struct Wal {
    inner: Arc<WalInner>,
}

#[derive(Debug)]
struct WalInner {
    next_offset: AtomicU64,
    tx: broadcast::Sender<(u64, StreamMessage)>,
    // Optional file for durability; guarded by a mutex for append writes
    file: Mutex<Option<tokio::fs::File>>,
    wal_path: Mutex<Option<PathBuf>>,
    // In-memory ring buffer for replay (offset, message)
    cache: Mutex<VecDeque<(u64, StreamMessage)>>,
    cache_capacity: usize,
    // Batched write buffer and timing
    write_buf: Mutex<Vec<u8>>,
    last_flush: Mutex<std::time::Instant>,
    fsync_interval_ms: u64,
    max_batch_bytes: usize,
    // Rotation state
    rotate_max_bytes: Option<u64>,
    rotate_max_seconds: Option<u64>,
    current_file_bytes: Mutex<u64>,
    current_file_started: Mutex<std::time::Instant>,
    file_seq: Mutex<u64>,
    // Checkpoint path
    checkpoint_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Default)]
pub struct WalConfig {
    pub dir: Option<PathBuf>,
    pub file_name: Option<String>, // default: wal.log (used when no rotation)
    pub cache_capacity: Option<usize>, // default: 1024
    pub fsync_interval_ms: Option<u64>,
    pub max_batch_bytes: Option<usize>,
    pub rotate_max_bytes: Option<u64>, // when set, rotate file after this many bytes appended
    pub rotate_max_seconds: Option<u64>, // when set, rotate file after this many seconds
}

impl WalConfig {
    fn wal_file_path(&self) -> Option<PathBuf> {
        let dir = self.dir.as_ref()?;
        let name = self
            .file_name
            .clone()
            .unwrap_or_else(|| "wal.log".to_string());
        Some(dir.join(name))
    }
    fn wal_dir(&self) -> Option<PathBuf> {
        self.dir.clone()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct WalCheckpoint {
    last_offset: u64,
    file_seq: u64,
    file_path: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UploaderCheckpoint {
    pub last_committed_offset: u64,
    pub last_object_id: Option<String>,
    pub updated_at: u64,
}

impl Default for Wal {
    fn default() -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        Self {
            inner: Arc::new(WalInner {
                next_offset: AtomicU64::new(0),
                tx,
                file: Mutex::new(None),
                wal_path: Mutex::new(None),
                cache: Mutex::new(VecDeque::with_capacity(1024)),
                cache_capacity: 1024,
                write_buf: Mutex::new(Vec::with_capacity(1024 * 8)),
                last_flush: Mutex::new(std::time::Instant::now()),
                fsync_interval_ms: 5,
                max_batch_bytes: 8 * 1024, // 8 KiB default batch
                rotate_max_bytes: None,
                rotate_max_seconds: None,
                current_file_bytes: Mutex::new(0),
                current_file_started: Mutex::new(std::time::Instant::now()),
                file_seq: Mutex::new(0),
                checkpoint_path: None,
            }),
        }
    }
}

impl Wal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a WAL using an optional file for durability.
    pub async fn with_config(cfg: WalConfig) -> Result<Self, PersistentStorageError> {
        let (tx, _rx) = broadcast::channel(1024);
        // Build optional file and wal_path without moving the Option twice
        let (file_opt, wal_path_opt) = if let Some(path) = cfg.wal_file_path() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).await.map_err(|e| {
                    PersistentStorageError::Io(format!("create wal dir failed: {}", e))
                })?;
            }
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await
                .map_err(|e| PersistentStorageError::Io(format!("open wal file failed: {}", e)))?;
            (Some(f), Some(path))
        } else {
            (None, None)
        };

        let capacity = cfg.cache_capacity.unwrap_or(1024);
        let fsync_interval_ms = cfg.fsync_interval_ms.unwrap_or(5);
        let max_batch_bytes = cfg.max_batch_bytes.unwrap_or(8 * 1024);
        let rotate_max_bytes = cfg.rotate_max_bytes;
        let rotate_max_seconds = cfg.rotate_max_seconds;
        let checkpoint_path = cfg.wal_dir().map(|mut d| {
            d.push("wal.ckpt");
            d
        });

        Ok(Self {
            inner: Arc::new(WalInner {
                next_offset: AtomicU64::new(0),
                tx,
                file: Mutex::new(file_opt),
                wal_path: Mutex::new(wal_path_opt),
                cache: Mutex::new(VecDeque::with_capacity(capacity)),
                cache_capacity: capacity,
                write_buf: Mutex::new(Vec::with_capacity(max_batch_bytes)),
                last_flush: Mutex::new(std::time::Instant::now()),
                fsync_interval_ms,
                max_batch_bytes,
                rotate_max_bytes,
                rotate_max_seconds,
                current_file_bytes: Mutex::new(0),
                current_file_started: Mutex::new(std::time::Instant::now()),
                file_seq: Mutex::new(0),
                checkpoint_path,
            }),
        })
    }

    /// Append a message and return the assigned offset.
    /// Live readers will be notified via broadcast. If a file is configured, append to it with
    /// batched writes and periodic fsync. On-disk frame: [u64 offset][u32 len][u32 crc][bytes].
    pub async fn append(&self, msg: &StreamMessage) -> Result<u64, PersistentStorageError> {
        let offset = self.inner.next_offset.fetch_add(1, Ordering::AcqRel);

        // Serialize the full message for durability
        let bytes = bincode::serialize(msg)
            .map_err(|e| PersistentStorageError::Io(format!("bincode serialize failed: {}", e)))?;
        let len = bytes.len() as u32;
        let crc = crc32fast::hash(&bytes);

        // Rotation must be checked outside of the file mutex to avoid deadlocks
        self.rotate_if_needed().await?;

        // If file configured, stage into write buffer and flush per policy
        {
            let mut file_guard = self.inner.file.lock().await;
            if let Some(file) = file_guard.as_mut() {
                let mut buf = self.inner.write_buf.lock().await;
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(&crc.to_le_bytes());
                buf.extend_from_slice(&bytes);

                // Track bytes for rotation
                {
                    let mut cur = self.inner.current_file_bytes.lock().await;
                    *cur += (8 + 4 + 4 + bytes.len()) as u64;
                }

                let should_flush_by_bytes = buf.len() >= self.inner.max_batch_bytes;
                let mut last_flush = self.inner.last_flush.lock().await;
                let should_flush_by_time = last_flush.elapsed()
                    >= std::time::Duration::from_millis(self.inner.fsync_interval_ms);

                if should_flush_by_bytes || should_flush_by_time {
                    file.write_all(&buf).await.map_err(|e| {
                        PersistentStorageError::Io(format!("wal write failed: {}", e))
                    })?;
                    file.flush().await.map_err(|e| {
                        PersistentStorageError::Io(format!("wal flush failed: {}", e))
                    })?;
                    buf.clear();
                    *last_flush = std::time::Instant::now();
                    // Write checkpoint after a flush
                    self.write_checkpoint(offset).await.ok();
                }
            }
        }

        // Update in-memory cache
        {
            let mut cache = self.inner.cache.lock().await;
            cache.push_back((offset, msg.clone()))
        }
        // Evict if over capacity
        {
            let mut cache = self.inner.cache.lock().await;
            while cache.len() > self.inner.cache_capacity {
                cache.pop_front();
            }
        }

        // Notify tailing readers
        let _ = self.inner.tx.send((offset, msg.clone()));
        Ok(offset)
    }

    /// Create a reader stream starting from the given offset.
    /// First yields any persisted (file) + cached messages with offset >= from_offset (deduped), then switches to live tailing.
    pub async fn tail_reader(
        &self,
        from_offset: u64,
    ) -> Result<TopicStream, PersistentStorageError> {
        // Read from file if present
        let wal_path_opt = self.inner.wal_path.lock().await.clone();
        let mut replay_items: Vec<(u64, StreamMessage)> = match &wal_path_opt {
            Some(path) => self.read_file_since(path, from_offset).await?,
            None => Vec::new(),
        };
        let mut seen: HashSet<u64> = replay_items.iter().map(|(o, _)| *o).collect();

        // Merge cached items beyond from_offset, avoiding duplicates
        {
            let cache = self.inner.cache.lock().await;
            for (off, msg) in cache.iter() {
                if *off >= from_offset && !seen.contains(off) {
                    replay_items.push((*off, msg.clone()));
                    seen.insert(*off);
                }
            }
        }
        // Sort by offset to guarantee ordering
        replay_items.sort_by_key(|(o, _)| *o);

        // Compute live watermark BEFORE moving replay_items
        let start_from_live = match replay_items.last() {
            Some((last, _)) => last.saturating_add(1),
            None => from_offset,
        };

        let replay_stream = tokio_stream::iter(replay_items.into_iter().map(|(_, msg)| Ok(msg)));

        // Live tailing from broadcast, starting after last_replayed if any replay happened,
        // otherwise start exactly at from_offset to include the very next append.
        let rx = self.inner.tx.subscribe();
        let live_stream = BroadcastStream::new(rx).filter_map(move |item| match item {
            Ok((off, msg)) if off >= start_from_live => Some(Ok(msg)),
            Ok(_) => None, // skip older
            Err(e) => Some(Err(PersistentStorageError::Other(format!(
                "broadcast error: {}",
                e
            )))),
        });

        // Chain replay then live
        let stream = replay_stream.chain(live_stream);
        Ok(Box::pin(stream))
    }

    /// Snapshot cached messages with offset greater than or equal to `after_offset`.
    /// Returns the collected items and the highest offset observed (watermark).
    pub async fn read_cached_since(
        &self,
        after_offset: u64,
    ) -> Result<(Vec<(u64, StreamMessage)>, u64), PersistentStorageError> {
        let cache = self.inner.cache.lock().await;
        let mut items = Vec::new();
        let mut watermark = after_offset;
        for (off, msg) in cache.iter() {
            if *off >= after_offset {
                items.push((*off, msg.clone()));
                if *off > watermark {
                    watermark = *off;
                }
            }
        }
        Ok((items, watermark))
    }

    /// Return the next offset that will be assigned on append (i.e., current tip + 1).
    pub fn current_offset(&self) -> u64 {
        self.inner.next_offset.load(Ordering::Acquire)
    }

    /// Read and decode WAL frames from file at `path` with offset >= `from_offset`.
    async fn read_file_since(
        &self,
        path: &PathBuf,
        from_offset: u64,
    ) -> Result<Vec<(u64, StreamMessage)>, PersistentStorageError> {
        let mut f = tokio::fs::File::open(path)
            .await
            .map_err(|e| PersistentStorageError::Io(format!("open wal file failed: {}", e)))?;
        let mut out = Vec::new();
        let mut header = [0u8; 8 + 4 + 4];
        loop {
            // Read header
            match tokio::io::AsyncReadExt::read_exact(&mut f, &mut header).await {
                Ok(_) => {}
                Err(e) => {
                    // EOF or error
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    return Err(PersistentStorageError::Io(format!(
                        "wal read header failed: {}",
                        e
                    )));
                }
            }
            let off = u64::from_le_bytes(header[0..8].try_into().unwrap());
            let len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(header[12..16].try_into().unwrap());

            let mut buf = vec![0u8; len];
            tokio::io::AsyncReadExt::read_exact(&mut f, &mut buf)
                .await
                .map_err(|e| PersistentStorageError::Io(format!("wal read frame failed: {}", e)))?;
            let actual_crc = crc32fast::hash(&buf);
            if actual_crc != crc {
                // Truncate on CRC mismatch: treat as end-of-log for safety
                break;
            }
            if off >= from_offset {
                let msg: StreamMessage = bincode::deserialize(&buf).map_err(|e| {
                    PersistentStorageError::Io(format!("bincode deserialize failed: {}", e))
                })?;
                out.push((off, msg));
            }
        }
        Ok(out)
    }

    async fn rotate_if_needed(&self) -> Result<(), PersistentStorageError> {
        // Check size
        if let Some(max_bytes) = self.inner.rotate_max_bytes {
            let cur = *self.inner.current_file_bytes.lock().await;
            if cur >= max_bytes {
                self.rotate_file().await?;
                return Ok(());
            }
        }
        // Check time
        if let Some(max_secs) = self.inner.rotate_max_seconds {
            let started = *self.inner.current_file_started.lock().await;
            if started.elapsed() >= std::time::Duration::from_secs(max_secs) {
                self.rotate_file().await?;
                return Ok(());
            }
        }
        Ok(())
    }

    async fn rotate_file(&self) -> Result<(), PersistentStorageError> {
        // Close current (implicitly by dropping), open new wal.<seq>.log
        let mut seq = self.inner.file_seq.lock().await;
        *seq += 1;
        let new_name = format!("wal.{}.log", *seq);
        let dir = {
            let lock = self.inner.wal_path.lock().await;
            match &*lock {
                Some(p) => p.parent().map(|p| p.to_path_buf()),
                None => None,
            }
        };
        if let Some(mut dirp) = dir {
            dirp.push(&new_name);
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&dirp)
                .await
                .map_err(|e| {
                    PersistentStorageError::Io(format!("open rotated wal file failed: {}", e))
                })?;
            // swap in new file and path
            {
                let mut file = self.inner.file.lock().await;
                *file = Some(f);
            }
            {
                let mut path_lock = self.inner.wal_path.lock().await;
                *path_lock = Some(dirp);
            }
            // reset counters
            *self.inner.current_file_bytes.lock().await = 0;
            *self.inner.current_file_started.lock().await = std::time::Instant::now();
        }
        Ok(())
    }

    async fn write_checkpoint(&self, last_offset: u64) -> Result<(), PersistentStorageError> {
        let ckpt_path = match &self.inner.checkpoint_path {
            Some(p) => p.clone(),
            None => return Ok(()),
        };
        let file_seq = *self.inner.file_seq.lock().await;
        let file_path = {
            let lock = self.inner.wal_path.lock().await;
            lock.as_ref()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default()
        };
        let ckpt = WalCheckpoint {
            last_offset,
            file_seq,
            file_path,
        };
        let bytes = serde_json::to_vec(&ckpt).map_err(|e| {
            PersistentStorageError::Io(format!("checkpoint serialize failed: {}", e))
        })?;
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&ckpt_path)
            .await
            .map_err(|e| PersistentStorageError::Io(format!("open checkpoint failed: {}", e)))?;
        f.write_all(&bytes)
            .await
            .map_err(|e| PersistentStorageError::Io(format!("write checkpoint failed: {}", e)))?;
        f.flush()
            .await
            .map_err(|e| PersistentStorageError::Io(format!("flush checkpoint failed: {}", e)))?;
        Ok(())
    }

    /// Compute the uploader checkpoint path if WAL checkpoints are enabled.
    async fn uploader_checkpoint_path(&self) -> Option<PathBuf> {
        let ckpt = self.inner.checkpoint_path.clone()?;
        let parent = ckpt.parent()?.to_path_buf();
        Some(parent.join("uploader.ckpt"))
    }

    /// Persist uploader checkpoint as JSON to `uploader.ckpt`.
    pub async fn write_uploader_checkpoint(
        &self,
        ckpt: &UploaderCheckpoint,
    ) -> Result<(), PersistentStorageError> {
        let path = match self.uploader_checkpoint_path().await {
            Some(p) => p,
            None => return Ok(()),
        };
        let bytes = serde_json::to_vec(ckpt).map_err(|e| {
            PersistentStorageError::Io(format!("uploader ckpt serialize failed: {}", e))
        })?;
        // Write atomically via temp + rename
        let tmp = path.with_extension("ckpt.tmp");
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .await
            .map_err(|e| {
                PersistentStorageError::Io(format!("open uploader ckpt tmp failed: {}", e))
            })?;
        use tokio::io::AsyncWriteExt as _;
        f.write_all(&bytes).await.map_err(|e| {
            PersistentStorageError::Io(format!("write uploader ckpt failed: {}", e))
        })?;
        f.flush().await.map_err(|e| {
            PersistentStorageError::Io(format!("flush uploader ckpt failed: {}", e))
        })?;
        tokio::fs::rename(&tmp, &path).await.map_err(|e| {
            PersistentStorageError::Io(format!("rename uploader ckpt failed: {}", e))
        })?;
        Ok(())
    }

    /// Read uploader checkpoint from `uploader.ckpt` if present.
    pub async fn read_uploader_checkpoint(
        &self,
    ) -> Result<Option<UploaderCheckpoint>, PersistentStorageError> {
        let path = match self.uploader_checkpoint_path().await {
            Some(p) => p,
            None => return Ok(None),
        };
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let ckpt: UploaderCheckpoint = serde_json::from_slice(&bytes).map_err(|e| {
                    PersistentStorageError::Io(format!("uploader ckpt parse failed: {}", e))
                })?;
                Ok(Some(ckpt))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PersistentStorageError::Io(format!(
                "read uploader ckpt failed: {}",
                e
            ))),
        }
    }
}
