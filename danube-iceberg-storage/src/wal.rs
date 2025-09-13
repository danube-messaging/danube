use crate::config::{SyncMode, WalConfig};
use crate::errors::{IcebergStorageError, Result};
use danube_core::message::StreamMessage;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tracing::info;

/// WAL entry types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    Message(StreamMessage),
    Checkpoint(u64),
}

/// Write-Ahead Log entry header
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalEntryHeader {
    /// Length of the payload in bytes
    payload_len: u32,
    /// CRC32 checksum of the payload
    checksum: u32,
    /// Timestamp when the entry was written
    timestamp: u64,
}

impl WalEntryHeader {
    const SIZE: usize = std::mem::size_of::<Self>();

    fn new(payload: &[u8]) -> Self {
        Self {
            payload_len: payload.len() as u32,
            checksum: crc32fast::hash(payload),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize WAL header")
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| IcebergStorageError::Serialization(e.to_string()))
    }
}

/// Write-Ahead Log for fast message writes
#[derive(Debug)]
pub struct WriteAheadLog {
    /// Configuration
    config: WalConfig,
    /// Current WAL file
    current_file: Arc<RwLock<Option<File>>>,
    /// Current file path
    current_file_path: Arc<RwLock<PathBuf>>,
    /// Current file size
    current_file_size: AtomicU64,
    /// Current offset (sequence number)
    current_offset: AtomicU64,
    /// Base directory for WAL files
    base_path: PathBuf,
    /// Last sync time in milliseconds
    last_sync_ms: AtomicU64,
}

impl WriteAheadLog {
    /// Create a new WriteAheadLog
    pub async fn new(config: &WalConfig) -> Result<Self> {
        let base_path = PathBuf::from(&config.base_path);

        // Create base directory if it doesn't exist
        if !base_path.exists() {
            tokio::fs::create_dir_all(&base_path).await?;
        }

        let wal = Self {
            config: config.clone(),
            current_file: Arc::new(RwLock::new(None)),
            current_file_path: Arc::new(RwLock::new(PathBuf::new())),
            current_file_size: AtomicU64::new(0),
            current_offset: AtomicU64::new(0),
            base_path,
            last_sync_ms: AtomicU64::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            ),
        };

        // Initialize first WAL file
        wal.rotate_file().await?;

        Ok(wal)
    }

    /// Write a message to the WAL
    pub async fn write_message(&self, message: StreamMessage) -> Result<u64> {
        let entry = WalEntry::Message(message);
        self.write_entry(entry).await
    }

    /// Write a checkpoint to the WAL
    pub async fn write_checkpoint(&self, offset: u64) -> Result<u64> {
        let entry = WalEntry::Checkpoint(offset);
        self.write_entry(entry).await
    }

    /// Write an entry to the WAL
    async fn write_entry(&self, entry: WalEntry) -> Result<u64> {
        // Serialize the entry
        let payload = bincode::serialize(&entry)
            .map_err(|e| IcebergStorageError::Serialization(e.to_string()))?;

        // Create header
        let header = WalEntryHeader::new(&payload);
        let header_bytes = header.to_bytes();

        // Check if we need to rotate the file
        let total_size = header_bytes.len() + payload.len();
        if self.current_file_size.load(Ordering::Relaxed) + total_size as u64
            > self.config.max_file_size
        {
            self.rotate_file().await?;
        }

        // Write to current file
        let mut file_guard = self.current_file.write().await;
        if let Some(ref mut file) = *file_guard {
            // Write header
            file.write_all(&header_bytes).await?;
            // Write payload
            file.write_all(&payload).await?;

            // Sync based on configuration
            match self.config.sync_mode {
                SyncMode::Always => {
                    file.sync_all().await?;
                }
                SyncMode::Periodic => {
                    // Simple time-based periodic sync (every ~1s)
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    let last = self.last_sync_ms.load(Ordering::Relaxed);
                    if now_ms.saturating_sub(last) > 1000 {
                        file.sync_data().await?;
                        self.last_sync_ms.store(now_ms, Ordering::Relaxed);
                    }
                }
                SyncMode::None => {
                    // No sync
                }
            }

            // Update size and offset
            self.current_file_size
                .fetch_add(total_size as u64, Ordering::Relaxed);
            let offset = self.current_offset.fetch_add(1, Ordering::Relaxed);

            Ok(offset)
        } else {
            Err(IcebergStorageError::Wal("No current WAL file".to_string()))
        }
    }

    /// Rotate to a new WAL file
    async fn rotate_file(&self) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let filename = format!("wal-{}.log", timestamp);
        let file_path = self.base_path.join(&filename);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&file_path)
            .await?;

        let mut current_file = self.current_file.write().await;
        let mut current_path = self.current_file_path.write().await;

        *current_file = Some(file);
        *current_path = file_path;
        self.current_file_size.store(0, Ordering::Relaxed);

        info!("Rotated to new WAL file: {}", filename);
        Ok(())
    }

    /// Create a WAL reader
    pub fn create_reader(&self) -> WalReader {
        WalReader::new(self.base_path.clone())
    }

    /// Get current offset
    pub fn current_offset(&self) -> u64 {
        self.current_offset.load(Ordering::Relaxed)
    }

    /// Shutdown the WAL
    pub async fn shutdown(&self) -> Result<()> {
        let mut file_guard = self.current_file.write().await;
        if let Some(ref mut file) = *file_guard {
            file.sync_all().await?;
        }
        *file_guard = None;
        info!("WAL shutdown complete");
        Ok(())
    }
}

/// WAL reader for reading entries from WAL files
#[derive(Debug)]
pub struct WalReader {
    base_path: PathBuf,
    current_file: Option<File>,
    current_offset: u64,
    files: Vec<PathBuf>,
    file_index: usize,
}

impl WalReader {
    /// Create a new WAL reader
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            current_file: None,
            current_offset: 0,
            files: Vec::new(),
            file_index: 0,
        }
    }

    /// Read the next entry from the WAL
    pub async fn read_next(&mut self) -> Result<Option<WalEntry>> {
        use tokio::io::AsyncReadExt;

        // Ensure we have a file open
        loop {
            if self.current_file.is_none() {
                // Initialize file list if needed
                if self.files.is_empty() {
                    let mut dir = tokio::fs::read_dir(&self.base_path).await?;
                    let mut files: Vec<PathBuf> = Vec::new();
                    while let Some(entry) = dir.next_entry().await? {
                        let p = entry.path();
                        if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                            if name.starts_with("wal-") && name.ends_with(".log") {
                                files.push(p);
                            }
                        }
                    }
                    // Sort by filename (timestamp-based naming ensures order)
                    files.sort();
                    self.files = files;
                    self.file_index = 0;
                }

                if self.file_index >= self.files.len() {
                    // No files to read
                    return Ok(None);
                }

                let path = self.files[self.file_index].clone();
                let file = OpenOptions::new().read(true).open(&path).await?;
                self.current_file = Some(file);
            }

            if let Some(file) = &mut self.current_file {
                // Read fixed-size header (bincode of fixed primitives yields 16 bytes)
                let mut header_buf = vec![0u8; WalEntryHeader::SIZE];
                match file.read_exact(&mut header_buf).await {
                    Ok(_) => {
                        let header = WalEntryHeader::from_bytes(&header_buf)?;

                        // Read payload
                        let mut payload = vec![0u8; header.payload_len as usize];
                        file.read_exact(&mut payload).await?;

                        // Verify checksum
                        let crc = crc32fast::hash(&payload);
                        if crc != header.checksum {
                            return Err(IcebergStorageError::Wal(
                                "WAL checksum mismatch".to_string(),
                            ));
                        }

                        // Deserialize entry
                        let entry: WalEntry = bincode::deserialize(&payload)
                            .map_err(|e| IcebergStorageError::Serialization(e.to_string()))?;

                        // Advance offset
                        self.current_offset = self.current_offset.saturating_add(1);
                        return Ok(Some(entry));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        // End of current file, move to next
                        self.current_file = None;
                        self.file_index += 1;
                        continue;
                    }
                    Err(e) => return Err(IcebergStorageError::Io(e)),
                }
            }
        }
    }

    /// Seek to a specific offset
    pub async fn seek(&mut self, offset: u64) -> Result<()> {
        // Reset state and scan forward to desired logical offset
        self.current_file = None;
        self.files.clear();
        self.file_index = 0;
        self.current_offset = 0;

        while self.current_offset < offset {
            if let Some(_entry) = self.read_next().await? {
                // continue scanning
            } else {
                break;
            }
        }

        Ok(())
    }
}

/// Sanitize topic name for use as directory name
fn sanitize_topic_name(topic_name: &str) -> String {
    topic_name
        .replace('/', "_")
        .replace('\\', "_")
        .replace(':', "_")
        .replace('*', "_")
        .replace('?', "_")
        .replace('"', "_")
        .replace('<', "_")
        .replace('>', "_")
        .replace('|', "_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_basic_operations() {
        let temp_dir = tempdir().unwrap();
        let config = WalConfig {
            base_path: temp_dir.path().to_string_lossy().to_string(),
            max_file_size: 1024 * 1024, // 1MB
            sync_mode: SyncMode::Always,
        };

        let wal = WriteAheadLog::new(&config).await.unwrap();

        // Test append
        let msg = StreamMessage {
            request_id: 0,
            msg_id: danube_core::message::MessageID {
                producer_id: 0,
                topic_name: "test".to_string(),
                broker_addr: "local".to_string(),
                segment_id: 0,
                segment_offset: 0,
            },
            payload: b"test message".to_vec(),
            publish_time: 0,
            producer_name: "test".to_string(),
            subscription_name: None,
            attributes: std::collections::HashMap::new(),
        };
        let offset = wal.write_message(msg).await.unwrap();
        assert_eq!(offset, 0);
    }

    #[tokio::test]
    async fn test_wal_reader() {
        let temp_dir = tempdir().unwrap();
        let config = WalConfig {
            base_path: temp_dir.path().to_string_lossy().to_string(),
            max_file_size: 1024 * 1024,
            sync_mode: SyncMode::Always,
        };

        let wal = WriteAheadLog::new(&config).await.unwrap();

        // Write some data
        for i in 0..3u64 {
            let msg = StreamMessage {
                request_id: i,
                msg_id: danube_core::message::MessageID {
                    producer_id: 0,
                    topic_name: "test".to_string(),
                    broker_addr: "local".to_string(),
                    segment_id: 0,
                    segment_offset: i,
                },
                payload: format!("msg{}", i).into_bytes(),
                publish_time: 0,
                producer_name: "test".to_string(),
                subscription_name: None,
                attributes: std::collections::HashMap::new(),
            };
            wal.write_message(msg).await.unwrap();
        }

        // Read with reader
        let mut reader = wal.create_reader();
        let mut read_messages = Vec::new();

        while let Some(data) = reader.read_next().await.unwrap() {
            read_messages.push(data);
        }
    }
}
