//! Local offset checkpoint persistence for edge replication.
//!
//! Tracks the last successfully replicated WAL offset per topic so the edge
//! can resume from the correct position after restart.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Checkpoint record for a single topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TopicCheckpoint {
    pub last_replicated_offset: u64,
}

/// Persistent checkpoint store for edge replication offsets.
///
/// Stores per-topic checkpoints as JSON files under a configured directory.
/// Thread-safe via internal mutex.
pub struct CheckpointStore {
    dir: PathBuf,
    cache: Mutex<HashMap<String, u64>>,
}

impl CheckpointStore {
    /// Create a new checkpoint store rooted at `dir`.
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Load all checkpoints from disk into the in-memory cache.
    pub async fn load_all(&self) -> Result<()> {
        if !self.dir.exists() {
            tokio::fs::create_dir_all(&self.dir).await?;
            return Ok(());
        }

        let mut entries = tokio::fs::read_dir(&self.dir).await?;
        let mut cache = self.cache.lock().await;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                if let Some(topic) = path.file_stem().and_then(|s| s.to_str()) {
                    match tokio::fs::read_to_string(&path).await {
                        Ok(data) => match serde_json::from_str::<TopicCheckpoint>(&data) {
                            Ok(ckpt) => {
                                let topic_name = topic.replace("__", "/");
                                debug!(
                                    topic = %topic_name,
                                    offset = ckpt.last_replicated_offset,
                                    "loaded replication checkpoint"
                                );
                                cache.insert(topic_name, ckpt.last_replicated_offset);
                            }
                            Err(e) => {
                                warn!(path = %path.display(), error = %e, "invalid checkpoint file");
                            }
                        },
                        Err(e) => {
                            warn!(path = %path.display(), error = %e, "failed to read checkpoint");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Get the last replicated offset for a topic (from cache).
    pub async fn load(&self, topic_name: &str) -> Option<u64> {
        let cache = self.cache.lock().await;
        cache.get(topic_name).copied()
    }

    /// Save a checkpoint for a topic (cache + disk).
    pub async fn save(&self, topic_name: &str, offset: u64) -> Result<()> {
        // Update cache
        {
            let mut cache = self.cache.lock().await;
            cache.insert(topic_name.to_string(), offset);
        }

        // Persist to disk
        tokio::fs::create_dir_all(&self.dir).await?;
        let file_name = topic_name.replace('/', "__");
        let path = self.dir.join(format!("{}.json", file_name));
        let ckpt = TopicCheckpoint {
            last_replicated_offset: offset,
        };
        let data = serde_json::to_string(&ckpt)?;

        // Atomic write via tmp + rename
        let tmp_path = path.with_extension("tmp");
        tokio::fs::write(&tmp_path, &data).await?;
        tokio::fs::rename(&tmp_path, &path).await?;

        debug!(topic = %topic_name, offset, "saved replication checkpoint");
        Ok(())
    }

    /// Get checkpoint directory path.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}
