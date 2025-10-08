use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::checkpoint::{CheckpointStore, UploaderCheckpoint, WalCheckpoint};
use danube_core::storage::PersistentStorageError;

#[derive(Debug, Clone)]
pub struct DeleterConfig {
    pub check_interval_minutes: u64,
    pub retention_time_minutes: Option<u64>,
    pub retention_size_mb: Option<u64>,
}

impl Default for DeleterConfig {
    fn default() -> Self {
        Self {
            check_interval_minutes: 5,
            retention_time_minutes: Some(24 * 60),
            retention_size_mb: Some(10 * 1024),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Deleter {
    topic_path: String,
    ckpt_store: Arc<CheckpointStore>,
    cfg: DeleterConfig,
}

impl Deleter {
    pub fn new(
        topic_path: String,
        ckpt_store: Arc<CheckpointStore>,
        cfg: DeleterConfig,
    ) -> Self {
        Self { topic_path, ckpt_store, cfg }
    }

    pub fn start(self: Arc<Self>) -> JoinHandle<Result<(), PersistentStorageError>> {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(self.cfg.check_interval_minutes * 60));
            // run immediately
            if let Err(e) = self.run_cycle().await {
                warn!(target = "wal_deleter", topic = %self.topic_path, error = %format!("{}", e), "initial retention cycle failed");
            }
            loop {
                ticker.tick().await;
                if let Err(e) = self.run_cycle().await {
                    warn!(target = "wal_deleter", topic = %self.topic_path, error = %format!("{}", e), "retention cycle failed");
                }
            }
        })
    }

    async fn run_cycle(&self) -> Result<(), PersistentStorageError> {
        let wal_ckpt_opt = self.ckpt_store.get_wal().await;
        let uploader_ckpt_opt = self.ckpt_store.get_uploader().await;
        let (mut wal_ckpt, uploader_ckpt) = match (wal_ckpt_opt, uploader_ckpt_opt) {
            (Some(w), Some(u)) => (w, u),
            _ => {
                // Nothing to do without checkpoints
                return Ok(());
            }
        };

        // Build candidate list from rotated_files: exclude active and files not fully processed by uploader
        let candidates = self.collect_candidates(&wal_ckpt, &uploader_ckpt).await?;
        if candidates.is_empty() {
            debug!(target = "wal_deleter", topic = %self.topic_path, "no candidates eligible for retention");
            return Ok(());
        }

        let to_delete = self.apply_retention_policy(&candidates)?;
        if to_delete.is_empty() {
            debug!(target = "wal_deleter", topic = %self.topic_path, "retention policy selected no files for deletion");
            return Ok(());
        }

        // Delete files
        let mut deleted_bytes: u64 = 0;
        let mut deleted_count: usize = 0;
        for (seq, path, size, _first_off, _mtime) in to_delete.iter() {
            match tokio::fs::remove_file(path).await {
                Ok(()) => {
                    deleted_bytes += *size as u64;
                    deleted_count += 1;
                    info!(target = "wal_deleter", topic = %self.topic_path, seq = *seq, file = %path.display(), size = *size, "deleted wal file");
                }
                Err(e) => {
                    warn!(target = "wal_deleter", topic = %self.topic_path, seq = *seq, file = %path.display(), error = %e, "failed to delete wal file");
                }
            }
        }

        // Update rotated_files by removing deleted and recompute start_offset
        wal_ckpt.rotated_files.retain(|(seq, path, _first)| {
            !to_delete.iter().any(|(dseq, dpath, _size, _first, _mtime)| dseq == seq && dpath == path)
        });

        // Compute new start_offset: min first_offset from remaining rotated files, else active file first_offset
        let new_start = wal_ckpt
            .rotated_files
            .iter()
            .map(|(_, _, first)| *first)
            .min()
            .or(wal_ckpt.active_file_first_offset)
            .unwrap_or(wal_ckpt.start_offset);
        wal_ckpt.start_offset = new_start;

        self.ckpt_store.update_wal(&wal_ckpt).await?;
        info!(target = "wal_deleter", topic = %self.topic_path, deleted_count, deleted_bytes, new_start_offset = new_start, "retention cycle completed");
        Ok(())
    }

    async fn collect_candidates(
        &self,
        wal_ckpt: &WalCheckpoint,
        uploader_ckpt: &UploaderCheckpoint,
    ) -> Result<Vec<(u64, PathBuf, u64 /*size*/, u64 /*first_offset*/, Option<SystemTime>)>, PersistentStorageError> {
        let mut out = Vec::new();
        // Only rotated files strictly older than the uploader's current file are eligible
        for (seq, path, first) in wal_ckpt.rotated_files.iter() {
            if *seq >= uploader_ckpt.last_read_file_seq {
                continue;
            }
            // metadata
            let meta = match tokio::fs::metadata(path).await {
                Ok(m) => m,
                Err(_) => continue,
            };
            let size = meta.len();
            let mtime = meta.modified().ok();
            out.push((*seq, path.clone(), size as u64, *first, mtime));
        }
        // sort by seq (oldest first)
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    fn apply_retention_policy(
        &self,
        candidates: &[(u64, PathBuf, u64, u64, Option<SystemTime>)],
    ) -> Result<Vec<(u64, PathBuf, u64, u64, Option<SystemTime>)>, PersistentStorageError> {
        let mut to_delete: Vec<(u64, PathBuf, u64, u64, Option<SystemTime>)> = Vec::new();

        // Time-based rule
        if let Some(minutes) = self.cfg.retention_time_minutes {
            let now = SystemTime::now();
            let ttl = Duration::from_secs(minutes * 60);
            for cand in candidates.iter() {
                if let Some(mtime) = cand.4 {
                    if now.duration_since(mtime).unwrap_or(Duration::ZERO) > ttl {
                        to_delete.push(cand.clone());
                    }
                }
            }
        }

        // Size-based rule (deterministic: already sorted by seq)
        if let Some(size_mb) = self.cfg.retention_size_mb {
            let limit_bytes = (size_mb as u64) * 1024 * 1024;
            let total: u64 = candidates.iter().map(|c| c.2).sum();
            if total > limit_bytes {
                let mut freed: u64 = 0;
                let excess = total - limit_bytes;
                for cand in candidates.iter() {
                    if freed >= excess { break; }
                    if !to_delete.iter().any(|d| d.0 == cand.0 && d.1 == cand.1) {
                        to_delete.push(cand.clone());
                        freed += cand.2;
                    }
                }
            }
        }

        Ok(to_delete)
    }
}
