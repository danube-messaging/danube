use super::StorageFactory;
use crate::checkpoint::{CheckpointStore, WalCheckpoint};
use crate::durable_store::DurableStore;
use crate::metadata::SegmentDescriptor;
use crate::wal::Wal;
use danube_core::storage::PersistentStorageError;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Debug)]
enum RecoveryStartSource {
    LocalWalContinuity,
    SealedMobilityState,
    DurableSegmentCatalog,
    EmptyTopic,
    MobilityStateReadFailed,
}

#[derive(Debug)]
struct RecoveryStartDecision {
    initial_offset: Option<u64>,
    resumed_from_sealed: bool,
    source: RecoveryStartSource,
}

impl StorageFactory {
    /// Get the existing topic WAL or create a new one with the correct recovery starting point.
    ///
    /// Functional flow
    /// - Reuse an already-open WAL if the topic was previously initialized in this process.
    /// - Derive a per-topic local WAL directory and preload checkpoint state when local staging is
    ///   enabled.
    /// - Inspect both local WAL continuity and durable segment metadata to decide where the next
    ///   append offset should start.
    /// - Return the WAL together with local-path/checkpoint details needed by lifecycle code.
    ///
    /// Recovery inputs
    /// - Local WAL continuity is only considered valid if the checkpoint references files that
    ///   still exist on disk.
    /// - Durable segment catalog state is used as a fallback when the mode requires durable
    ///   history and local staged WAL state is unavailable.
    /// - A sealed mobility marker takes precedence over both and resumes from the stored committed
    ///   offset + 1.
    pub(super) async fn get_or_create_wal(
        &self,
        topic_path: &str,
    ) -> Result<
        (
            Wal,
            Option<PathBuf>,
            Option<Arc<CheckpointStore>>,
            bool,
        ),
        PersistentStorageError,
    > {
        if let Some(existing) = self.topics.get(topic_path) {
            return Ok((existing.clone(), None, None, false));
        }

        let mut cfg = self.base_cfg.clone();
        let mut root_path: Option<PathBuf> = None;
        let mut ckpt_store: Option<Arc<CheckpointStore>> = None;
        if let Some(mut root) = cfg.dir.clone() {
            let parts: Vec<&str> = topic_path.split('/').collect();
            if parts.len() == 2 {
                root.push(parts[0]);
                root.push(parts[1]);
            }
            root_path = Some(root.clone());
            cfg.dir = Some(root);
        }
        if let Some(dir) = cfg.dir.as_ref() {
            let wal_ckpt = dir.join("wal.ckpt");
            let store = Arc::new(CheckpointStore::new(wal_ckpt));
            if let Err(e) = store.load_from_disk().await {
                warn!(target = "storage_factory", topic = %topic_path, error = %e, "failed to preload checkpoints from disk");
            }
            ckpt_store = Some(store);
        } else if self.mode.requires_local_wal_staging() {
            return Err(PersistentStorageError::Other(
                "export-later durable mode requires wal.dir for durable WAL state".to_string(),
            ));
        }

        let wal_checkpoint = match ckpt_store.as_ref() {
            Some(store) => store.get_wal().await,
            None => None,
        };
        let local_wal_state_available = wal_checkpoint
            .as_ref()
            .map(Self::wal_checkpoint_has_local_data)
            .unwrap_or(false);
        if wal_checkpoint.is_some() && !local_wal_state_available {
            warn!(
                target = "storage_factory",
                topic = %topic_path,
                "startup recovery found wal checkpoint but no referenced local wal files; falling back to durable segment continuity"
            );
        }
        let catalog_current_segment = match self
            .segment_catalog
            .metadata()
            .get_current_segment_descriptor(topic_path)
            .await
        {
            Ok(segment) => segment,
            Err(e) => {
                warn!(
                    target = "storage_factory",
                    topic = %topic_path,
                    error = %e,
                    "failed to read current segment descriptor during startup recovery"
                );
                None
            }
        };
        let recovery = self
            .resolve_recovery_start(topic_path, local_wal_state_available, catalog_current_segment)
            .await;
        info!(
            target = "storage_factory",
            topic = %topic_path,
            source = ?recovery.source,
            initial_offset = ?recovery.initial_offset,
            resumed_from_sealed = recovery.resumed_from_sealed,
            "startup recovery resolved initial WAL offset"
        );

        let wal = Wal::with_config_with_store(cfg, ckpt_store.clone(), recovery.initial_offset).await?;
        self.topics.insert(topic_path.to_string(), wal.clone());
        Ok((wal, root_path, ckpt_store, recovery.resumed_from_sealed))
    }

    pub(super) fn topic_wal_dir(&self, topic_path: &str) -> Option<PathBuf> {
        let mut root = self.base_cfg.dir.clone()?;
        let parts: Vec<&str> = topic_path.split('/').collect();
        if parts.len() == 2 {
            root.push(parts[0]);
            root.push(parts[1]);
        }
        Some(root)
    }

    /// Return whether a checkpoint still points at usable local WAL files.
    ///
    /// A checkpoint alone is not enough to claim local continuity: the active file or at least one
    /// rotated file it references must still exist on disk.
    fn wal_checkpoint_has_local_data(wal_checkpoint: &WalCheckpoint) -> bool {
        wal_checkpoint
            .rotated_files
            .iter()
            .any(|(_, path, _)| path.exists())
            || (!wal_checkpoint.file_path.is_empty()
                && PathBuf::from(&wal_checkpoint.file_path).exists())
    }

    pub(super) fn durable_store_for_topic(&self) -> Result<Option<Arc<dyn DurableStore>>, PersistentStorageError> {
        match self.durable_store.clone() {
            Some(store) => Ok(Some(store)),
            None if self.mode.requires_separate_durable_backend() => Err(
                PersistentStorageError::Other(
                    "storage mode requires separate durable backend".to_string(),
                ),
            ),
            None => Ok(None),
        }
    }

    /// Resolve the initial WAL offset for topic startup or ownership handoff.
    ///
    /// Decision order
    /// - **Sealed mobility state**: resume from the last sealed committed offset + 1.
    /// - **Durable segment catalog**: in modes that require a separate durable backend and lack
    ///   local WAL continuity, resume from the durable segment end offset + 1.
    /// - **Local WAL continuity**: let the WAL recover from its existing local files/checkpoint.
    /// - **Empty topic**: start from an empty WAL when no durable or local continuity exists.
    ///
    /// The returned `resumed_from_sealed` flag tells higher layers whether readers should treat
    /// durable history as authoritative for the already-committed prefix.
    async fn resolve_recovery_start(
        &self,
        topic_path: &str,
        local_wal_state_available: bool,
        catalog_current_segment: Option<SegmentDescriptor>,
    ) -> RecoveryStartDecision {
        match self.mobility_state.load(topic_path).await {
            Ok(Some(sealed_state)) if sealed_state.sealed => RecoveryStartDecision {
                initial_offset: Some(sealed_state.last_committed_offset.saturating_add(1)),
                resumed_from_sealed: true,
                source: RecoveryStartSource::SealedMobilityState,
            },
            Ok(_) => {
                let can_recover_from_durable_catalog = self.mode.requires_separate_durable_backend();
                if can_recover_from_durable_catalog && !local_wal_state_available {
                    match catalog_current_segment {
                        Some(segment) => RecoveryStartDecision {
                            initial_offset: Some(segment.end_offset.saturating_add(1)),
                            resumed_from_sealed: true,
                            source: RecoveryStartSource::DurableSegmentCatalog,
                        },
                        None => RecoveryStartDecision {
                            initial_offset: None,
                            resumed_from_sealed: false,
                            source: RecoveryStartSource::EmptyTopic,
                        },
                    }
                } else {
                    RecoveryStartDecision {
                        initial_offset: None,
                        resumed_from_sealed: false,
                        source: RecoveryStartSource::LocalWalContinuity,
                    }
                }
            }
            Err(e) => {
                warn!(
                    target = "storage_factory",
                    topic = %topic_path,
                    error = %e,
                    "failed to read mobility state"
                );
                RecoveryStartDecision {
                    initial_offset: None,
                    resumed_from_sealed: false,
                    source: RecoveryStartSource::MobilityStateReadFailed,
                }
            }
        }
    }
}
