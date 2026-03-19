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
                if self.mode.requires_separate_durable_backend() && !local_wal_state_available {
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
