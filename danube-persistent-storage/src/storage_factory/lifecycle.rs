use super::{normalize_topic_path, CommitInfo, SealInfo, StorageFactory, StorageFactoryConfig, StorageMode};
use crate::durable_store::{DurableStore, OpendalDurableStore};
use crate::metadata::{StorageMetadata, StorageStateSealed};
use crate::opendal::{BackendConfig, LocalBackend};
use crate::wal::deleter::{Deleter, DeleterConfig};
use crate::wal_storage::WalStorage;
use danube_core::metadata::MetadataStore;
use danube_core::storage::PersistentStorageError;
use std::sync::Arc;
use tracing::{info, warn};

impl StorageFactory {
    pub fn new(config: StorageFactoryConfig, metadata_store: Arc<dyn MetadataStore>) -> Self {
        let segment_export_interval_seconds =
            config.segment_export_interval_seconds.unwrap_or(300);
        let durable_backend = config.durable_backend.clone().or_else(|| {
            if config.mode == StorageMode::Local {
                config.wal.dir.as_ref().map(|dir| BackendConfig::Local {
                    backend: LocalBackend::Fs,
                    root: dir.to_string_lossy().to_string(),
                })
            } else {
                None
            }
        });
        let durable_store = durable_backend
            .map(OpendalDurableStore::from_backend)
            .transpose()
            .expect("init durable store")
            .map(|store| Arc::new(store) as Arc<dyn DurableStore>);
        let metadata = StorageMetadata::new(metadata_store, config.metadata_root.clone());
        let segment_catalog = crate::metadata::SegmentCatalog::new(metadata.clone());
        let mobility_state = crate::metadata::MobilityState::new(metadata.clone());
        Self {
            mode: config.mode,
            base_cfg: config.wal,
            segment_catalog,
            mobility_state,
            durable_store,
            topics: Arc::new(dashmap::DashMap::new()),
            segment_exporters: Arc::new(dashmap::DashMap::new()),
            segment_exporter_tokens: Arc::new(dashmap::DashMap::new()),
            segment_export_interval_seconds,
            deleter_cfg: config.retention.map(|retention| DeleterConfig {
                check_interval_minutes: retention.check_interval_minutes,
                retention_time_minutes: retention.time_minutes,
                retention_size_mb: retention.size_mb,
            }),
            deleters: Arc::new(dashmap::DashMap::new()),
            deleter_tokens: Arc::new(dashmap::DashMap::new()),
            metadata_root: config.metadata_root,
        }
    }

    pub async fn for_topic(&self, topic_name: &str) -> Result<WalStorage, PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        let (topic_wal, resolved_dir, ckpt_store, resumed_from_sealed) =
            self.get_or_create_wal(&topic_path).await?;
        let durable_store = self.durable_store_for_topic()?;

        if let Some(ref dir) = resolved_dir {
            info!(
                target = "storage_factory",
                topic = %topic_path,
                wal_dir = %dir.display(),
                mode = ?self.mode,
                metadata_root = %self.metadata_root,
                "created per-topic storage"
            );
            if let Err(e) = self.mobility_state.clear(&topic_path).await {
                warn!(
                    target = "storage_factory",
                    topic = %topic_path,
                    error = %e,
                    "failed to clear mobility state marker"
                );
            }
        }

        if durable_store.is_some()
            && self.mode == StorageMode::CloudNative
            && !self.segment_exporters.contains_key(&topic_path)
        {
            let factory = self.clone();
            let hot_log = topic_wal.clone();
            let topic_path_for_task = topic_path.clone();
            let interval_seconds = self.segment_export_interval_seconds;
            let cancel = tokio_util::sync::CancellationToken::new();
            let cancel_for_task = cancel.clone();
            let handle = tokio::spawn(async move {
                info!(
                    target = "storage_factory",
                    topic = %topic_path_for_task,
                    interval = interval_seconds,
                    "cloud_native segment exporter started"
                );
                if let Err(e) = factory
                    .cut_and_export_topic_segments(&topic_path_for_task, &hot_log)
                    .await
                {
                    warn!(
                        target = "storage_factory",
                        topic = %topic_path_for_task,
                        error = %e,
                        "cloud_native segment exporter cycle failed on startup"
                    );
                }
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_secs(interval_seconds));
                loop {
                    tokio::select! {
                        _ = cancel_for_task.cancelled() => {
                            info!(
                                target = "storage_factory",
                                topic = %topic_path_for_task,
                                "cloud_native segment exporter stopped"
                            );
                            break;
                        }
                        _ = ticker.tick() => {
                            if let Err(e) = factory
                                .cut_and_export_topic_segments(&topic_path_for_task, &hot_log)
                                .await
                            {
                                warn!(
                                    target = "storage_factory",
                                    topic = %topic_path_for_task,
                                    error = %e,
                                    "cloud_native segment exporter cycle failed"
                                );
                            }
                        }
                    }
                }
                Ok(())
            });
            self.segment_exporters.insert(topic_path.clone(), handle);
            self.segment_exporter_tokens.insert(topic_path.clone(), cancel);
        }

        if self.mode == StorageMode::CloudNative && !self.deleters.contains_key(&topic_path) {
            if let (Some(_dir), Some(cfg)) = (resolved_dir.clone(), self.deleter_cfg.clone()) {
                let store = ckpt_store.clone().ok_or_else(|| {
                    PersistentStorageError::Other(
                        "cloud_native requires wal.dir for deleter state".to_string(),
                    )
                })?;
                let deleter = Deleter::new(
                    topic_path.clone(),
                    store,
                    self.segment_catalog.metadata().clone(),
                    cfg,
                );
                let arc_del = Arc::new(deleter);
                let cancel = tokio_util::sync::CancellationToken::new();
                let handle = arc_del.clone().start_with_cancel(cancel.clone());
                self.deleters.insert(topic_path.clone(), handle);
                self.deleter_tokens.insert(topic_path.clone(), cancel);
            }
        }

        let mut storage = WalStorage::from_hot_log(topic_wal);
        if resumed_from_sealed {
            storage = storage.with_hot_cutover();
        }
        match (self.mode, durable_store) {
            (StorageMode::Local, Some(store)) | (StorageMode::SharedFs, Some(store)) => Ok(storage
                .with_durable_history(store, self.segment_catalog.metadata().clone(), topic_path)),
            (_, Some(store)) if self.mode != StorageMode::Local => Ok(storage.with_durable_history(
                store,
                self.segment_catalog.metadata().clone(),
                topic_path,
            )),
            _ => Ok(storage),
        }
    }

    pub async fn shutdown(&self) {
        for item in self.segment_exporter_tokens.iter() {
            item.value().cancel();
        }
        for item in self.deleter_tokens.iter() {
            item.value().cancel();
        }
        self.segment_exporters.clear();
        self.deleters.clear();
        self.segment_exporter_tokens.clear();
        self.deleter_tokens.clear();
    }

    pub async fn commit_info(
        &self,
        topic_name: &str,
    ) -> Result<CommitInfo, PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        let hot_log = match self.topics.get(&topic_path) {
            Some(w) => w.clone(),
            None => {
                return Err(PersistentStorageError::Other(
                    "no storage for topic".to_string(),
                ))
            }
        };

        Ok(CommitInfo {
            last_committed_offset: hot_log.last_committed_offset(),
        })
    }

    pub async fn seal(
        &self,
        topic_name: &str,
        broker_id: u64,
    ) -> Result<SealInfo, PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        let hot_log = match self.topics.remove(&topic_path) {
            Some((_, w)) => w,
            None => {
                return Err(PersistentStorageError::Other(
                    "no storage for topic".to_string(),
                ))
            }
        };

        let last_committed_offset = hot_log.last_committed_offset();
        hot_log.shutdown().await;
        self.stop_topic_background_tasks(&topic_path).await;
        if self.uses_sealed_segment_export() {
            self.export_topic_segments(&topic_path, &hot_log, true).await?;
        }
        self.clear_topic_wal_state(&topic_path).await?;

        let state = StorageStateSealed {
            sealed: true,
            last_committed_offset,
            broker_id,
            timestamp: chrono::Utc::now().timestamp() as u64,
        };
        self.mobility_state.store(&topic_path, &state).await?;

        Ok(SealInfo {
            last_committed_offset,
        })
    }

    pub async fn delete_storage_metadata(
        &self,
        topic_name: &str,
    ) -> Result<(), PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        if let Some((_, hot_log)) = self.topics.remove(&topic_path) {
            hot_log.shutdown().await;
        }
        self.stop_topic_background_tasks(&topic_path).await;
        self.delete_topic_durable_segments(&topic_path).await?;
        self.segment_catalog.delete_topic(&topic_path).await?;
        self.clear_topic_wal_state(&topic_path).await?;
        Ok(())
    }
}
