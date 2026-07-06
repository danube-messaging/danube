use super::{normalize_topic_path, CommitInfo, SealInfo, StorageFactory, StorageFactoryConfig};
use crate::durable_history_reader::DurableHistoryReader;
use crate::tiered_storage::TieredStorage;
use crate::valkey::stream_reader::ValkeyStreamReader;
use crate::durable_store::DurableStore;
use crate::metadata::{StorageMetadata, StorageStateSealed};
use crate::object_store_backend::ObjStoreDurable;
use crate::valkey::ValkeyClient;
use crate::wal::deleter::{Deleter, DeleterConfig};
use crate::wal_storage::WalStorage;
use danube_core::metadata::MetadataStore;
use danube_core::storage::{PersistentStorage, PersistentStorageError};
use std::sync::Arc;
use tracing::{info, warn};

impl StorageFactory {
    pub fn new(config: StorageFactoryConfig, metadata_store: Arc<dyn MetadataStore>) -> Self {
        let segment_export_interval_seconds = Self::segment_export_interval_seconds(&config);
        let durable_store = Self::build_durable_store(&config);
        let deleter_cfg = Self::build_deleter_config(&config);
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
            deleter_cfg,
            deleters: Arc::new(dashmap::DashMap::new()),
            deleter_tokens: Arc::new(dashmap::DashMap::new()),
            metadata_root: config.metadata_root,
            write_buffer: config.write_buffer,
            valkey_client: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    fn segment_export_interval_seconds(config: &StorageFactoryConfig) -> u64 {
        config.segment_export_interval_seconds.unwrap_or(300)
    }

    fn build_durable_store(config: &StorageFactoryConfig) -> Option<Arc<dyn DurableStore>> {
        config
            .durable_backend()
            .map(ObjStoreDurable::from_backend)
            .transpose()
            .expect("init durable store")
            .map(|store| Arc::new(store) as Arc<dyn DurableStore>)
    }

    fn build_deleter_config(config: &StorageFactoryConfig) -> Option<DeleterConfig> {
        config.retention.as_ref().map(|retention| DeleterConfig {
            check_interval_minutes: retention.check_interval_minutes,
            retention_time_minutes: retention.time_minutes,
            retention_size_mb: retention.size_mb,
        })
    }

    pub(crate) fn should_start_background_export(&self) -> bool {
        self.mode.uses_background_export()
    }

    fn should_run_retention_deleter(&self) -> bool {
        self.deleter_cfg.is_some()
    }

    /// Build or retrieve per-topic storage and start any required background services.
    ///
    /// Functional flow
    /// - Normalize the logical topic name into the metadata/storage path shape used internally.
    /// - Recover or create the topic WAL, including recovery start-offset resolution.
    /// - Clear any stale sealed mobility marker once the topic is actively owned again.
    /// - Start background segment export and retention tasks when the selected storage mode uses
    ///   them.
    /// - Wrap the WAL in `WalStorage`, optionally wiring durable-history reads and hot-cutover
    ///   behavior for sealed recovery.
    ///
    /// Important behavior
    /// - `resumed_from_sealed` enables `with_hot_cutover()`, which tells readers to prefer durable
    ///   history for the already-sealed prefix and use the hot WAL only for new local progress.
    /// - Export/deleter tasks are started once per topic and reused across repeated `for_topic()`
    ///   calls in the same process.
    pub async fn for_topic(&self, topic_name: &str) -> Result<Arc<dyn PersistentStorage>, PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        let (topic_wal, resolved_dir, ckpt_store, resumed_from_sealed, rotation_rx) =
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

        let should_spawn_segment_exporter = durable_store.is_some()
            && self.should_start_background_export()
            && !self.segment_exporters.contains_key(&topic_path);
        if should_spawn_segment_exporter {
            let factory = self.clone();
            let wal = topic_wal.clone();
            let topic_path_for_task = topic_path.clone();
            let interval_seconds = self.segment_export_interval_seconds;
            let cancel = tokio_util::sync::CancellationToken::new();
            let cancel_for_task = cancel.clone();
            let handle = tokio::spawn(async move {
                info!(
                    target = "storage_factory",
                    topic = %topic_path_for_task,
                    interval = interval_seconds,
                    "durable segment exporter started"
                );
                if let Err(e) = factory
                    .cut_and_export_topic_segments(&topic_path_for_task, &wal)
                    .await
                {
                    warn!(
                        target = "storage_factory",
                        topic = %topic_path_for_task,
                        error = %e,
                        "durable segment exporter cycle failed on startup"
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
                                "durable segment exporter stopped"
                            );
                            break;
                        }
                        _ = ticker.tick() => {
                            if let Err(e) = factory
                                .cut_and_export_topic_segments(&topic_path_for_task, &wal)
                                .await
                            {
                                warn!(
                                    target = "storage_factory",
                                    topic = %topic_path_for_task,
                                    error = %e,
                                    "durable segment exporter cycle failed"
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

        if self.should_run_retention_deleter() && !self.deleters.contains_key(&topic_path) {
            if let (Some(_dir), Some(cfg)) = (resolved_dir.clone(), self.deleter_cfg.clone()) {
                let store = ckpt_store.clone().ok_or_else(|| {
                    PersistentStorageError::Other(
                        "retention deleter requires wal.dir for checkpoint state".to_string(),
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

        // --- Construct the 3 independent tiers ---

        // Hot Tier: local WAL (always present)
        let hot = WalStorage::from_wal(topic_wal);

        // Cold Tier: S3/Cloud durable history (optional)
        let cold = durable_store.map(|store| {
            DurableHistoryReader::new(
                store,
                self.segment_catalog.metadata().clone(),
                topic_path.clone(),
            )
        });

        // Warm Tier: Valkey stream reader + client (optional)
        let warm = if let Some(ref wb_config) = self.write_buffer {
            let valkey_client = self.get_or_connect_valkey(wb_config).await?;

            // Spawn the rotation listener: WAL rotation → XTRIM old segments
            if let Some(rx) = rotation_rx {
                crate::valkey::rotation_listener::spawn_rotation_listener(
                    valkey_client.clone(),
                    topic_path.clone(),
                    wb_config.clone(),
                    rx,
                    self.segment_catalog.metadata().clone(),
                );
            }

            info!(
                topic = %topic_path,
                endpoints = ?wb_config.endpoints,
                "Valkey write buffer enabled for topic"
            );

            let reader = ValkeyStreamReader::new(valkey_client.clone(), &topic_path);
            Some((reader, valkey_client, wb_config.clone()))
        } else {
            None
        };

        // --- Construct the single orchestrator ---
        let mut tiered = TieredStorage::new(hot, warm, cold, topic_path.clone());
        if resumed_from_sealed {
            tiered = tiered.with_hot_cutover();
        }

        Ok(Arc::new(tiered))
    }

    /// Get or lazily connect the shared Valkey client.
    pub(super) async fn get_or_connect_valkey(
        &self,
        config: &crate::valkey::config::WriteBufferConfig,
    ) -> Result<Arc<ValkeyClient>, PersistentStorageError> {
        self.valkey_client
            .get_or_try_init(|| async {
                ValkeyClient::connect(config)
                    .await
                    .map(Arc::new)
                    .map_err(|e| {
                        PersistentStorageError::Other(format!(
                            "failed to connect to Valkey write buffer: {}",
                            e
                        ))
                    })
            })
            .await
            .cloned()
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
        let wal = match self.topics.get(&topic_path) {
            Some(w) => w.clone(),
            None => {
                return Err(PersistentStorageError::Other(
                    "no storage for topic".to_string(),
                ))
            }
        };

        Ok(CommitInfo {
            last_committed_offset: wal.last_committed_offset(),
        })
    }

    /// Seal a topic for ownership transfer or shutdown.
    ///
    /// Functional flow
    /// - Remove the WAL from the factory's active-topic map so no new callers reuse it.
    /// - Capture the last locally committed offset before shutdown.
    /// - Flush and stop the WAL plus any background exporter/deleter tasks.
    /// - Export any remaining local WAL data into durable segments when the mode supports sealed
    ///   segment export.
    /// - Clear local WAL state and persist a sealed mobility marker so the next owner can resume at
    ///   `last_committed_offset + 1`.
    ///
    /// The returned `SealInfo` reports the final local WAL boundary captured into sealed state.
    pub async fn seal(
        &self,
        topic_name: &str,
        broker_id: u64,
    ) -> Result<SealInfo, PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        let wal = match self.topics.remove(&topic_path) {
            Some((_, w)) => w,
            None => {
                return Err(PersistentStorageError::Other(
                    "no storage for topic".to_string(),
                ))
            }
        };

        let last_local_wal_offset = wal.last_committed_offset();
        wal.shutdown().await?;
        self.stop_topic_background_tasks(&topic_path).await;
        if self.mode.requires_separate_durable_backend() {
            self.export_topic_segments(&topic_path, &wal, true).await?;
            self.clear_topic_wal_state(&topic_path).await?;
        }

        let state = StorageStateSealed {
            sealed: true,
            last_committed_offset: last_local_wal_offset,
            broker_id,
            timestamp: chrono::Utc::now().timestamp() as u64,
        };
        self.mobility_state.store(&topic_path, &state).await?;

        Ok(SealInfo {
            last_committed_offset: last_local_wal_offset,
        })
    }

    /// Remove all persistent state associated with a topic.
    ///
    /// Cleanup order
    /// - Stop and remove any live WAL instance for the topic.
    /// - Stop background export/deletion tasks so they do not race the cleanup.
    /// - Delete durable segment objects and their catalog metadata.
    /// - Delete any remaining local WAL directory state.
    ///
    /// This is the destructive full-cleanup path used when a topic's storage footprint should be
    /// removed completely, not just sealed for later recovery.
    pub async fn delete_storage_metadata(
        &self,
        topic_name: &str,
    ) -> Result<(), PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        if let Some((_, wal)) = self.topics.remove(&topic_path) {
            wal.shutdown().await?;
        }
        self.stop_topic_background_tasks(&topic_path).await;
        self.delete_topic_durable_segments(&topic_path).await?;
        self.segment_catalog.delete_topic(&topic_path).await?;
        self.clear_topic_wal_state(&topic_path).await?;
        Ok(())
    }
}
