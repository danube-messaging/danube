use crate::checkpoint::CheckpointStore;
use crate::cloud::{BackendConfig, LocalBackend};
use crate::durable_store::{DurableStore, OpendalDurableStore};
use crate::frames::{extract_offsets_in_prefix, scan_safe_frame_boundary_with_crc, FRAME_HEADER_SIZE};
use crate::hot_log::HotLog;
use crate::mobility_state::MobilityState;
use crate::segment_catalog::SegmentCatalog;
use crate::storage_metadata::{SegmentDescriptor, StorageMetadata, StorageStateSealed};
use crate::wal::deleter::{Deleter, DeleterConfig};
use crate::wal::WalConfig;
use crate::wal_storage::WalStorage;
use dashmap::DashMap;
use danube_core::metadata::MetadataStore;
use danube_core::storage::PersistentStorageError;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    Local,
    SharedFs,
    CloudNative,
}

#[derive(Debug, Clone)]
pub struct StorageFactoryConfig {
    mode: StorageMode,
    wal: WalConfig,
    metadata_root: String,
    durable_backend: Option<BackendConfig>,
    retention: Option<RetentionConfig>,
    segment_export_interval_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RetentionConfig {
    pub check_interval_minutes: u64,
    pub time_minutes: Option<u64>,
    pub size_mb: Option<u64>,
}

impl StorageFactoryConfig {
    pub fn local(wal: WalConfig, metadata_root: impl Into<String>) -> Self {
        Self {
            mode: StorageMode::Local,
            wal,
            metadata_root: metadata_root.into(),
            durable_backend: None,
            retention: None,
            segment_export_interval_seconds: None,
        }
    }

    pub fn shared_fs(
        wal: WalConfig,
        metadata_root: impl Into<String>,
        durable_backend: BackendConfig,
        retention: Option<RetentionConfig>,
    ) -> Self {
        Self {
            mode: StorageMode::SharedFs,
            wal,
            metadata_root: metadata_root.into(),
            durable_backend: Some(durable_backend),
            retention,
            segment_export_interval_seconds: None,
        }
    }

    pub fn cloud_native(
        wal: WalConfig,
        metadata_root: impl Into<String>,
        durable_backend: BackendConfig,
        retention: Option<RetentionConfig>,
    ) -> Self {
        Self {
            mode: StorageMode::CloudNative,
            wal,
            metadata_root: metadata_root.into(),
            durable_backend: Some(durable_backend),
            retention,
            segment_export_interval_seconds: None,
        }
    }

    pub fn with_segment_export_interval_seconds(mut self, interval_seconds: u64) -> Self {
        self.segment_export_interval_seconds = Some(interval_seconds);
        self
    }
}

#[derive(Debug, Clone)]
pub struct StorageFactory {
    mode: StorageMode,
    base_cfg: WalConfig,
    segment_catalog: SegmentCatalog,
    mobility_state: MobilityState,
    durable_store: Option<Arc<dyn DurableStore>>,
    topics: Arc<DashMap<String, HotLog>>,
    segment_exporters: Arc<DashMap<String, JoinHandle<Result<(), PersistentStorageError>>>>,
    segment_exporter_tokens: Arc<DashMap<String, CancellationToken>>,
    segment_export_interval_seconds: u64,
    deleter_cfg: Option<DeleterConfig>,
    deleters: Arc<DashMap<String, JoinHandle<Result<(), PersistentStorageError>>>>,
    deleter_tokens: Arc<DashMap<String, CancellationToken>>,
    metadata_root: String,
}

#[derive(Debug, Clone)]
pub struct CommitInfo {
    pub last_committed_offset: u64,
}

#[derive(Debug, Clone)]
pub struct SealInfo {
    pub last_committed_offset: u64,
}

impl StorageFactory {
    pub fn new(config: StorageFactoryConfig, metadata_store: Arc<dyn MetadataStore>) -> Self {
        let segment_export_interval_seconds =
            config.segment_export_interval_seconds.unwrap_or(300);
        let durable_backend = config
            .durable_backend
            .clone()
            .or_else(|| {
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
        let segment_catalog = SegmentCatalog::new(metadata.clone());
        let mobility_state = MobilityState::new(metadata.clone());
        Self {
            mode: config.mode,
            base_cfg: config.wal,
            segment_catalog,
            mobility_state,
            durable_store,
            topics: Arc::new(DashMap::new()),
            segment_exporters: Arc::new(DashMap::new()),
            segment_exporter_tokens: Arc::new(DashMap::new()),
            segment_export_interval_seconds,
            deleter_cfg: config.retention.map(|retention| DeleterConfig {
                check_interval_minutes: retention.check_interval_minutes,
                retention_time_minutes: retention.time_minutes,
                retention_size_mb: retention.size_mb,
            }),
            deleters: Arc::new(DashMap::new()),
            deleter_tokens: Arc::new(DashMap::new()),
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
            let cancel = CancellationToken::new();
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
            self.segment_exporter_tokens
                .insert(topic_path.clone(), cancel);
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
                let cancel = CancellationToken::new();
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

    async fn get_or_create_wal(
        &self,
        topic_path: &str,
    ) -> Result<
        (
            HotLog,
            Option<std::path::PathBuf>,
            Option<Arc<CheckpointStore>>,
            bool,
        ),
        PersistentStorageError,
    > {
        if let Some(existing) = self.topics.get(topic_path) {
            return Ok((existing.clone(), None, None, false));
        }

        let mut cfg = self.base_cfg.clone();
        let mut root_path: Option<std::path::PathBuf> = None;
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
        } else if self.mode == StorageMode::CloudNative {
            return Err(PersistentStorageError::Other(
                "cloud_native requires wal.dir for durable WAL state".to_string(),
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
        let catalog_current_segment = match self.segment_catalog.metadata().get_current_segment_descriptor(topic_path).await {
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
        let (initial_offset, resumed_from_sealed) = match self.mobility_state.load(topic_path).await {
            Ok(Some(sealed_state)) if sealed_state.sealed => {
                (
                    Some(sealed_state.last_committed_offset.saturating_add(1)),
                    true,
                )
            }
            Ok(_) => {
                if matches!(self.mode, StorageMode::SharedFs | StorageMode::CloudNative)
                    && !local_wal_state_available
                {
                    match catalog_current_segment {
                        Some(segment) => (Some(segment.end_offset.saturating_add(1)), true),
                        None => (None, false),
                    }
                } else {
                    (None, false)
                }
            }
            Err(e) => {
                warn!(
                    target = "storage_factory",
                    topic = %topic_path,
                    error = %e,
                    "failed to read mobility state"
                );
                (None, false)
            }
        };

        let wal = crate::wal::Wal::with_config_with_store(cfg, ckpt_store.clone(), initial_offset).await?;
        let hot_log = HotLog::from(wal);
        self.topics.insert(topic_path.to_string(), hot_log.clone());
        Ok((hot_log, root_path, ckpt_store, resumed_from_sealed))
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

    async fn cut_and_export_topic_segments(
        &self,
        topic_path: &str,
        hot_log: &HotLog,
    ) -> Result<(), PersistentStorageError> {
        if self.mode != StorageMode::CloudNative {
            return Ok(());
        }
        hot_log.rotate().await?;
        self.export_topic_segments(topic_path, hot_log, false).await
    }

    async fn export_topic_segments(
        &self,
        topic_path: &str,
        hot_log: &HotLog,
        include_active_file: bool,
    ) -> Result<(), PersistentStorageError> {
        if !self.uses_sealed_segment_export() {
            return Ok(());
        }
        let durable_store = match self.durable_store.clone() {
            Some(store) => store,
            None if matches!(self.mode, StorageMode::SharedFs | StorageMode::CloudNative) => {
                return Err(PersistentStorageError::Other(
                    "durable storage mode requires durable backend for segment export".to_string(),
                ))
            }
            None => return Ok(()),
        };
        let wal_ckpt = match hot_log.current_wal_checkpoint().await {
            Some(ckpt) => ckpt,
            None => return Ok(()),
        };
        let existing_starts: HashSet<u64> = self
            .segment_catalog
            .list_segments(topic_path)
            .await?
            .into_iter()
            .map(|desc| desc.start_offset)
            .collect();

        let mut files: Vec<(u64, PathBuf)> = wal_ckpt
            .rotated_files
            .iter()
            .map(|(_, path, first_offset)| (*first_offset, path.clone()))
            .collect();
        if include_active_file {
            if let Some(first_offset) = wal_ckpt.active_file_first_offset {
                if !wal_ckpt.file_path.is_empty() {
                    files.push((first_offset, PathBuf::from(wal_ckpt.file_path.clone())));
                }
            }
        }
        files.sort_by_key(|(first_offset, _)| *first_offset);

        for (file_first_offset, path) in files {
            if existing_starts.contains(&file_first_offset) {
                continue;
            }
            let file_bytes = match tokio::fs::read(&path).await {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(PersistentStorageError::Io(format!(
                        "read local wal file {} failed: {}",
                        path.display(),
                        e
                    )))
                }
            };
            if file_bytes.is_empty() {
                continue;
            }

            let safe_len = scan_safe_frame_boundary_with_crc(&file_bytes);
            if safe_len == 0 {
                continue;
            }
            let safe_bytes = &file_bytes[..safe_len];
            let (start_offset, end_offset) = match extract_offsets_in_prefix(safe_bytes) {
                (Some(start), Some(end)) => (start, end),
                _ => continue,
            };
            let segment_id = format!(
                "data-{}-{}.dnb1",
                start_offset,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            );
            let object_path = format!("storage/topics/{}/segments/{}", topic_path, segment_id);
            let meta = durable_store.put_segment(&object_path, safe_bytes).await?;
            let desc = SegmentDescriptor {
                segment_id: segment_id.clone(),
                start_offset,
                end_offset,
                size: safe_len as u64,
                etag: meta.etag().map(|s| s.to_string()),
                created_at: chrono::Utc::now().timestamp() as u64,
                completed: true,
                offset_index: build_offset_index(safe_bytes),
            };
            let start_padded = format!("{:020}", start_offset);
            self.segment_catalog
                .put_segment(topic_path, &start_padded, &desc)
                .await?;
            self.segment_catalog
                .set_current_segment(topic_path, &start_padded)
                .await?;
        }

        Ok(())
    }

    async fn clear_topic_wal_state(&self, topic_path: &str) -> Result<(), PersistentStorageError> {
        if let Some(dir) = self.topic_wal_dir(topic_path) {
            match tokio::fs::remove_dir_all(&dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(PersistentStorageError::Io(format!(
                        "remove local wal dir {} failed: {}",
                        dir.display(),
                        e
                    )))
                }
            }
        }
        Ok(())
    }

    async fn delete_topic_durable_segments(
        &self,
        topic_path: &str,
    ) -> Result<(), PersistentStorageError> {
        let durable_store = match self.durable_store.clone() {
            Some(store) => store,
            None if matches!(self.mode, StorageMode::SharedFs | StorageMode::CloudNative) => {
                return Err(PersistentStorageError::Other(
                    "durable storage mode requires durable backend for segment deletion".to_string(),
                ))
            }
            None => return Ok(()),
        };
        let segments = self.segment_catalog.list_segments(topic_path).await?;
        for segment in segments {
            let object_path = format!("storage/topics/{}/segments/{}", topic_path, segment.segment_id);
            durable_store.delete_segment(&object_path).await?;
        }
        Ok(())
    }

    async fn stop_topic_background_tasks(&self, topic_path: &str) {
        if let Some((_, cancel)) = self.segment_exporter_tokens.remove(topic_path) {
            cancel.cancel();
        }
        if let Some((_, handle)) = self.segment_exporters.remove(topic_path) {
            let _ = handle.await;
        }
        if let Some((_, cancel)) = self.deleter_tokens.remove(topic_path) {
            cancel.cancel();
        }
        if let Some((_, handle)) = self.deleters.remove(topic_path) {
            let _ = handle.await;
        }
    }

    fn topic_wal_dir(&self, topic_path: &str) -> Option<PathBuf> {
        let mut root = self.base_cfg.dir.clone()?;
        let parts: Vec<&str> = topic_path.split('/').collect();
        if parts.len() == 2 {
            root.push(parts[0]);
            root.push(parts[1]);
        }
        Some(root)
    }

    fn wal_checkpoint_has_local_data(wal_checkpoint: &crate::checkpoint::WalCheckpoint) -> bool {
        wal_checkpoint
            .rotated_files
            .iter()
            .any(|(_, path, _)| path.exists())
            || (!wal_checkpoint.file_path.is_empty()
                && PathBuf::from(&wal_checkpoint.file_path).exists())
    }

    fn uses_sealed_segment_export(&self) -> bool {
        matches!(
            self.mode,
            StorageMode::Local | StorageMode::SharedFs | StorageMode::CloudNative
        )
    }

    fn durable_store_for_topic(&self) -> Result<Option<Arc<dyn DurableStore>>, PersistentStorageError> {
        match self.durable_store.clone() {
            Some(store) => Ok(Some(store)),
            None if matches!(self.mode, StorageMode::SharedFs | StorageMode::CloudNative) => Err(
                PersistentStorageError::Other(
                    "durable storage mode requires durable backend".to_string(),
                ),
            ),
            None => Ok(None),
        }
    }
}

fn normalize_topic_path(topic_name: &str) -> String {
    let trimmed = topic_name.strip_prefix('/').unwrap_or(topic_name);
    let mut parts = trimmed.split('/');
    let ns = parts.next().unwrap_or("");
    let topic = parts.next().unwrap_or("");
    format!("{}/{}", ns, topic)
}

fn build_offset_index(bytes: &[u8]) -> Option<Vec<(u64, u64)>> {
    let mut idx = 0usize;
    let mut offsets = Vec::new();
    let mut msgs_since_index = 0usize;
    while idx + FRAME_HEADER_SIZE <= bytes.len() {
        let offset = u64::from_le_bytes(bytes[idx..idx + 8].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[idx + 8..idx + 12].try_into().unwrap()) as usize;
        let next = idx + FRAME_HEADER_SIZE + len;
        if next > bytes.len() {
            break;
        }
        if msgs_since_index == 0 {
            offsets.push((offset, idx as u64));
        }
        msgs_since_index = (msgs_since_index + 1) % 1000;
        idx = next;
    }
    if offsets.is_empty() {
        None
    } else {
        Some(offsets)
    }
}
