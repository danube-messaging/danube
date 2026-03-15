use crate::checkpoint::CheckpointStore;
use crate::cloud::{BackendConfig, CloudStore, Uploader, UploaderBaseConfig, UploaderConfig};
use crate::hot_log::HotLog;
use crate::mobility_state::MobilityState;
use crate::segment_catalog::SegmentCatalog;
use crate::storage_metadata::{StorageMetadata, StorageStateSealed};
use crate::wal::deleter::{Deleter, DeleterConfig};
use crate::wal::WalConfig;
use crate::wal_storage::WalStorage;
use dashmap::DashMap;
use danube_core::metadata::MetadataStore;
use danube_core::storage::PersistentStorageError;
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
    pub mode: StorageMode,
    pub wal: WalConfig,
    pub metadata_root: String,
    pub durable_backend: Option<BackendConfig>,
    pub retention: Option<RetentionConfig>,
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
        }
    }
}

#[derive(Debug, Clone)]
pub struct StorageFactory {
    mode: StorageMode,
    base_cfg: WalConfig,
    segment_catalog: SegmentCatalog,
    mobility_state: MobilityState,
    durable_store: Option<CloudStore>,
    topics: Arc<DashMap<String, HotLog>>,
    uploaders: Arc<DashMap<String, JoinHandle<Result<(), PersistentStorageError>>>>,
    uploader_tokens: Arc<DashMap<String, CancellationToken>>,
    uploader_base_cfg: UploaderBaseConfig,
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
        let durable_store = config
            .durable_backend
            .clone()
            .map(CloudStore::new)
            .transpose()
            .expect("init durable store");
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
            uploaders: Arc::new(DashMap::new()),
            uploader_tokens: Arc::new(DashMap::new()),
            uploader_base_cfg: UploaderBaseConfig::default(),
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
        let (topic_wal, resolved_dir, ckpt_store) = self.get_or_create_wal(&topic_path).await?;

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

        if let (Some(store), Some(cfg)) = (self.durable_store.clone(), Some(self.uploader_base_cfg.clone())) {
            if self.mode != StorageMode::Local && !self.uploaders.contains_key(&topic_path) {
                let uploader_cfg = UploaderConfig::from_base(&cfg, topic_path.clone(), self.metadata_root.clone());
                let ckpt_for_uploader = ckpt_store.clone();
                if let Ok(uploader) = Uploader::new(
                    uploader_cfg,
                    store,
                    self.segment_catalog.metadata().clone(),
                    ckpt_for_uploader,
                ) {
                    let arc_up = Arc::new(uploader);
                    let cancel = CancellationToken::new();
                    let handle = arc_up.clone().start_with_cancel(cancel.clone());
                    self.uploaders.insert(topic_path.clone(), handle);
                    self.uploader_tokens.insert(topic_path.clone(), cancel);
                }
            }
        }

        if self.mode != StorageMode::Local && !self.deleters.contains_key(&topic_path) {
            if let (Some(_dir), Some(store), Some(cfg)) = (
                resolved_dir.clone(),
                ckpt_store.clone(),
                self.deleter_cfg.clone(),
            ) {
                let deleter = Deleter::new(topic_path.clone(), store, cfg);
                let arc_del = Arc::new(deleter);
                let cancel = CancellationToken::new();
                let handle = arc_del.clone().start_with_cancel(cancel.clone());
                self.deleters.insert(topic_path.clone(), handle);
                self.deleter_tokens.insert(topic_path.clone(), cancel);
            }
        }

        let storage = WalStorage::from_hot_log(topic_wal);
        match self.durable_store.clone() {
            Some(store) if self.mode != StorageMode::Local => Ok(storage.with_durable_history(
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
        ),
        PersistentStorageError,
    > {
        if let Some(existing) = self.topics.get(topic_path) {
            return Ok((existing.clone(), None, None));
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
            let uploader_ckpt = dir.join("uploader.ckpt");
            let store = Arc::new(CheckpointStore::new(wal_ckpt, uploader_ckpt));
            if let Err(e) = store.load_from_disk().await {
                warn!(target = "storage_factory", topic = %topic_path, error = %e, "failed to preload checkpoints from disk");
            }
            ckpt_store = Some(store);
        }

        let initial_offset = match self.mobility_state.load(topic_path).await {
            Ok(Some(sealed_state)) if sealed_state.sealed => {
                Some(sealed_state.last_committed_offset.saturating_add(1))
            }
            Ok(_) => None,
            Err(e) => {
                warn!(
                    target = "storage_factory",
                    topic = %topic_path,
                    error = %e,
                    "failed to read mobility state"
                );
                None
            }
        };

        let wal = crate::wal::Wal::with_config_with_store(cfg, ckpt_store.clone(), initial_offset).await?;
        let hot_log = HotLog::from(wal);
        self.topics.insert(topic_path.to_string(), hot_log.clone());
        Ok((hot_log, root_path, ckpt_store))
    }

    pub async fn shutdown(&self) {
        for item in self.uploader_tokens.iter() {
            item.value().cancel();
        }
        for item in self.deleter_tokens.iter() {
            item.value().cancel();
        }
        self.uploaders.clear();
        self.deleters.clear();
        self.uploader_tokens.clear();
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
        let hot_log = match self.topics.get(&topic_path) {
            Some(w) => w.clone(),
            None => {
                return Err(PersistentStorageError::Other(
                    "no storage for topic".to_string(),
                ))
            }
        };

        hot_log.flush().await?;
        if let Some(cancel) = self.uploader_tokens.get(&topic_path) {
            cancel.cancel();
        }
        if let Some(handle) = self.uploaders.remove(&topic_path) {
            let _ = handle.1.await;
        }
        if let Some(cancel) = self.deleter_tokens.get(&topic_path) {
            cancel.cancel();
        }
        if let Some(handle) = self.deleters.remove(&topic_path) {
            let _ = handle.1.await;
        }

        let commit_info = self.commit_info(topic_name).await?;
        let state = StorageStateSealed {
            sealed: true,
            last_committed_offset: commit_info.last_committed_offset,
            broker_id,
            timestamp: chrono::Utc::now().timestamp() as u64,
        };
        self.mobility_state.store(&topic_path, &state).await?;

        Ok(SealInfo {
            last_committed_offset: commit_info.last_committed_offset,
        })
    }

    pub async fn delete_storage_metadata(
        &self,
        topic_name: &str,
    ) -> Result<(), PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        self.segment_catalog.delete_topic(&topic_path).await
    }
}

fn normalize_topic_path(topic_name: &str) -> String {
    let trimmed = topic_name.strip_prefix('/').unwrap_or(topic_name);
    let mut parts = trimmed.split('/');
    let ns = parts.next().unwrap_or("");
    let topic = parts.next().unwrap_or("");
    format!("{}/{}", ns, topic)
}
