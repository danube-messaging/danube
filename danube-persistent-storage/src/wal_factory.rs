use dashmap::DashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::checkpoint::CheckpointStore;
use crate::wal::deleter::{Deleter, DeleterConfig};
use crate::{
    cloud::{
        BackendConfig, CloudBackend, CloudStore, LocalBackend, Uploader, UploaderBaseConfig,
        UploaderConfig,
    },
    storage_metadata::StorageMetadata,
    wal::{Wal, WalConfig},
    wal_storage::WalStorage,
};
use danube_core::storage::PersistentStorageError;
use tracing::{info, warn};

/// WalStorageFactory encapsulates the storage stack and produces per-topic WalStorage instances.
/// It also ensures a per-topic Uploader is started exactly once.
#[derive(Debug, Clone)]
pub struct WalStorageFactory {
    // base config provides the root dir and knobs to apply to each per-topic WAL
    base_cfg: WalConfig,
    // CloudStore instance for storing objects persistently in cloud
    cloud: CloudStore,
    // StorageMetadata instance for storing metadata persistently
    metadata: StorageMetadata,
    // topic_path (ns/topic) -> Wal
    topics: Arc<DashMap<String, Wal>>,
    // topic_path (ns/topic) -> uploader task handle
    uploaders: Arc<DashMap<String, JoinHandle<Result<(), PersistentStorageError>>>>,
    // topic_path (ns/topic) -> uploader cancellation token
    uploader_tokens: Arc<DashMap<String, CancellationToken>>,
    // Static namespace root for metadata paths
    root_prefix: String,
    // Base uploader configuration from broker config
    uploader_base_cfg: UploaderBaseConfig,
    // Retention (deleter) configuration
    deleter_cfg: DeleterConfig,
    // topic_path (ns/topic) -> deleter task handle
    deleters: Arc<DashMap<String, JoinHandle<Result<(), PersistentStorageError>>>>,
    // topic_path (ns/topic) -> deleter cancellation token
    deleter_tokens: Arc<DashMap<String, CancellationToken>>,
    // (no per-topic checkpoint map needed with cancel-and-drain uploader)
}

#[derive(Debug, Clone)]
pub struct SealInfo {
    pub last_committed_offset: u64,
}

impl WalStorageFactory {
    /// Constructor: accepts BackendConfig and a MetadataStore and builds CloudStore/StorageMetadata internally.
    pub fn new(
        cfg: WalConfig,
        backend: BackendConfig,
        metadata_store: Arc<dyn danube_core::metadata::MetadataStore>,
        metadata_root: impl Into<String>,
        uploader_base_cfg: UploaderBaseConfig,
        deleter_cfg: DeleterConfig,
    ) -> Self {
        // Log the chosen base WAL root and backend
        let wal_root = cfg
            .dir
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<none>".to_string());
        let backend_str = format_backend_string(&backend);
        info!(
            target = "wal_factory",
            wal_root = %wal_root,
            cloud_backend = %backend_str,
            "initializing WalStorageFactory with backend"
        );
        let cloud = CloudStore::new(backend).expect("init cloud store");
        let metadata = StorageMetadata::new(metadata_store, metadata_root.into());
        Self {
            base_cfg: cfg,
            topics: Arc::new(DashMap::new()),
            cloud,
            metadata,
            uploaders: Arc::new(DashMap::new()),
            uploader_tokens: Arc::new(DashMap::new()),
            root_prefix: "/danube".to_string(),
            uploader_base_cfg,
            deleter_cfg,
            deleters: Arc::new(DashMap::new()),
            deleter_tokens: Arc::new(DashMap::new()),
        }
    }

    /// Test constructor: accepts pre-created CloudStore and StorageMetadata instances for shared memory stores
    /// This method is intended for testing only to allow sharing memory store instances
    pub fn new_with_stores(
        cfg: WalConfig,
        cloud: CloudStore,
        metadata: StorageMetadata,
        uploader_base_cfg: UploaderBaseConfig,
        deleter_cfg: DeleterConfig,
    ) -> Self {
        Self {
            base_cfg: cfg,
            topics: Arc::new(DashMap::new()),
            cloud,
            metadata,
            uploaders: Arc::new(DashMap::new()),
            uploader_tokens: Arc::new(DashMap::new()),
            root_prefix: "/danube".to_string(),
            uploader_base_cfg,
            deleter_cfg,
            deleters: Arc::new(DashMap::new()),
            deleter_tokens: Arc::new(DashMap::new()),
        }
    }

    /// Create (or reuse) a WalStorage bound to the provided topic name ("/ns/topic").
    /// Also starts the per-topic uploader if not already running.
    pub async fn for_topic(&self, topic_name: &str) -> Result<WalStorage, PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        // Ensure per-topic WAL exists
        let (topic_wal, resolved_dir, ckpt_store) = self.get_or_create_wal(&topic_path).await?;
        if let Some(ref dir) = resolved_dir {
            info!(
                target = "wal_factory",
                topic = %topic_path,
                wal_dir = %dir.display(),
                "created per-topic WAL"
            );
            // Clean up sealed state marker after successful topic load to prevent reuse
            // This is non-critical, so we log but don't fail if cleanup fails
            if let Err(e) = self.metadata.delete_storage_state_sealed(&topic_path).await {
                warn!(
                    target = "wal_factory",
                    topic = %topic_path,
                    error = %e,
                    "failed to cleanup sealed state marker (non-critical)"
                );
            }
        } else {
            info!(
                target = "wal_factory",
                topic = %topic_path,
                "using existing per-topic WAL"
            );
        }

        // Start uploader once per topic
        if !self.uploaders.contains_key(&topic_path) {
            let cfg = UploaderConfig::from_base(
                &self.uploader_base_cfg,
                topic_path.clone(),
                self.root_prefix.clone(),
            );
            // clone the checkpoint store so we can still use it below for the deleter
            let ckpt_for_uploader = ckpt_store.clone();
            if let Ok(uploader) = Uploader::new(
                cfg,
                self.cloud.clone(),
                self.metadata.clone(),
                ckpt_for_uploader,
            ) {
                info!(target = "wal_factory", topic = %topic_path, "starting per-topic uploader");
                let arc_up = Arc::new(uploader);
                let cancel = CancellationToken::new();
                let handle = arc_up.clone().start_with_cancel(cancel.clone());
                self.uploaders.insert(topic_path.clone(), handle);
                self.uploader_tokens.insert(topic_path.clone(), cancel);
            } else {
                warn!(target = "wal_factory", topic = %topic_path, "failed to initialize uploader for topic");
            }
        }

        // Start deleter once per topic (only when checkpoints are available and a directory is configured)
        if !self.deleters.contains_key(&topic_path) {
            if let (Some(_dir), Some(store)) = (resolved_dir.clone(), ckpt_store.clone()) {
                let deleter = Deleter::new(topic_path.clone(), store, self.deleter_cfg.clone());
                let arc_del = Arc::new(deleter);
                let cancel = CancellationToken::new();
                let handle = arc_del.clone().start_with_cancel(cancel.clone());
                self.deleters.insert(topic_path.clone(), handle);
                self.deleter_tokens.insert(topic_path.clone(), cancel);
                info!(target = "wal_factory", topic = %topic_path, "starting per-topic deleter");
            }
        }

        Ok(WalStorage::from_wal(topic_wal).with_cloud(
            self.cloud.clone(),
            self.metadata.clone(),
            topic_path,
        ))
    }

    /// Get or create a per-topic WAL configured under <wal_root>/<ns>/<topic>/ and return it
    /// together with the resolved directory path (if any) for logging.
    async fn get_or_create_wal(
        &self,
        topic_path: &str,
    ) -> Result<
        (
            Wal,
            Option<std::path::PathBuf>,
            Option<std::sync::Arc<CheckpointStore>>,
        ),
        PersistentStorageError,
    > {
        if let Some(existing) = self.topics.get(topic_path) {
            // We cannot retrieve the store back from WAL trivially in this branch; factory tracks only Wal instances.
            // Return None for the store when reusing existing WAL.
            return Ok((existing.clone(), None, None));
        }
        // Build per-topic config by deriving dir from base root
        let mut cfg = self.base_cfg.clone();
        let mut root_path: Option<std::path::PathBuf> = None;
        let mut ckpt_store: Option<std::sync::Arc<CheckpointStore>> = None;
        if let Some(mut root) = cfg.dir.clone() {
            // Append ns/topic
            let parts: Vec<&str> = topic_path.split('/').collect();
            if parts.len() == 2 {
                root.push(parts[0]);
                root.push(parts[1]);
            }
            root_path = Some(root.clone());
            cfg.dir = Some(root);
        }
        // Initialize CheckpointStore if we have a directory
        if let Some(dir) = cfg.dir.as_ref() {
            let wal_ckpt = dir.join("wal.ckpt");
            let uploader_ckpt = dir.join("uploader.ckpt");
            let store = std::sync::Arc::new(CheckpointStore::new(wal_ckpt, uploader_ckpt));
            // Best-effort preload from disk
            if let Err(e) = store.load_from_disk().await {
                warn!(target = "wal_factory", topic = %topic_path, error = %e, "failed to preload checkpoints from disk");
            }
            ckpt_store = Some(store);
        }

        // Check for sealed state from previous broker to ensure offset continuity
        let initial_offset = match self.metadata.get_storage_state_sealed(&topic_path).await {
            Ok(Some(sealed_state)) if sealed_state.sealed => {
                let next_offset = sealed_state.last_committed_offset.saturating_add(1);
                info!(
                    target = "wal_factory",
                    topic = %topic_path,
                    sealed_offset = sealed_state.last_committed_offset,
                    from_broker = sealed_state.broker_id,
                    resuming_from = next_offset,
                    "found sealed state, resuming WAL from next offset to ensure continuity"
                );
                Some(next_offset)
            }
            Ok(Some(state)) => {
                // State exists but not sealed - shouldn't happen normally
                warn!(
                    target = "wal_factory",
                    topic = %topic_path,
                    state = ?state,
                    "found unsealed storage state marker, ignoring"
                );
                None
            }
            Ok(None) => {
                // No sealed state - new topic or same broker restart (will use local checkpoint)
                None
            }
            Err(e) => {
                warn!(
                    target = "wal_factory",
                    topic = %topic_path,
                    error = %e,
                    "failed to read sealed state, starting WAL from 0"
                );
                None
            }
        };

        // Create WAL asynchronously with injected CheckpointStore and initial offset
        let wal = Wal::with_config_with_store(cfg, ckpt_store.clone(), initial_offset).await?;
        self.topics.insert(topic_path.to_string(), wal.clone());
        Ok((wal, root_path, ckpt_store))
    }

    /// Best-effort shutdown: currently does not signal tasks, just drops handles.
    /// Can be extended to add a stop signal to Uploader.
    pub async fn shutdown(&self) {
        // best-effort cancel and clear
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

    /// Flush WAL, drain and stop uploader, stop deleter, and persist sealed state to metadata store.
    pub async fn flush_and_seal(
        &self,
        topic_name: &str,
        broker_id: u64,
    ) -> Result<SealInfo, PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        // Ensure WAL exists
        let wal = match self.topics.get(&topic_path) {
            Some(w) => w.clone(),
            None => {
                return Err(PersistentStorageError::Other(
                    "no WAL for topic".to_string(),
                ))
            }
        };
        // Flush WAL writer
        wal.flush().await?;
        // Cancel uploader and await completion (uploader drains on cancel)
        if let Some(cancel) = self.uploader_tokens.get(&topic_path) {
            cancel.cancel();
        }
        if let Some(handle) = self.uploaders.remove(&topic_path) {
            let _ = handle.1.await;
        }
        // Cancel deleter and await completion (no drain needed)
        if let Some(cancel) = self.deleter_tokens.get(&topic_path) {
            cancel.cancel();
        }
        if let Some(handle) = self.deleters.remove(&topic_path) {
            let _ = handle.1.await;
        }
        // After WAL flush and uploader drain, WAL tip reflects last committed offset boundary
        let last_committed_offset = wal.current_offset().saturating_sub(1);
        // Persist sealed state
        let state = crate::storage_metadata::StorageStateSealed {
            sealed: true,
            last_committed_offset,
            broker_id,
            timestamp: chrono::Utc::now().timestamp() as u64,
        };
        self.metadata
            .put_storage_state_sealed(&topic_path, &state)
            .await?;

        Ok(SealInfo {
            last_committed_offset,
        })
    }

    /// Delete storage metadata for a topic (objects, cur pointer, state).
    pub async fn delete_storage_metadata(
        &self,
        topic_name: &str,
    ) -> Result<(), PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        self.metadata.delete_storage_topic(&topic_path).await
    }
}

/// Convert a broker topic name "/ns/topic" into a storage topic path "ns/topic".
fn normalize_topic_path(topic_name: &str) -> String {
    let trimmed = topic_name.strip_prefix('/').unwrap_or(topic_name);
    // Danube model: only namespace and topic, ensure exactly 2 parts
    let mut parts = trimmed.split('/');
    let ns = parts.next().unwrap_or("");
    let topic = parts.next().unwrap_or("");
    format!("{}/{}", ns, topic)
}

/// Format backend configuration for logging purposes
fn format_backend_string(backend: &BackendConfig) -> String {
    match backend {
        BackendConfig::Local { backend, root } => {
            let b = match backend {
                LocalBackend::Memory => "local-memory",
                LocalBackend::Fs => "local-fs",
            };
            format!("{} root={}", b, root)
        }
        BackendConfig::Cloud { backend, root, .. } => {
            let b = match backend {
                CloudBackend::S3 => "s3",
                CloudBackend::Gcs => "gcs",
                CloudBackend::Azblob => "azblob",
            };
            format!("{} root={}", b, root)
        }
    }
}
