use dashmap::DashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::checkpoint::CheckpointStore;
use crate::{
    cloud_store::{BackendConfig, CloudBackend, CloudStore, LocalBackend},
    etcd_metadata::EtcdMetadata,
    uploader::{Uploader, UploaderBaseConfig, UploaderConfig},
    wal::{Wal, WalConfig},
    wal_storage::WalStorage,
};
use danube_core::storage::PersistentStorageError;
use danube_metadata_store::MetadataStorage;
use tracing::{info, warn};

/// WalStorageFactory encapsulates the storage stack and produces per-topic WalStorage instances.
/// It also ensures a per-topic Uploader is started exactly once.
#[derive(Debug, Clone)]
pub struct WalStorageFactory {
    // base config provides the root dir and knobs to apply to each per-topic WAL
    base_cfg: WalConfig,
    // CloudStore instance for storing objects persistently in cloud
    cloud: CloudStore,
    // EtcdMetadata instance for storing metadata persistently in etcd
    etcd: EtcdMetadata,
    // topic_path (ns/topic) -> Wal
    topics: Arc<DashMap<String, Wal>>,
    // topic_path (ns/topic) -> uploader task handle
    uploaders: Arc<DashMap<String, JoinHandle<Result<(), PersistentStorageError>>>>,
    // Static namespace root for metadata paths
    root_prefix: String,
    // Base uploader configuration from broker config
    uploader_base_cfg: UploaderBaseConfig,
}

impl WalStorageFactory {
    /// Constructor: accepts BackendConfig and MetadataStorage and builds CloudStore/EtcdMetadata internally.
    pub fn new(
        cfg: WalConfig,
        backend: BackendConfig,
        metadata_store: MetadataStorage,
        etcd_root: impl Into<String>,
        uploader_base_cfg: UploaderBaseConfig,
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
        let etcd = EtcdMetadata::new(metadata_store, etcd_root.into());
        Self {
            base_cfg: cfg,
            topics: Arc::new(DashMap::new()),
            cloud,
            etcd,
            uploaders: Arc::new(DashMap::new()),
            root_prefix: "/danube".to_string(),
            uploader_base_cfg,
        }
    }

    /// Test constructor: accepts pre-created CloudStore and EtcdMetadata instances for shared memory stores
    /// This method is intended for testing only to allow sharing memory store instances
    pub fn new_with_stores(
        cfg: WalConfig,
        cloud: CloudStore,
        etcd: EtcdMetadata,
        uploader_base_cfg: UploaderBaseConfig,
    ) -> Self {
        Self {
            base_cfg: cfg,
            topics: Arc::new(DashMap::new()),
            cloud,
            etcd,
            uploaders: Arc::new(DashMap::new()),
            root_prefix: "/danube".to_string(),
            uploader_base_cfg,
        }
    }

    /// Create (or reuse) a WalStorage bound to the provided topic name ("/ns/topic").
    /// Also starts the per-topic uploader if not already running.
    pub async fn for_topic(&self, topic_name: &str) -> Result<WalStorage, PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        // Ensure per-topic WAL exists
        let (topic_wal, resolved_dir, ckpt_store) = self.get_or_create_wal(&topic_path).await?;
        if let Some(dir) = resolved_dir {
            info!(
                target = "wal_factory",
                topic = %topic_path,
                wal_dir = %dir.display(),
                "created per-topic WAL"
            );
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
            if let Ok(uploader) =
                Uploader::new(cfg, self.cloud.clone(), self.etcd.clone(), ckpt_store)
            {
                info!(target = "wal_factory", topic = %topic_path, "starting per-topic uploader");
                let handle = Arc::new(uploader).start();
                self.uploaders.insert(topic_path.clone(), handle);
            } else {
                warn!(target = "wal_factory", topic = %topic_path, "failed to initialize uploader for topic");
            }
        }

        Ok(WalStorage::from_wal(topic_wal).with_cloud(
            self.cloud.clone(),
            self.etcd.clone(),
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
                warn!(target = "wal_factory", topic = %topic_path, error = %format!("{}", e), "failed to preload checkpoints from disk");
            }
            ckpt_store = Some(store);
        }
        // Create WAL asynchronously with injected CheckpointStore
        let wal = Wal::with_config_with_store(cfg, ckpt_store.clone()).await?;
        self.topics.insert(topic_path.to_string(), wal.clone());
        Ok((wal, root_path, ckpt_store))
    }

    /// Best-effort shutdown: currently does not signal tasks, just drops handles.
    /// Can be extended to add a stop signal to Uploader.
    pub async fn shutdown(&self) {
        self.uploaders.clear();
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
            };
            format!("{} root={}", b, root)
        }
    }
}
