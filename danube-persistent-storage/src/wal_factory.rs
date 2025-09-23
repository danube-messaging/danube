use dashmap::DashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::{
    cloud_store::CloudStore,
    etcd_metadata::EtcdMetadata,
    uploader::{Uploader, UploaderConfig},
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
    // topic_path (ns/topic) -> Wal
    topics: Arc<DashMap<String, Wal>>,
    cloud: CloudStore,
    etcd: EtcdMetadata,
    // topic_path (ns/topic) -> uploader task handle
    uploaders: Arc<DashMap<String, JoinHandle<Result<(), PersistentStorageError>>>>,
    // Static namespace root for metadata paths
    root_prefix: String,
    // Default uploader knobs (can be extended to read from config)
    default_interval_seconds: u64,
    default_max_batch_bytes: usize,
}

impl WalStorageFactory {
    /// Preferred constructor: takes a WalConfig (root + knobs) and builds per-topic WALs on demand.
    pub fn new_with_config(cfg: WalConfig, cloud: CloudStore, etcd: EtcdMetadata) -> Self {
        Self {
            base_cfg: cfg,
            topics: Arc::new(DashMap::new()),
            cloud,
            etcd,
            uploaders: Arc::new(DashMap::new()),
            root_prefix: "/danube".to_string(),
            default_interval_seconds: 10,
            default_max_batch_bytes: 8 * 1024 * 1024,
        }
    }

    /// Convenience constructor: accepts BackendConfig and MetadataStorage and builds CloudStore/EtcdMetadata internally.
    pub fn new_with_backend(
        cfg: WalConfig,
        backend: crate::cloud_store::BackendConfig,
        metadata_store: danube_metadata_store::MetadataStorage,
        etcd_root: impl Into<String>,
    ) -> Self {
        // Log the chosen base WAL root and backend
        let wal_root = cfg
            .dir
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<none>".to_string());
        let backend_str = match &backend {
            crate::cloud_store::BackendConfig::Local { backend, root } => {
                let b = match backend {
                    crate::cloud_store::LocalBackend::Memory => "local-memory",
                    crate::cloud_store::LocalBackend::Fs => "local-fs",
                };
                format!("{} root={}", b, root)
            }
            crate::cloud_store::BackendConfig::Cloud { backend, root, .. } => {
                let b = match backend {
                    crate::cloud_store::CloudBackend::S3 => "s3",
                    crate::cloud_store::CloudBackend::Gcs => "gcs",
                };
                format!("{} root={}", b, root)
            }
        };
        info!(
            target = "wal_factory",
            wal_root = %wal_root,
            cloud_backend = %backend_str,
            "initializing WalStorageFactory with backend"
        );
        let cloud = CloudStore::new(backend).expect("init cloud store");
        let etcd = EtcdMetadata::new(metadata_store, etcd_root.into());
        Self::new_with_config(cfg, cloud, etcd)
    }

    /// Get or create a per-topic WAL configured under <wal_root>/<ns>/<topic>/ and return it
    /// together with the resolved directory path (if any) for logging.
    async fn get_or_create_wal(&self, topic_path: &str) -> Result<(Wal, Option<std::path::PathBuf>), PersistentStorageError> {
        if let Some(existing) = self.topics.get(topic_path) {
            return Ok((existing.clone(), None));
        }
        // Build per-topic config by deriving dir from base root
        let mut cfg = self.base_cfg.clone();
        let mut resolved_dir: Option<std::path::PathBuf> = None;
        if let Some(mut root) = cfg.dir.clone() {
            // Append ns/topic
            let parts: Vec<&str> = topic_path.split('/').collect();
            if parts.len() == 2 {
                root.push(parts[0]);
                root.push(parts[1]);
            }
            resolved_dir = Some(root.clone());
            cfg.dir = Some(root);
        }
        // Create WAL asynchronously without blocking
        let wal = Wal::with_config(cfg).await?;
        self.topics.insert(topic_path.to_string(), wal.clone());
        Ok((wal, resolved_dir))
    }

    /// Create (or reuse) a WalStorage bound to the provided topic name ("/ns/topic").
    /// Also starts the per-topic uploader if not already running.
    pub async fn for_topic(&self, topic_name: &str) -> Result<WalStorage, PersistentStorageError> {
        let topic_path = normalize_topic_path(topic_name);
        // Ensure per-topic WAL exists
        let (topic_wal, resolved_dir) = self.get_or_create_wal(&topic_path).await?;
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
            let cfg = UploaderConfig {
                interval_seconds: self.default_interval_seconds,
                max_batch_bytes: self.default_max_batch_bytes,
                topic_path: topic_path.clone(),
                root_prefix: self.root_prefix.clone(),
            };
            if let Ok(uploader) = Uploader::new(cfg, topic_wal.clone(), self.cloud.clone(), self.etcd.clone()) {
                info!(target = "wal_factory", topic = %topic_path, "starting per-topic uploader");
                let handle = Arc::new(uploader).start();
                self.uploaders.insert(topic_path.clone(), handle);
            } else {
                warn!(target = "wal_factory", topic = %topic_path, "failed to initialize uploader for topic");
            }
        }

        Ok(WalStorage::from_wal(topic_wal)
            .with_cloud(self.cloud.clone(), self.etcd.clone(), topic_path))
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
