use crate::opendal::{BackendConfig, LocalBackend};
use crate::wal::WalConfig;

#[derive(Debug, Clone)]
pub enum StorageMode {
    Local,
    SharedFs(SharedFsConfig),
    CloudNative(CloudNativeConfig),
}

#[derive(Debug, Clone)]
pub struct SharedFsConfig {
    pub root: String,
}

impl SharedFsConfig {
    pub fn new(root: impl Into<String>) -> Self {
        Self { root: root.into() }
    }
}

#[derive(Debug, Clone)]
pub struct CloudNativeConfig {
    pub durable_backend: BackendConfig,
}

impl CloudNativeConfig {
    pub fn new(durable_backend: BackendConfig) -> Self {
        Self { durable_backend }
    }
}

impl StorageMode {
    pub(crate) fn is_local(&self) -> bool {
        matches!(self, Self::Local)
    }

    pub(crate) fn is_cloud_native(&self) -> bool {
        matches!(self, Self::CloudNative(_))
    }

    pub(crate) fn requires_durable_backend(&self) -> bool {
        !self.is_local()
    }
}

#[derive(Debug, Clone)]
pub struct StorageFactoryConfig {
    pub(crate) mode: StorageMode,
    pub(crate) wal: WalConfig,
    pub(crate) metadata_root: String,
    pub(crate) retention: Option<RetentionConfig>,
    pub(crate) segment_export_interval_seconds: Option<u64>,
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
            retention: None,
            segment_export_interval_seconds: None,
        }
    }

    pub fn shared_fs(
        wal: WalConfig,
        metadata_root: impl Into<String>,
        root: impl Into<String>,
        retention: Option<RetentionConfig>,
    ) -> Self {
        Self {
            mode: StorageMode::SharedFs(SharedFsConfig::new(root)),
            wal,
            metadata_root: metadata_root.into(),
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
            mode: StorageMode::CloudNative(CloudNativeConfig::new(durable_backend)),
            wal,
            metadata_root: metadata_root.into(),
            retention,
            segment_export_interval_seconds: None,
        }
    }

    pub fn with_segment_export_interval_seconds(mut self, interval_seconds: u64) -> Self {
        self.segment_export_interval_seconds = Some(interval_seconds);
        self
    }

    pub(crate) fn durable_backend(&self) -> Option<BackendConfig> {
        match &self.mode {
            StorageMode::Local => self.wal.dir.as_ref().map(|dir| BackendConfig::Local {
                backend: LocalBackend::Fs,
                root: dir.to_string_lossy().to_string(),
            }),
            StorageMode::SharedFs(shared_fs) => Some(BackendConfig::Local {
                backend: LocalBackend::Fs,
                root: shared_fs.root.clone(),
            }),
            StorageMode::CloudNative(cloud_native) => Some(cloud_native.durable_backend.clone()),
        }
    }
}
