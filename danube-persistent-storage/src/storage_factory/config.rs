use crate::opendal::BackendConfig;
use crate::wal::WalConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    Local,
    SharedFs,
    CloudNative,
}

#[derive(Debug, Clone)]
pub struct StorageFactoryConfig {
    pub(crate) mode: StorageMode,
    pub(crate) wal: WalConfig,
    pub(crate) metadata_root: String,
    pub(crate) durable_backend: Option<BackendConfig>,
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
