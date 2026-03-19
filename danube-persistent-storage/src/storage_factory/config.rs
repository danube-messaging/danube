use crate::opendal::{BackendConfig, ObjectStoreBackend};
use crate::wal::WalConfig;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum StorageMode {
    Local,
    SharedFs(SharedFsConfig),
    ObjectStore(ObjectStoreConfig),
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
pub enum ObjectStoreConfig {
    ObjectStore {
        backend: ObjectStoreBackend,
        root: String,
        options: HashMap<String, String>,
    },
    TestFilesystem {
        root: String,
    },
}

impl ObjectStoreConfig {
    pub fn new(backend: ObjectStoreBackend, root: impl Into<String>) -> Self {
        Self::ObjectStore {
            backend,
            root: root.into(),
            options: HashMap::new(),
        }
    }

    pub fn filesystem_for_tests(root: impl Into<String>) -> Self {
        Self::TestFilesystem { root: root.into() }
    }

    pub fn with_options(mut self, options: HashMap<String, String>) -> Self {
        if let Self::ObjectStore {
            options: current_options,
            ..
        } = &mut self
        {
            *current_options = options;
        }
        self
    }

    pub(crate) fn durable_backend(&self) -> BackendConfig {
        match self {
            Self::ObjectStore {
                backend,
                root,
                options,
            } => BackendConfig::ObjectStore {
                backend: backend.clone(),
                root: root.clone(),
                options: options.clone(),
            },
            Self::TestFilesystem { root } => BackendConfig::Filesystem { root: root.clone() },
        }
    }
}

impl StorageMode {
    pub(crate) fn is_local(&self) -> bool {
        matches!(self, Self::Local)
    }

    pub(crate) fn uses_export_later_durable_mode(&self) -> bool {
        matches!(self, Self::SharedFs(_) | Self::ObjectStore(_))
    }

    pub(crate) fn uses_background_export(&self) -> bool {
        self.uses_export_later_durable_mode()
    }

    pub(crate) fn uses_retention_deleter(&self) -> bool {
        self.uses_export_later_durable_mode()
    }

    pub(crate) fn requires_local_wal_staging(&self) -> bool {
        self.uses_export_later_durable_mode()
    }

    /// Return whether this mode requires a separate non-local durable segment
    /// backend instead of reusing local `wal.dir` segment storage.
    pub(crate) fn requires_separate_durable_backend(&self) -> bool {
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

    pub fn object_store(
        wal: WalConfig,
        metadata_root: impl Into<String>,
        object_store: ObjectStoreConfig,
        retention: Option<RetentionConfig>,
    ) -> Self {
        Self {
            mode: StorageMode::ObjectStore(object_store),
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

    /// Build the segment-store backend used for sealed/exported segment I/O.
    ///
    /// In `Local` mode this reuses `wal.dir` as a filesystem-backed segment
    /// store for sealed export and historical reads. In `SharedFs` and
    /// `ObjectStore` modes the durable segment backend is provided by the mode
    /// itself.
    pub(crate) fn durable_backend(&self) -> Option<BackendConfig> {
        match &self.mode {
            StorageMode::Local => self.wal.dir.as_ref().map(|dir| BackendConfig::Filesystem {
                root: dir.to_string_lossy().to_string(),
            }),
            StorageMode::SharedFs(shared_fs) => Some(BackendConfig::Filesystem {
                root: shared_fs.root.clone(),
            }),
            StorageMode::ObjectStore(object_store) => Some(object_store.durable_backend()),
        }
    }
}
