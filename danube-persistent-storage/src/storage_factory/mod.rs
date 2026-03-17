mod config;
mod lifecycle;
mod recovery;
mod segment_export;

pub use config::{
    ObjectStoreConfig, RetentionConfig, SharedFsConfig, StorageFactoryConfig, StorageMode,
};

use crate::durable_store::DurableStore;
use crate::metadata::{MobilityState, SegmentCatalog};
use crate::wal::Wal;
use crate::wal::deleter::DeleterConfig;
use dashmap::DashMap;
use danube_core::storage::PersistentStorageError;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct StorageFactory {
    mode: StorageMode,
    base_cfg: crate::wal::WalConfig,
    segment_catalog: SegmentCatalog,
    mobility_state: MobilityState,
    durable_store: Option<Arc<dyn DurableStore>>,
    topics: Arc<DashMap<String, Wal>>,
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

pub(super) fn normalize_topic_path(topic_name: &str) -> String {
    let trimmed = topic_name.strip_prefix('/').unwrap_or(topic_name);
    let mut parts = trimmed.split('/');
    let ns = parts.next().unwrap_or("");
    let topic = parts.next().unwrap_or("");
    format!("{}/{}", ns, topic)
}
