mod config;
mod lifecycle;
mod recovery;
mod segment_export;

pub use config::{
    ObjectStoreConfig, RetentionConfig, SharedFsConfig, StorageFactoryConfig, StorageMode,
};

use crate::durable_store::DurableStore;
use crate::metadata::{MobilityState, SegmentCatalog};
use crate::valkey::config::WriteBufferConfig;
use crate::valkey::ValkeyClient;
use crate::wal::Wal;
use crate::wal::deleter::DeleterConfig;
use dashmap::{DashMap, DashSet};
use danube_core::storage::PersistentStorageError;
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct StorageFactory {
    mode: Arc<RwLock<StorageMode>>,
    base_cfg: crate::wal::WalConfig,
    segment_catalog: SegmentCatalog,
    mobility_state: MobilityState,
    durable_store: Arc<RwLock<Option<Arc<dyn DurableStore>>>>,
    cloud_tiered_topics: Arc<DashSet<String>>,
    topics: Arc<DashMap<String, Wal>>,
    segment_exporters: Arc<DashMap<String, JoinHandle<Result<(), PersistentStorageError>>>>,
    segment_exporter_tokens: Arc<DashMap<String, CancellationToken>>,
    segment_export_interval_seconds: u64,
    deleter_cfg: Option<DeleterConfig>,
    deleters: Arc<DashMap<String, JoinHandle<Result<(), PersistentStorageError>>>>,
    deleter_tokens: Arc<DashMap<String, CancellationToken>>,
    metadata_root: String,
    write_buffer: Option<WriteBufferConfig>,
    /// Shared Valkey client, lazily initialized on first topic with write_buffer.
    valkey_client: Arc<tokio::sync::OnceCell<Arc<ValkeyClient>>>,
}

#[derive(Debug, Clone)]
pub struct CommitInfo {
    /// Highest offset already accepted by the local WAL for the topic.
    ///
    /// In export-later modes this is local WAL progress, not the durable export
    /// boundary in the segment catalog.
    pub last_committed_offset: u64,
}

#[derive(Debug, Clone)]
pub struct SealInfo {
    /// Highest offset already accepted by the local WAL before sealing.
    ///
    /// In export-later modes this is the local WAL boundary captured into sealed
    /// mobility state, not a claim that all offsets were already exported before
    /// `seal()` returned.
    pub last_committed_offset: u64,
}

pub(super) fn normalize_topic_path(topic_name: &str) -> String {
    topic_name.trim_start_matches('/').to_string()
}
