use crate::catalog::create_catalog;
use crate::config::{CatalogConfig, IcebergConfig};
use crate::errors::Result;
use crate::topic_reader::TopicReader;
use crate::topic_writer::TopicWriter;
use crate::wal::WriteAheadLog;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorage, PersistentStorageError};
use danube_metadata_store::MetadataStore; // bring trait into scope for get/put
use danube_metadata_store::{MetaOptions, MetadataStorage};
use futures::StreamExt;
use iceberg::{Catalog as IcebergCatalog, NamespaceIdent, TableIdent};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

/// IcebergStorage implements persistent storage using Apache Iceberg
#[derive(Debug)]
pub struct IcebergStorage {
    /// Configuration
    config: IcebergConfig,
    /// Write-Ahead Log for fast writes
    wal: Arc<WriteAheadLog>,
    /// Iceberg catalog
    catalog: Arc<dyn IcebergCatalog>,
    /// Topic writers for background processing
    topic_writers: Arc<tokio::sync::RwLock<HashMap<String, JoinHandle<()>>>>,
    /// Topic readers for streaming
    topic_readers: Arc<tokio::sync::RwLock<HashMap<String, JoinHandle<()>>>>,
    /// Message broadcasters per topic (fan-out per topic)
    message_senders:
        Arc<tokio::sync::RwLock<HashMap<String, tokio::sync::broadcast::Sender<StreamMessage>>>>,
    /// Optional metadata store for subscription progress (etcd or memory)
    metadata_store: Option<MetadataStorage>,
    /// Buffered per-subscription cursors to flush periodically to metadata store
    progress_buffer: Arc<RwLock<std::collections::HashMap<String, SubscriptionProgress>>>,
    /// Background task handle for periodic cursor flushing
    progress_flush_task: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl IcebergStorage {
    /// Create a new IcebergStorage instance
    pub async fn new(config: IcebergConfig) -> Result<Self> {
        info!("Initializing IcebergStorage");

        // Create catalog
        let catalog =
            create_catalog(&config.catalog, &config.object_store, &config.warehouse).await?;

        // Create Write-Ahead Log
        let wal = Arc::new(WriteAheadLog::new(&config.wal).await?);

        let storage = Self {
            config,
            wal,
            catalog,
            topic_writers: Arc::new(RwLock::new(HashMap::new())),
            topic_readers: Arc::new(RwLock::new(HashMap::new())),
            message_senders: Arc::new(RwLock::new(HashMap::new())),
            metadata_store: match std::env::var("DANUBE_METADATA_ENDPOINT") {
                Ok(endpoint) => match danube_metadata_store::EtcdStore::new(endpoint).await {
                    Ok(etcd) => Some(MetadataStorage::Etcd(etcd)),
                    Err(e) => {
                        warn!(error = %e, "Failed to initialize etcd metadata store; continuing without subscription progress store");
                        None
                    }
                },
                Err(_) => None,
            },
            progress_buffer: Arc::new(RwLock::new(HashMap::new())),
            progress_flush_task: Arc::new(RwLock::new(None)),
        };

        // Start periodic progress flusher if metadata store is enabled
        if storage.metadata_store.is_some() {
            let buffer = storage.progress_buffer.clone();
            let store = storage.metadata_store.clone();
            let flush_every_ms: u64 = std::env::var("DANUBE_SUB_CURSOR_FLUSH_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(10_000);
            let handle = tokio::spawn(async move {
                let mut ticker = interval(Duration::from_millis(flush_every_ms));
                loop {
                    ticker.tick().await;
                    // If metadata store went away, skip
                    let Some(store) = &store else { continue };
                    // Snapshot current buffer to reduce lock time
                    let snapshot: Vec<(String, SubscriptionProgress)> = {
                        let guard = buffer.read().await;
                        guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                    };
                    for (key, progress) in snapshot {
                        if let Err(e) = store
                            .put(
                                &key,
                                serde_json::to_value(&progress).unwrap_or_default(),
                                MetaOptions::None,
                            )
                            .await
                        {
                            warn!(key = %key, error = %e, "Failed to flush subscription cursor to metadata store");
                        }
                    }
                }
            });
            *storage.progress_flush_task.write().await = Some(handle);
        }

        Ok(storage)
    }

    /// Start background tasks for a topic
    async fn start_topic_tasks(&self, topic_name: &str) -> Result<mpsc::Sender<StreamMessage>> {
        // Guard: features require REST catalog for commit and scan planning
        if let CatalogConfig::Glue { .. } = self.config.catalog {
            return Err(crate::errors::IcebergStorageError::Config(
                "Glue catalog is not supported for commit_add_files/plan_scan; use REST catalog for Iceberg commits and precise reader incrementals".to_string(),
            )
            .into());
        }

        let mut writers = self.topic_writers.write().await;
        let mut readers = self.topic_readers.write().await;
        let mut senders = self.message_senders.write().await;

        // Check if tasks already exist
        if senders.contains_key(topic_name) {
            // Return a bridge mpsc sender to maintain trait compatibility for now
            let (tx, _rx) = mpsc::channel(1);
            return Ok(tx);
        }

        // Create broadcast channel for streaming (fan-out per topic)
        let (message_tx, _message_rx) = tokio::sync::broadcast::channel(1024);

        // Create shutdown channels
        let (_writer_shutdown_tx, writer_shutdown_rx) = mpsc::channel(1);
        let (_reader_shutdown_tx, reader_shutdown_rx) = mpsc::channel(1);

        // Create WAL reader
        let wal_reader = Arc::new(tokio::sync::Mutex::new(self.wal.create_reader()));

        // Convert topic_name to owned String for moving into async blocks
        let topic_name_owned = topic_name.to_string();
        let topic_name_for_reader = topic_name_owned.clone();

        // Start TopicWriter
        let writer = TopicWriter::new(
            topic_name_owned.clone(),
            wal_reader,
            self.catalog.clone(),
            writer_shutdown_rx,
            &self.config.writer,
            &self.config.warehouse,
        )
        .await?;

        let writer_handle = tokio::spawn(async move {
            if let Err(e) = writer.start().await {
                error!(topic = topic_name_owned, error = %e, "TopicWriter failed");
            }
        });

        // Start TopicReader
        let reader = TopicReader::new(
            topic_name_for_reader.clone(),
            self.catalog.clone(),
            &self.config.reader,
            message_tx.clone(),
            reader_shutdown_rx,
        )
        .await?;

        let reader_handle = tokio::spawn(async move {
            if let Err(e) = reader.start().await {
                error!(topic = topic_name_for_reader, error = %e, "TopicReader failed");
            }
        });

        // Store handles and sender
        writers.insert(topic_name.to_string(), writer_handle);
        readers.insert(topic_name.to_string(), reader_handle);
        senders.insert(topic_name.to_string(), message_tx.clone());

        info!(topic = topic_name, "Started background tasks for topic");

        // Return a dummy mpsc::Sender placeholder here; actual stream is created in create_message_stream()
        let (tx, _rx) = mpsc::channel(1);
        Ok(tx)
    }

    /// Build etcd path for subscription progress without affecting existing business flows
    /// Path: /persistent_storage/iceberg/subscriptions/{subscription}/{namespace}/{topic}
    fn subscription_progress_path(
        &self,
        namespace: &str,
        topic: &str,
        subscription: &str,
    ) -> String {
        format!(
            "/persistent_storage/iceberg/subscriptions/{}/{}/{}",
            subscription, namespace, topic
        )
    }

    /// Record a subscription's last processed position (snapshot/file/offset) into metadata store (if configured)
    pub async fn record_subscription_progress(
        &self,
        namespace: &str,
        topic: &str,
        subscription: &str,
        snapshot_id: i64,
        file_path: Option<String>,
        offset_in_file: Option<u64>,
    ) {
        // Immediate write (legacy behavior) retained for compatibility
        if let Some(store) = &self.metadata_store {
            let key = self.subscription_progress_path(namespace, topic, subscription);
            let value = json!({
                "snapshot_id": snapshot_id,
                "file_path": file_path,
                "offset_in_file": offset_in_file,
                "updated_ms": chrono::Utc::now().timestamp_millis(),
            });
            if let Err(e) = store.put(&key, value, MetaOptions::None).await {
                warn!(key = %key, error = %e, "Failed to persist subscription progress");
            }
        }
        // Also stage into buffered cursor for periodic flushes
        self.update_subscription_cursor(
            namespace,
            topic,
            subscription,
            snapshot_id,
            file_path,
            offset_in_file,
        )
        .await;
    }

    /// Update in-memory cursor for a subscription; periodically flushed to etcd by background task
    pub async fn update_subscription_cursor(
        &self,
        namespace: &str,
        topic: &str,
        subscription: &str,
        snapshot_id: i64,
        file_path: Option<String>,
        offset_in_file: Option<u64>,
    ) {
        let key = self.subscription_progress_path(namespace, topic, subscription);
        let progress = SubscriptionProgress {
            snapshot_id,
            file_path,
            offset_in_file,
            updated_ms: chrono::Utc::now().timestamp_millis(),
        };
        let mut guard = self.progress_buffer.write().await;
        guard.insert(key, progress);
    }

    /// Fetch a subscription's last processed position from metadata store (if configured)
    pub async fn get_subscription_progress(
        &self,
        namespace: &str,
        topic: &str,
        subscription: &str,
    ) -> Option<SubscriptionProgress> {
        if let Some(store) = &self.metadata_store {
            let key = self.subscription_progress_path(namespace, topic, subscription);
            match store.get(&key, MetaOptions::None).await {
                Ok(Some(val)) => serde_json::from_value::<SubscriptionProgress>(val).ok(),
                _ => None,
            }
        } else {
            None
        }
    }

    /// Create a message stream seeded from a stored subscription cursor in etcd.
    /// This performs a one-off backfill from the recorded snapshot (and file/offset when supported),
    /// then attaches to the live broadcast for ongoing messages.
    pub async fn create_subscription_stream(
        &self,
        namespace: &str,
        topic_name: &str,
        subscription: &str,
    ) -> std::result::Result<tokio::sync::mpsc::Receiver<StreamMessage>, PersistentStorageError>
    {
        // Ensure background tasks for the topic are running
        self.start_topic_tasks(topic_name)
            .await
            .map_err(|e| PersistentStorageError::Wal(e.to_string()))?;

        // Prepare output channel
        let (tx, rx) = mpsc::channel(1024);

        // Fetch stored cursor (if any)
        let cursor = self
            .get_subscription_progress(namespace, topic_name, subscription)
            .await;

        // Clone handles for async task
        let topic = topic_name.to_string();
        let catalog = self.catalog.clone();

        // Backfill task: read from snapshot_id if present, then attach to live broadcast
        let sender_guard = self.message_senders.read().await;
        let broadcaster = sender_guard
            .get(topic_name)
            .ok_or_else(|| {
                PersistentStorageError::Wal(format!("Topic {} not initialized", topic_name))
            })?
            .clone();
        drop(sender_guard);

        tokio::spawn(async move {
            // Backfill from snapshot if we have a cursor
            if let Some(progress) = cursor {
                if let Some(snapshot_id) = Some(progress.snapshot_id).filter(|id| *id > 0) {
                    // Load table and plan tasks for the specific snapshot
                    let ident = match TableIdent::from_strs(["danube", &topic]) {
                        Ok(i) => i,
                        Err(_) => {
                            // If ident fails, skip backfill and continue to live
                            let mut brx = broadcaster.subscribe();
                            while let Ok(msg) = brx.recv().await {
                                if tx.send(msg).await.is_err() {
                                    break;
                                }
                            }
                            return;
                        }
                    };

                    if let Ok(table) = catalog.load_table(&ident).await {
                        let reader = table.reader_builder().build();
                        if let Ok(scan) = table.scan().select_all().snapshot_id(snapshot_id).build()
                        {
                            if let Ok(tasks) = scan.plan_files().await {
                                if let Ok(mut batch_stream) = reader.read(tasks).await {
                                    while let Some(batch_res) = batch_stream.next().await {
                                        match batch_res {
                                            Ok(batch) => {
                                                match TopicReader::record_batch_to_messages(
                                                    &batch, &topic,
                                                ) {
                                                    Ok(mut msgs) => {
                                                        for msg in msgs.drain(..) {
                                                            if tx.send(msg).await.is_err() {
                                                                return;
                                                            }
                                                        }
                                                    }
                                                    Err(_) => { /* skip on decode error */ }
                                                }
                                            }
                                            Err(_) => { /* skip on read error */ }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Attach to live broadcast
            let mut brx = broadcaster.subscribe();
            while let Ok(msg) = brx.recv().await {
                if tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }
}

#[async_trait::async_trait]
impl PersistentStorage for IcebergStorage {
    async fn store_messages(
        &self,
        topic_name: &str,
        messages: Vec<StreamMessage>,
    ) -> std::result::Result<(), PersistentStorageError> {
        // Ensure topic tasks are running
        self.start_topic_tasks(topic_name)
            .await
            .map_err(|e| PersistentStorageError::Wal(e.to_string()))?;

        // Write messages to WAL for fast acknowledgment
        for message in messages {
            self.wal
                .write_message(message)
                .await
                .map_err(|e| PersistentStorageError::Wal(e.to_string()))?;
        }

        Ok(())
    }

    async fn create_subscription_stream(
        &self,
        namespace: &str,
        topic_name: &str,
        subscription: &str,
    ) -> std::result::Result<tokio::sync::mpsc::Receiver<StreamMessage>, PersistentStorageError>
    {
        self.create_subscription_stream(namespace, topic_name, subscription)
            .await
    }

    async fn get_write_position(
        &self,
        _topic_name: &str,
    ) -> std::result::Result<u64, PersistentStorageError> {
        // Return current WAL position
        Ok(self.wal.current_offset())
    }

    async fn get_committed_position(
        &self,
        topic_name: &str,
    ) -> std::result::Result<u64, PersistentStorageError> {
        // Derive committed position from Iceberg table's current snapshot
        // Namespace is fixed to "danube" for now
        let ident = TableIdent::from_strs(["danube", topic_name])
            .map_err(|e| PersistentStorageError::Catalog(e.to_string()))?;
        match self.catalog.load_table(&ident).await {
            Ok(table) => Ok(table.metadata().current_snapshot_id().unwrap_or(0) as u64),
            Err(e) => Err(PersistentStorageError::Catalog(e.to_string())),
        }
    }

    async fn create_topic(
        &self,
        topic_name: &str,
    ) -> std::result::Result<(), PersistentStorageError> {
        info!(topic = topic_name, "Creating topic");

        // Start background tasks which will create the Iceberg table
        self.start_topic_tasks(topic_name)
            .await
            .map_err(|e| PersistentStorageError::Wal(e.to_string()))?;

        Ok(())
    }

    async fn delete_topic(
        &self,
        topic_name: &str,
    ) -> std::result::Result<(), PersistentStorageError> {
        info!(topic = topic_name, "Deleting topic");

        // Stop background tasks
        let mut writers = self.topic_writers.write().await;
        let mut readers = self.topic_readers.write().await;
        let mut senders = self.message_senders.write().await;

        // Remove and abort tasks
        if let Some(writer_handle) = writers.remove(topic_name) {
            writer_handle.abort();
        }
        if let Some(reader_handle) = readers.remove(topic_name) {
            reader_handle.abort();
        }
        senders.remove(topic_name);

        // TODO: Delete Iceberg table from catalog

        Ok(())
    }

    async fn list_topics(&self) -> std::result::Result<Vec<String>, PersistentStorageError> {
        // List topics from Iceberg catalog
        let catalog = self.catalog.clone();
        let ns = NamespaceIdent::from_strs(["danube"])
            .map_err(|e| PersistentStorageError::Catalog(e.to_string()))?;
        let idents = catalog
            .list_tables(&ns)
            .await
            .map_err(|e| PersistentStorageError::Catalog(e.to_string()))?;
        let names = idents.into_iter().map(|ti| ti.name().to_string()).collect();
        Ok(names)
    }

    async fn shutdown(&self) -> std::result::Result<(), PersistentStorageError> {
        info!("Shutting down IcebergStorage");

        // Wait for all tasks to complete
        let writers = self.topic_writers.read().await;
        let readers = self.topic_readers.read().await;

        for (topic, handle) in writers.iter() {
            if !handle.is_finished() {
                warn!(topic = topic, "Waiting for TopicWriter to shutdown");
                handle.abort();
            }
        }

        for (topic, handle) in readers.iter() {
            if !handle.is_finished() {
                warn!(topic = topic, "Waiting for TopicReader to shutdown");
                handle.abort();
            }
        }

        // Shutdown WAL
        self.wal
            .shutdown()
            .await
            .map_err(|e| PersistentStorageError::Wal(e.to_string()))?;

        // Stop progress flusher
        if let Some(handle) = self.progress_flush_task.write().await.take() {
            handle.abort();
        }

        info!("IcebergStorage shutdown complete");

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionProgress {
    pub snapshot_id: i64,
    pub file_path: Option<String>,
    pub offset_in_file: Option<u64>,
    pub updated_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use danube_core::message::{MessageID, StreamMessage};
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn create_test_config() -> IcebergConfig {
        let temp_dir = tempdir().unwrap();

        IcebergConfig {
            catalog: CatalogConfig::Rest {
                uri: "http://localhost:8181".to_string(),
                token: None,
                properties: HashMap::new(),
            },
            object_store: ObjectStoreConfig::Local {
                path: temp_dir.path().join("data").to_string_lossy().to_string(),
            },
            wal: WalConfig {
                base_path: temp_dir.path().join("wal").to_string_lossy().to_string(),
                max_file_size: 1024 * 1024,
                sync_mode: SyncMode::Always,
            },
            warehouse: temp_dir
                .path()
                .join("warehouse")
                .to_string_lossy()
                .to_string(),
            writer: WriterConfig {
                batch_size: 100,
                flush_interval_ms: 1000,
                max_memory_bytes: 64 * 1024 * 1024,
            },
            reader: ReaderConfig {
                poll_interval_ms: 500,
                max_concurrent_reads: 5,
                prefetch_size: 3,
            },
        }
    }

    fn create_test_message(id: u64) -> StreamMessage {
        StreamMessage {
            request_id: id,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test_topic".to_string(),
                broker_addr: "localhost:6650".to_string(),
                segment_id: 0,
                segment_offset: 0,
            },
            payload: format!("test message {}", id).into_bytes(),
            publish_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            producer_name: "test_producer".to_string(),
            subscription_name: Some("test_subscription".to_string()),
            attributes: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_iceberg_storage_creation() {
        let config = create_test_config();
        let storage = IcebergStorage::new(config).await.unwrap();
        storage.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_messages() {
        let config = create_test_config();
        let storage = IcebergStorage::new(config).await.unwrap();

        let messages = vec![
            create_test_message(1),
            create_test_message(2),
            create_test_message(3),
        ];

        storage
            .store_messages("test_topic", messages)
            .await
            .unwrap();

        // Verify topic stats
        let stats = storage.get_write_position("test_topic").await.unwrap();
        assert!(stats > 0);

        storage.shutdown().await.unwrap();
    }
}
