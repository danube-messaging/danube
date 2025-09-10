use crate::catalog::{create_catalog, IcebergCatalog};
use crate::config::IcebergConfig;
use crate::errors::Result;
use crate::topic_reader::TopicReader;
use crate::topic_writer::TopicWriter;
use crate::wal::WriteAheadLog;
use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorage, PersistentStorageError};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// IcebergStorage implements persistent storage using Apache Iceberg
#[derive(Debug)]
pub struct IcebergStorage {
    /// Configuration
    config: IcebergConfig,
    /// Write-Ahead Log for fast writes
    wal: Arc<WriteAheadLog>,
    /// Object store for Parquet files
    object_store: Arc<dyn ObjectStore>,
    /// Iceberg catalog
    catalog: Arc<dyn IcebergCatalog>,
    /// Topic writers for background processing
    topic_writers: Arc<tokio::sync::RwLock<HashMap<String, JoinHandle<()>>>>,
    /// Topic readers for streaming
    topic_readers: Arc<tokio::sync::RwLock<HashMap<String, JoinHandle<()>>>>,
    /// Message senders for each topic
    message_senders: Arc<tokio::sync::RwLock<HashMap<String, mpsc::Sender<StreamMessage>>>>,
    /// Shutdown signal
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl IcebergStorage {
    /// Create a new IcebergStorage instance
    pub async fn new(config: IcebergConfig) -> Result<Self> {
        info!("Initializing IcebergStorage");

        // Create catalog
        let catalog = create_catalog(&config.catalog, &config.warehouse).await?;

        // Create Write-Ahead Log
        let wal = Arc::new(WriteAheadLog::new(&config.wal).await?);

        // Create object store
        let object_store = Self::create_object_store(&config).await?;

        Ok(Self {
            config,
            wal,
            object_store,
            catalog,
            topic_writers: Arc::new(RwLock::new(HashMap::new())),
            topic_readers: Arc::new(RwLock::new(HashMap::new())),
            message_senders: Arc::new(RwLock::new(HashMap::new())),
            shutdown_tx: None,
        })
    }

    /// Create object store based on configuration
    async fn create_object_store(config: &IcebergConfig) -> Result<Arc<dyn ObjectStore>> {
        match &config.object_store {
            crate::config::ObjectStoreConfig::Local { path } => {
                let local_store = LocalFileSystem::new_with_prefix(path)?;
                Ok(Arc::new(local_store))
            }
            crate::config::ObjectStoreConfig::S3 {
                bucket,
                region,
                endpoint,
                path_style,
                ..
            } => {
                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region(region);

                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }

                if *path_style {
                    builder = builder.with_virtual_hosted_style_request(false);
                }

                let s3_store = builder.build()?;
                Ok(Arc::new(s3_store))
            }
        }
    }

    /// Start background tasks for a topic
    async fn start_topic_tasks(&self, topic_name: &str) -> Result<mpsc::Sender<StreamMessage>> {
        let mut writers = self.topic_writers.write().await;
        let mut readers = self.topic_readers.write().await;
        let mut senders = self.message_senders.write().await;

        // Check if tasks already exist
        if senders.contains_key(topic_name) {
            return Ok(senders[topic_name].clone());
        }

        // Create message channel for streaming
        let (message_tx, message_rx) = mpsc::channel(1000);

        // Create shutdown channels
        let (writer_shutdown_tx, writer_shutdown_rx) = mpsc::channel(1);
        let (reader_shutdown_tx, reader_shutdown_rx) = mpsc::channel(1);

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
            self.object_store.clone(),
            writer_shutdown_rx,
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

        Ok(message_tx)
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

    async fn create_message_stream(
        &self,
        topic_name: &str,
        _start_position: Option<u64>,
    ) -> std::result::Result<tokio::sync::mpsc::Receiver<StreamMessage>, PersistentStorageError>
    {
        // Ensure topic tasks are running
        let _message_sender = self
            .start_topic_tasks(topic_name)
            .await
            .map_err(|e| PersistentStorageError::Wal(e.to_string()))?;

        // Create a new receiver for this consumer
        let (_tx, rx) = mpsc::channel(1000);

        // TODO: Wire up the actual message streaming from TopicReader
        Ok(rx)
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
        _topic_name: &str,
    ) -> std::result::Result<u64, PersistentStorageError> {
        // TODO: Track committed position from Iceberg snapshots
        Ok(0)
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
        let topics = catalog
            .list_tables("danube")
            .await
            .map_err(|e| PersistentStorageError::Catalog(e.to_string()))?;

        Ok(topics)
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

        info!("IcebergStorage shutdown complete");

        Ok(())
    }
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
