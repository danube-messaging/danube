mod admin;
mod args_parse;
mod auth;
mod auth_jwt;
mod broker_metrics;
mod broker_server;
mod broker_service;
mod consumer;
mod danube_service;
mod dispatch_strategy;
mod dispatcher;
mod error_message;
mod message;
mod policies;
mod producer;
mod resources;
mod schema;
mod service_configuration;
mod subscription;
mod topic;
mod topic_worker;
mod utils;

#[cfg(test)]
mod async_performance_test;

use std::{fs::read_to_string, path::Path, sync::Arc};

use crate::{
    args_parse::Args,
    broker_metrics::init_metrics,
    broker_service::BrokerService,
    danube_service::{DanubeService, LeaderElection, LoadManager, LocalCache, Syncronizer},
    resources::{Resources, LEADER_ELECTION_PATH},
    service_configuration::{LoadConfiguration, ServiceConfiguration},
};

use anyhow::{Context, Result};
use danube_core::storage::StorageConfig;
use danube_metadata_store::{EtcdStore, MetadataStorage};
use danube_reliable_dispatch::{create_message_storage, TopicCache};
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse()?;

    // Load the configuration from the specified YAML file
    let config_content = read_to_string(Path::new(&args.config_file))?;
    let load_config: LoadConfiguration = serde_yaml::from_str(&config_content)?;

    // Attempt to transform LoadConfiguration into ServiceConfiguration
    let mut service_config: ServiceConfiguration = load_config.try_into()?;

    // If `broker_addr` is provided via command-line args, override the value from the config file
    if let Some(broker_addr) = args.broker_addr {
        let broker_address: SocketAddr = broker_addr.parse().context(format!(
            "Failed to parse into Socket address: {}",
            broker_addr
        ))?;
        service_config.broker_addr = broker_address;
    }

    // If "advertised_addr" is provided via command-line args
    if let Some(advertised_addr) = args.advertised_addr {
        service_config.advertised_addr = Some(advertised_addr)
    }

    // If `admin_addr` is provided via command-line args, override the value from the config file
    if let Some(admin_addr) = args.admin_addr {
        let admin_address: SocketAddr = admin_addr.parse().context(format!(
            "Failed to parse into Socket address: {}",
            admin_addr
        ))?;
        service_config.admin_addr = admin_address;
    }

    // If `prom_exporter` is provided via command-line args, override the value from the config file
    if let Some(prom_exporter) = args.prom_exporter {
        let prom_address: SocketAddr = prom_exporter.parse().context(format!(
            "Failed to parse into Socket address: {}",
            prom_exporter
        ))?;
        service_config.prom_exporter = Some(prom_address);
    }

    // Init metrics with or without prometheus exporter
    if let Some(prometheus_exporter) = service_config.prom_exporter.clone() {
        init_metrics(Some(prometheus_exporter));
    } else {
        init_metrics(None)
    }

    // initialize the metadata storage layer for Danube Broker
    info!("Initializing ETCD as metadata persistent store");
    let metadata_store: MetadataStorage =
        MetadataStorage::Etcd(EtcdStore::new(service_config.meta_store_addr.clone()).await?);

    // initialize the message storage layer for Danube Broker
    info!(
        "Initializing {} for message persistence",
        service_config.storage
    );
    let message_storage = create_message_storage_from_config(&service_config.storage).await?;

    // caching metadata locally to reduce the number of remote calls to Metadata Store
    let local_cache = LocalCache::new(metadata_store.clone());

    // convenient functions to handle the metadata and configurations required
    // for managing the cluster, namespaces & topics
    let resources = Resources::new(local_cache.clone(), metadata_store.clone());

    // The synchronizer ensures that metadata & configuration settings across different brokers remains consistent.
    // using the client Producers to distribute metadata updates across brokers.
    let syncroniser = Syncronizer::new();

    // the broker service, is responsible to reliable deliver the messages from producers to consumers.
    let broker_service = BrokerService::new(resources.clone(), message_storage);
    let broker_id = broker_service.broker_id;

    // the service selects one broker per cluster to be the leader to coordinate and take assignment decision.
    let leader_election_service = LeaderElection::new(
        metadata_store.clone(),
        LEADER_ELECTION_PATH,
        broker_service.broker_id,
    );

    // Load Manager, monitor and distribute load across brokers.
    let load_manager = LoadManager::new(broker_service.broker_id, metadata_store.clone());

    let broker: Arc<BrokerService> = Arc::new(broker_service);

    let broker_addr = service_config.broker_addr;
    info!(
        "Initializing Danube Message Broker service on {}",
        broker_addr
    );

    // DanubeService coordinate and start all the services
    let mut danube = DanubeService::new(
        broker_id,
        Arc::clone(&broker),
        service_config,
        metadata_store,
        local_cache,
        resources,
        leader_election_service,
        syncroniser,
        load_manager,
    );

    danube
        .start()
        .await
        .expect("Danube Message Broker service unable to start");

    info!("Danube Message Broker service has started succesfully");

    Ok(())
}

/// Create message storage from configuration
async fn create_message_storage_from_config(storage_config: &StorageConfig) -> Result<TopicCache> {
    match storage_config {
        StorageConfig::InMemory { cache } => {
            let storage_backend = create_message_storage("inmemory")?;
            Ok(TopicCache::new(
                storage_backend.into(),
                cache.max_capacity as u64,
                cache.time_to_idle as u64,
            ))
        }
        StorageConfig::Local {
            local_config,
            cache,
        } => {
            // For now, fall back to in-memory storage for local disk
            // TODO: Implement proper local disk storage with PersistentStorage trait
            let storage_backend = create_message_storage("inmemory")?;
            Ok(TopicCache::new(
                storage_backend.into(),
                cache.max_capacity as u64,
                cache.time_to_idle as u64,
            ))
        }
        StorageConfig::Remote {
            remote_config,
            cache,
        } => {
            // For now, fall back to in-memory storage for remote
            // TODO: Implement proper remote storage with PersistentStorage trait
            let storage_backend = create_message_storage("inmemory")?;
            Ok(TopicCache::new(
                storage_backend.into(),
                cache.max_capacity as u64,
                cache.time_to_idle as u64,
            ))
        }
        StorageConfig::Iceberg { iceberg_config } => {
            // Create Iceberg storage backend
            use danube_iceberg_storage::{config::IcebergConfig, IcebergStorage};
            use danube_reliable_dispatch::create_persistent_storage_adapter;

            // Convert broker config to Iceberg config
            let config = IcebergConfig {
                catalog: danube_iceberg_storage::config::CatalogConfig::Rest {
                    uri: iceberg_config.catalog_uri.clone().unwrap_or_default(),
                    token: None,
                    properties: std::collections::HashMap::new(),
                },
                object_store: match iceberg_config.object_store_type.as_str() {
                    "s3" => danube_iceberg_storage::config::ObjectStoreConfig::S3 {
                        bucket: iceberg_config.bucket_name.clone().unwrap_or_default(),
                        region: iceberg_config.region.clone().unwrap_or_default(),
                        profile: None,
                        endpoint: iceberg_config.endpoint.clone(),
                        path_style: false,
                    },
                    "local" => danube_iceberg_storage::config::ObjectStoreConfig::Local {
                        path: iceberg_config
                            .local_path
                            .clone()
                            .unwrap_or_else(|| "/tmp/danube".to_string()),
                    },
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Unsupported object store type: {}",
                            iceberg_config.object_store_type
                        ))
                    }
                },
                wal: danube_iceberg_storage::config::WalConfig {
                    base_path: iceberg_config.wal_path.clone(),
                    max_file_size: iceberg_config.wal_segment_size as u64,
                    sync_mode: match iceberg_config.wal_sync_mode.as_str() {
                        "always" => danube_iceberg_storage::config::SyncMode::Always,
                        "periodic" => danube_iceberg_storage::config::SyncMode::Periodic,
                        "none" => danube_iceberg_storage::config::SyncMode::None,
                        _ => danube_iceberg_storage::config::SyncMode::Periodic,
                    },
                },
                warehouse: iceberg_config.warehouse_path.clone(),
                writer: danube_iceberg_storage::config::WriterConfig {
                    batch_size: iceberg_config.writer_batch_size,
                    flush_interval_ms: iceberg_config.writer_flush_interval_ms,
                    max_memory_bytes: iceberg_config.writer_max_memory_bytes,
                },
                reader: danube_iceberg_storage::config::ReaderConfig {
                    poll_interval_ms: iceberg_config.reader_poll_interval_ms,
                    max_concurrent_reads: 10, // Default value
                    prefetch_size: iceberg_config.reader_prefetch_batches,
                },
            };

            // Create Iceberg storage instance
            let iceberg_storage = IcebergStorage::new(config)
                .await
                .context("Failed to initialize Iceberg storage")?;

            // Create adapter to bridge with legacy TopicCache
            let storage_backend = create_persistent_storage_adapter(Arc::new(iceberg_storage));
            Ok(TopicCache::new(storage_backend.into(), 1000, 3600)) // Default cache settings for Iceberg
        }
    }
}
