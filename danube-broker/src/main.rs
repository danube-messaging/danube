mod admin;
mod args_parse;
mod auth;
mod auth_jwt;
mod broker_metrics;
mod broker_server;
mod broker_service;
mod consumer;
mod danube_service;
mod dispatcher;
mod error_message;
mod message;
mod policies;
mod producer;
mod rate_limiter;
mod resources;
mod schema;
mod service_configuration;
mod subscription;
mod topic;
mod topic_cluster;
mod topic_schema;
mod topic_control;
mod topic_worker;
mod utils;

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
use danube_metadata_store::{EtcdStore, MetadataStorage};
use danube_persistent_storage::wal::deleter::DeleterConfig;
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{BackendConfig, UploaderBaseConfig, WalStorageFactory};
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

    // initialize the metadata storage layer for Danube Broker
    info!("Initializing ETCD as metadata persistent store");
    let metadata_store: MetadataStorage =
        MetadataStorage::Etcd(EtcdStore::new(service_config.meta_store_addr.clone()).await?);

    // Initialize WAL + Cloud (wal_cloud) configuration (Phase D)
    let wal_cfg = service_config
        .wal_cloud
        .as_ref()
        .cloned()
        .expect("wal_cloud configuration is required in Phase D");

    // Prepare WalConfig (per-topic WALs will be created by the factory using this as base)
    let wal_base_cfg = WalConfig {
        dir: wal_cfg.wal.dir.as_ref().map(|d| d.into()),
        file_name: wal_cfg.wal.file_name.clone(),
        cache_capacity: wal_cfg.wal.cache_capacity,
        fsync_interval_ms: wal_cfg.wal.file_sync.as_ref().and_then(|f| f.interval_ms),
        fsync_max_batch_bytes: wal_cfg
            .wal
            .file_sync
            .as_ref()
            .and_then(|f| f.max_batch_bytes),
        rotate_max_bytes: wal_cfg.wal.rotation.as_ref().and_then(|r| r.max_bytes),
        // Map hours (config) to seconds (WalConfig)
        rotate_max_seconds: wal_cfg
            .wal
            .rotation
            .as_ref()
            .and_then(|r| r.max_hours.map(|h| h.saturating_mul(3600))),
        ..Default::default()
    };

    // Build BackendConfig from CloudConfig (conversion defined in service_configuration.rs)
    let cloud_backend: BackendConfig = (&wal_cfg.cloud).into();

    // Create UploaderBaseConfig from broker configuration
    let uploader_base_cfg = UploaderBaseConfig {
        interval_seconds: wal_cfg.uploader.interval_seconds,
        max_object_mb: wal_cfg.uploader.max_object_mb,
    };

    // Build DeleterConfig (retention) from broker configuration (defaults handled in factory layer if needed)
    let deleter_cfg = if let Some(ret) = &wal_cfg.wal.retention {
        DeleterConfig {
            check_interval_minutes: ret.check_interval_minutes.unwrap_or(5),
            retention_time_minutes: ret.time_minutes,
            retention_size_mb: ret.size_mb,
        }
    } else {
        DeleterConfig {
            check_interval_minutes: 5,
            retention_time_minutes: None,
            retention_size_mb: None,
        }
    };

    // Create WalStorageFactory to encapsulate storage stack and per-topic uploaders
    let wal_factory = WalStorageFactory::new(
        wal_base_cfg,
        cloud_backend,
        metadata_store.clone(),
        wal_cfg
            .uploader
            .root_prefix
            .clone()
            .unwrap_or_else(|| "/danube".to_string()),
        uploader_base_cfg,
        deleter_cfg,
    );

    // caching metadata locally to reduce the number of remote calls to Metadata Store
    let local_cache = LocalCache::new(metadata_store.clone());

    // convenient functions to handle the metadata and configurations required
    // for managing the cluster, namespaces & topics
    let resources = Resources::new(local_cache.clone(), metadata_store.clone());

    // The synchronizer ensures that metadata & configuration settings across different brokers remains consistent.
    // using the client Producers to distribute metadata updates across brokers.
    let syncroniser = Syncronizer::new();

    // the broker service, is responsible to reliable deliver the messages from producers to consumers.
    let broker_service = BrokerService::new(
        resources.clone(),
        wal_factory,
        service_config.auto_create_topics,
    );
    let broker_id = broker_service.broker_id;

    // Init metrics with or without prometheus exporter
    if let Some(prometheus_exporter) = service_config.prom_exporter.clone() {
        init_metrics(Some(prometheus_exporter), broker_id);
    } else {
        init_metrics(None, broker_id);
    }

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
