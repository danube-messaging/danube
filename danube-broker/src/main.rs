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
    service_configuration::{CloudConfig, LoadConfiguration, ServiceConfiguration},
};

use anyhow::{Context, Result};
use danube_metadata_store::{EtcdStore, MetadataStorage};
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::{
    BackendConfig, CloudBackend, CloudStore, EtcdMetadata, LocalBackend,
};
use std::collections::HashMap;
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

    // Initialize WAL + Cloud (wal_cloud) configuration (Phase D)
    let wal_cfg = service_config
        .wal_cloud
        .as_ref()
        .cloned()
        .expect("wal_cloud configuration is required in Phase D");

    // Build Wal
    let wal = Wal::with_config(WalConfig {
        dir: wal_cfg.wal.dir.as_ref().map(|d| d.into()),
        cache_capacity: wal_cfg.wal.cache_capacity,
        // rotation and retention mapping can be extended inside Wal as needed
        ..Default::default()
    })
    .await
    .expect("Failed to initialize WAL from wal_cloud.wal config");

    // Build CloudStore from config
    let cloud_backend = match wal_cfg.cloud {
        CloudConfig::Memory { ref root } => BackendConfig::Local {
            backend: LocalBackend::Memory,
            root: root.clone(),
        },
        CloudConfig::Fs { ref root } => BackendConfig::Local {
            backend: LocalBackend::Fs,
            root: root.clone(),
        },
        CloudConfig::S3 {
            ref root,
            ref region,
            ref endpoint,
            ref access_key,
            ref secret_key,
            ref profile,
            ref role_arn,
            ref session_token,
            anonymous,
        } => {
            let mut options: HashMap<String, String> = HashMap::new();
            if let Some(v) = region {
                options.insert("region".into(), v.clone());
            }
            if let Some(v) = endpoint {
                options.insert("endpoint".into(), v.clone());
            }
            if let Some(v) = access_key {
                options.insert("access_key".into(), v.clone());
            }
            if let Some(v) = secret_key {
                options.insert("secret_key".into(), v.clone());
            }
            if let Some(v) = profile {
                options.insert("profile".into(), v.clone());
            }
            if let Some(v) = role_arn {
                options.insert("role_arn".into(), v.clone());
            }
            if let Some(v) = session_token {
                options.insert("session_token".into(), v.clone());
            }
            if let Some(v) = anonymous {
                options.insert("anonymous".into(), v.to_string());
            }
            BackendConfig::Cloud {
                backend: CloudBackend::S3,
                root: root.clone(),
                options,
            }
        }
        CloudConfig::Gcs {
            ref root,
            ref project,
            ref credentials_json,
            ref credentials_path,
        } => {
            let mut options: HashMap<String, String> = HashMap::new();
            if let Some(v) = project {
                options.insert("project".into(), v.clone());
            }
            if let Some(v) = credentials_json {
                options.insert("credentials_json".into(), v.clone());
            }
            if let Some(v) = credentials_path {
                options.insert("credentials_path".into(), v.clone());
            }
            BackendConfig::Cloud {
                backend: CloudBackend::Gcs,
                root: root.clone(),
                options,
            }
        }
    };
    let cloud_store = CloudStore::new(cloud_backend).expect("init cloud store");

    // Wrap Metadata storage for object descriptors
    let etcd_meta = EtcdMetadata::new(metadata_store.clone(), "/danube".to_string());

    // caching metadata locally to reduce the number of remote calls to Metadata Store
    let local_cache = LocalCache::new(metadata_store.clone());

    // convenient functions to handle the metadata and configurations required
    // for managing the cluster, namespaces & topics
    let resources = Resources::new(local_cache.clone(), metadata_store.clone());

    // The synchronizer ensures that metadata & configuration settings across different brokers remains consistent.
    // using the client Producers to distribute metadata updates across brokers.
    let syncroniser = Syncronizer::new();

    // the broker service, is responsible to reliable deliver the messages from producers to consumers.
    let broker_service = BrokerService::new(resources.clone(), wal, cloud_store, etcd_meta);
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
