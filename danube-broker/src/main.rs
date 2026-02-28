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
mod message;
mod metadata_storage;
mod policies;
mod producer;
mod rate_limiter;
mod resources;
mod schema;
mod service_configuration;
mod subscription;
mod topic;
mod topic_cluster;
mod topic_control;
mod topic_schema;
mod topic_worker;
mod utils;

use std::{fs::read_to_string, path::Path, sync::Arc};

use crate::{
    args_parse::Args,
    broker_metrics::init_metrics,
    broker_service::BrokerService,
    danube_service::{DanubeService, LeaderElection, LoadManager, Syncronizer},
    resources::{Resources, LEADER_ELECTION_PATH},
    service_configuration::{LoadConfiguration, ServiceConfiguration},
};

use anyhow::{Context, Result};
use danube_persistent_storage::wal::deleter::DeleterConfig;
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{BackendConfig, UploaderBaseConfig, WalStorageFactory};
use danube_raft::node::{RaftNode, RaftNodeConfig};
use std::net::SocketAddr;

use crate::metadata_storage::MetadataStorage;

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
        // When no advertised_listeners configured, broker_url/connect_url must follow broker_addr
        if !service_config.proxy_enabled {
            // Preserve the scheme already set by TryFrom (based on auth mode)
            let scheme = if service_config.broker_url.starts_with("https://") {
                "https"
            } else {
                "http"
            };
            let url = format!("{}://{}", scheme, broker_address);
            service_config.broker_url = url.clone();
            service_config.connect_url = url;
        }
    }

    // If "advertised_addr" is provided via command-line args, override broker_url and connect_url
    // (simple K8s case: sets both to the same value, no proxy)
    if let Some(advertised_addr) = args.advertised_addr {
        let scheme = if service_config.broker_url.starts_with("https://") {
            "https"
        } else {
            "http"
        };
        let url = if advertised_addr.contains("://") {
            advertised_addr
        } else {
            format!("{}://{}", scheme, advertised_addr)
        };
        service_config.broker_url = url.clone();
        service_config.connect_url = url;
        service_config.proxy_enabled = false;
    }

    // If "connect_url" is provided, override connect_url only (proxy/ingress mode).
    // When connect_url differs from broker_url, proxy mode is enabled.
    if let Some(connect_url) = args.connect_url {
        let scheme = if service_config.broker_url.starts_with("https://") {
            "https"
        } else {
            "http"
        };
        let url = if connect_url.contains("://") {
            connect_url
        } else {
            format!("{}://{}", scheme, connect_url)
        };
        service_config.proxy_enabled = service_config.broker_url != url;
        service_config.connect_url = url;
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

    // If `data_dir` is provided via command-line args, override meta_store.data_dir
    if let Some(data_dir) = args.data_dir {
        service_config.meta_store.data_dir = data_dir;
    }

    // If `seed_nodes` is provided via command-line args, override meta_store.seed_nodes
    if let Some(seed_nodes_str) = args.seed_nodes {
        service_config.meta_store.seed_nodes = seed_nodes_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }

    // If `raft_addr` is provided via command-line args, override the value from the config file
    if let Some(raft_addr_arg) = args.raft_addr {
        let addr: SocketAddr = raft_addr_arg
            .parse()
            .context(format!("Failed to parse --raft-addr: {}", raft_addr_arg))?;
        service_config.raft_port = addr.port() as usize;
    }

    // initialize the Raft metadata storage layer
    // node_id is auto-generated on first boot and persisted in {data_dir}/node_id
    let meta_cfg = &service_config.meta_store;
    let raft_addr: SocketAddr = format!(
        "{}:{}",
        service_config.broker_addr.ip(),
        service_config.raft_port
    )
    .parse()
    .context("Failed to parse raft_addr")?;

    // Derive the advertised Raft address from the broker's advertised hostname.
    // In Docker, broker_url is e.g. "http://broker1:6650" → advertised raft addr = "broker1:7650".
    // On localhost (0.0.0.0), this stays None and falls back to raft_addr.
    let advertised_raft_addr = {
        let url = &service_config.broker_url;
        if let Some(rest) = url
            .strip_prefix("http://")
            .or_else(|| url.strip_prefix("https://"))
        {
            let host = rest.split(':').next().unwrap_or("");
            if !host.is_empty() && host != "0.0.0.0" && host != "127.0.0.1" {
                Some(format!("{}:{}", host, service_config.raft_port))
            } else {
                None
            }
        } else {
            None
        }
    };

    let raft_node = RaftNode::start(RaftNodeConfig {
        data_dir: (&meta_cfg.data_dir).into(),
        raft_addr,
        advertised_addr: advertised_raft_addr,
        ttl_check_interval: std::time::Duration::from_secs(5),
    })
    .await?;

    let node_id = raft_node.node_id;
    info!(node_id, %raft_addr, "Raft metadata store initialized");

    // Bootstrap the cluster (config-driven, NATS-style):
    //   - seed_nodes empty  → single-node auto-init (zero config)
    //   - seed_nodes present → discovers peers, lowest node_id initializes
    raft_node
        .bootstrap_cluster(
            &raft_node.advertised_addr,
            &service_config.meta_store.seed_nodes,
        )
        .await?;

    let raft_handle = raft_node.raft.clone();
    let leadership_handle = raft_node.leadership_handle();
    let advertised_raft_addr_str = raft_node.advertised_addr.clone();
    let metadata_store = MetadataStorage::Raft(std::sync::Arc::new(raft_node.store));

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
    let store_arc: Arc<dyn danube_core::metadata::MetadataStore> = Arc::new(metadata_store.clone());
    let wal_factory = WalStorageFactory::new(
        wal_base_cfg,
        cloud_backend,
        store_arc,
        wal_cfg
            .uploader
            .root_prefix
            .clone()
            .unwrap_or_else(|| "/danube".to_string()),
        uploader_base_cfg,
        deleter_cfg,
    );

    // convenient functions to handle the metadata and configurations required
    // for managing the cluster, namespaces & topics
    let resources = Resources::new(metadata_store.clone(), Some(leadership_handle.clone()));

    // The synchronizer ensures that metadata & configuration settings across different brokers remains consistent.
    // using the client Producers to distribute metadata updates across brokers.
    let syncroniser = Syncronizer::new();

    // The broker_id IS the Raft node_id — a single stable identity.
    let broker_id = node_id;

    // the broker service, is responsible to reliable deliver the messages from producers to consumers.
    let broker_service = BrokerService::new(
        broker_id,
        resources.clone(),
        wal_factory,
        service_config.auto_create_topics,
    );

    // Init metrics with or without prometheus exporter
    if let Some(prometheus_exporter) = service_config.prom_exporter.clone() {
        init_metrics(Some(prometheus_exporter), broker_id);
    } else {
        init_metrics(None, broker_id);
    }

    // Raft-based leader election: the Raft leader is the cluster leader.
    let leader_election_service = LeaderElection::new(
        leadership_handle.clone(),
        metadata_store.clone(),
        LEADER_ELECTION_PATH,
        broker_id,
    );

    // Load Manager, monitor and distribute load across brokers.
    let (assignment_strategy, rebalancing_config) =
        if let Some(ref lm_config) = service_config.load_manager {
            (
                lm_config.assignment_strategy.clone(),
                Some(lm_config.rebalancing.clone()),
            )
        } else {
            (Default::default(), None)
        };

    let load_manager = LoadManager::with_config(
        broker_id,
        metadata_store.clone(),
        assignment_strategy,
        rebalancing_config,
    );

    let broker: Arc<BrokerService> = Arc::new(broker_service);

    let broker_addr = service_config.broker_addr;
    info!(
        broker_addr = %broker_addr,
        broker_id = %broker_id,
        "initializing Danube message broker service"
    );

    // DanubeService coordinate and start all the services
    let mut danube = DanubeService::new(
        broker_id,
        Arc::clone(&broker),
        service_config,
        metadata_store,
        resources,
        leader_election_service,
        syncroniser,
        load_manager,
        raft_handle,
        leadership_handle,
        advertised_raft_addr_str,
    );

    danube
        .start()
        .await
        .expect("Danube Message Broker service unable to start");

    info!(
        broker_addr = %broker_addr,
        broker_id = %broker_id,
        "Danube message broker service started successfully"
    );

    Ok(())
}
