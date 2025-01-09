mod admin;
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
mod utils;

use std::{fs::read_to_string, path::Path, sync::Arc};

use crate::{
    broker_metrics::init_metrics,
    broker_service::BrokerService,
    danube_service::{DanubeService, LeaderElection, LoadManager, LocalCache, Syncronizer},
    resources::{Resources, LEADER_ELECTION_PATH},
    service_configuration::{LoadConfiguration, ServiceConfiguration},
};

use anyhow::{Context, Result};
use clap::Parser;
use danube_metadata_store::{EtcdStore, StorageBackend};
use std::net::SocketAddr;
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber;

pub(crate) mod proto {
    include!("proto/danube.rs");
}

pub(crate) mod admin_proto {
    include!("proto/danube_admin.rs");
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to config file (required)
    #[arg(short = 'c', long)]
    config_file: String,

    /// Danube Broker advertised address (optional, overrides config file)
    #[arg(short = 'b', long)]
    broker_addr: Option<String>,

    /// Danube Broker Admin address (optional, overrides config file)
    #[arg(short = 'a', long)]
    admin_addr: Option<String>,

    /// Prometheus Exporter http address (optional, overrides config file)
    #[arg(short = 'p', long)]
    prom_exporter: Option<String>,

    /// Advertised address - fqdn (optional, required for kubernetes deployment)
    #[arg(short = 'd', long)]
    advertised_addr: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();

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
    info!("Use ETCD storage as metadata persistent store");
    let metadata_store: StorageBackend =
        StorageBackend::Etcd(EtcdStore::new(service_config.meta_store_addr.clone()).await?);

    // caching metadata locally to reduce the number of remote calls to Metadata Store
    let local_cache = LocalCache::new(metadata_store.clone());

    // convenient functions to handle the metadata and configurations required
    // for managing the cluster, namespaces & topics
    let resources = Resources::new(local_cache.clone(), metadata_store.clone());

    // The synchronizer ensures that metadata & configuration settings across different brokers remains consistent.
    // using the client Producers to distribute metadata updates across brokers.
    let syncroniser = Syncronizer::new();

    // the broker service, is responsible to reliable deliver the messages from producers to consumers.
    let broker_service = BrokerService::new(resources.clone(), service_config.storage.clone());
    let broker_id = broker_service.broker_id;

    // the service selects one broker per cluster to be the leader to coordinate and take assignment decision.
    let leader_election_service = LeaderElection::new(
        metadata_store.clone(),
        LEADER_ELECTION_PATH,
        broker_service.broker_id,
    );

    // Load Manager, monitor and distribute load across brokers.
    let load_manager = LoadManager::new(broker_service.broker_id, metadata_store.clone());

    let broker: Arc<Mutex<BrokerService>> = Arc::new(Mutex::new(broker_service));

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

    info!("Start the Danube Service");
    danube.start().await.expect("the broker unable to start");

    info!("The Danube Service has started succesfully");

    Ok(())
}
