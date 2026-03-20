use crate::auth::{AuthConfig, AuthMode};
use crate::danube_service::load_manager::config::LoadManagerConfig;
use crate::policies::Policies;
use crate::storage_configuration::StorageConfig;

use anyhow::{Context, Result};
use serde::Deserialize;
use std::net::SocketAddr;

/// configuration settings loaded from the config file
#[derive(Debug, Deserialize)]
pub(crate) struct LoadConfiguration {
    /// Danube cluster name
    pub(crate) cluster_name: String,
    /// Broker services configuration
    pub(crate) broker: BrokerConfig,
    /// Metadata store configuration
    pub(crate) meta_store: MetaStoreConfig,
    /// User Namespaces to be created on boot
    pub(crate) bootstrap_namespaces: Vec<String>,
    /// Allow producers to auto-create topics when missing (if None, defaults to true)
    pub(crate) auto_create_topics: Option<bool>,
    /// Broker policies, that can be overwritten by namespace / topic policies
    pub(crate) policies: Policies,
    pub(crate) storage: StorageConfig,
    /// Authentication configuration
    pub(crate) auth: AuthConfig,
    /// Enable TLS on admin API (default: false, for remote management set to true)
    #[serde(default)]
    pub(crate) admin_tls: bool,
    /// Load Manager configuration
    #[serde(default)]
    pub(crate) load_manager: Option<LoadManagerConfig>,
}

/// configuration settings for the Danube broker service
/// includes various parameters that control the behavior and performance of the broker
#[derive(Debug, Deserialize)]
pub(crate) struct ServiceConfiguration {
    /// Danube cluster name
    pub(crate) cluster_name: String,
    /// Broker Service address for serving gRPC requests (bind address).
    pub(crate) broker_addr: std::net::SocketAddr,
    /// Internal broker identity (how other brokers find this broker)
    pub(crate) broker_url: String,
    /// External connect address (how clients reach this broker, may go through proxy)
    pub(crate) connect_url: String,
    /// Whether proxy mode is enabled (connect_url != broker_url)
    pub(crate) proxy_enabled: bool,
    /// Admin API address
    pub(crate) admin_addr: std::net::SocketAddr,
    /// Prometheus exporter address
    pub(crate) prom_exporter: Option<std::net::SocketAddr>,
    /// Raft inter-node gRPC transport port (from broker.ports.raft).
    pub(crate) raft_port: usize,
    /// Metadata store configuration (Raft data directory).
    pub(crate) meta_store: MetaStoreConfig,
    /// User Namespaces to be created on boot
    pub(crate) bootstrap_namespaces: Vec<String>,
    /// Allow producers to auto-create topics when missing
    pub(crate) auto_create_topics: bool,
    /// Broker policies, that can be overwritten by namespace / topic policies
    pub(crate) policies: Policies,
    pub(crate) storage: StorageConfig,
    /// Authentication configuration
    pub(crate) auth: AuthConfig,
    /// Enable TLS on admin API (default: false, for remote management set to true)
    pub(crate) admin_tls: bool,
    /// Load Manager configuration
    pub(crate) load_manager: Option<LoadManagerConfig>,
}

/// Broker services configuration
#[derive(Debug, Deserialize)]
pub(crate) struct BrokerConfig {
    /// Hostname or IP address for all broker services
    pub(crate) host: String,
    /// Port configuration for broker services
    pub(crate) ports: BrokerPorts,
    /// Optional advertised addresses for proxy/k8s mode
    #[serde(default)]
    pub(crate) advertised_listeners: Option<AdvertisedListeners>,
}

/// Optional advertised addresses for proxy/k8s mode
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct AdvertisedListeners {
    /// Internal identity: reachable inside the cluster (inter-broker, topic ownership)
    pub(crate) broker_url: String,
    /// External: where clients connect (proxy/ingress address)
    pub(crate) connect_url: String,
}

/// Broker port configuration
#[derive(Debug, Deserialize)]
pub(crate) struct BrokerPorts {
    /// Client connections port (producers/consumers)
    pub(crate) client: usize,
    /// Admin API port
    pub(crate) admin: usize,
    /// Raft inter-node gRPC transport port
    pub(crate) raft: usize,
    /// Prometheus metrics exporter port (optional)
    pub(crate) prometheus: Option<usize>,
}

/// Metadata store configuration (Raft-only, ETCD removed).
///
/// The `node_id` is auto-generated on first boot and persisted in `{data_dir}/node_id`.
/// The Raft transport port is in `broker.ports.raft`.
///
/// ```yaml
/// meta_store:
///   data_dir: "./danube-data/raft"
/// ```
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct MetaStoreConfig {
    /// Directory for Raft log store, snapshots, and auto-generated node_id.
    pub(crate) data_dir: String,
    /// Seed node Raft transport addresses for cluster formation.
    /// Empty (default) = single-node auto-init.
    /// Non-empty = multi-node: peers discover each other and auto-bootstrap.
    #[serde(default)]
    pub(crate) seed_nodes: Vec<String>,
}

/// Implementing the TryFrom trait to transform LoadConfiguration into ServiceConfiguration
impl TryFrom<LoadConfiguration> for ServiceConfiguration {
    type Error = anyhow::Error;

    fn try_from(config: LoadConfiguration) -> Result<Self> {
        let LoadConfiguration {
            cluster_name,
            broker,
            meta_store,
            bootstrap_namespaces,
            auto_create_topics,
            policies,
            storage,
            auth,
            admin_tls,
            load_manager,
        } = config;
        let BrokerConfig {
            host,
            ports,
            advertised_listeners,
        } = broker;
        let BrokerPorts {
            client,
            admin,
            raft,
            prometheus,
        } = ports;

        let broker_addr: SocketAddr = format!("{}:{}", host, client)
            .parse()
            .context("Failed to create broker_addr")?;
        let admin_addr: SocketAddr = format!("{}:{}", host, admin)
            .parse()
            .context("Failed to create admin_addr")?;
        let prom_exporter: Option<SocketAddr> = if let Some(prom_port) = prometheus {
            Some(
                format!("{}:{}", host, prom_port)
                    .parse()
                    .context("Failed to create prom_exporter")?,
            )
        } else {
            None
        };

        let scheme = match auth.mode {
            AuthMode::Tls | AuthMode::TlsWithJwt => "https",
            _ => "http",
        };
        let broker_addr_str = broker_addr.to_string();
        let (broker_url, connect_url) = if let Some(listeners) = advertised_listeners {
            (
                ensure_scheme(&listeners.broker_url, scheme),
                ensure_scheme(&listeners.connect_url, scheme),
            )
        } else {
            let url = format!("{}://{}", scheme, broker_addr_str);
            (url.clone(), url)
        };
        let proxy_enabled = broker_url != connect_url;

        Ok(ServiceConfiguration {
            cluster_name,
            broker_addr,
            broker_url,
            connect_url,
            proxy_enabled,
            admin_addr,
            prom_exporter,
            raft_port: raft,
            meta_store,
            bootstrap_namespaces,
            auto_create_topics: auto_create_topics.unwrap_or(true),
            policies,
            storage,
            auth,
            admin_tls,
            load_manager,
        })
    }
}

/// Ensures a URL string has a scheme prefix. If it already contains "://", returns as-is.
fn ensure_scheme(url: &str, default_scheme: &str) -> String {
    if url.contains("://") {
        url.to_string()
    } else {
        format!("{}://{}", default_scheme, url)
    }
}
