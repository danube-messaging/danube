use crate::policies::Policies;

use anyhow::{Context, Result};
use danube_core::storage::StorageConfig;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// configuration settings loaded from the config file
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LoadConfiguration {
    /// Danube cluster name
    pub(crate) cluster_name: String,
    /// Hostname or IP address shared by Broker, Admin API, and Prometheus exporter
    pub(crate) broker_host: String,
    /// Port for gRPC communication with the broker
    pub(crate) broker_port: usize,
    /// Port for the Admin API
    pub(crate) admin_port: usize,
    /// Port for Prometheus exporter
    pub(crate) prom_port: Option<usize>,
    /// Hostname or IP of Metadata Persistent Store (etcd)
    pub(crate) meta_store_host: String,
    /// Port for etcd or metadata store
    pub(crate) meta_store_port: usize,
    /// User Namespaces to be created on boot
    pub(crate) bootstrap_namespaces: Vec<String>,
    /// Broker policies, that can be overwritten by namespace / topic policies
    pub(crate) policies: Policies,
    /// Storage message configuration
    pub(crate) storage: StorageConfig,
}

/// configuration settings for the Danube broker service
/// includes various parameters that control the behavior and performance of the broker
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ServiceConfiguration {
    /// Danube cluster name
    pub(crate) cluster_name: String,
    /// Broker Service address for serving gRPC requests.
    pub(crate) broker_addr: std::net::SocketAddr,
    /// Broker Advertised address, used for kubernetes deployment
    pub(crate) advertised_addr: Option<String>,
    /// Admin API address
    pub(crate) admin_addr: std::net::SocketAddr,
    /// Prometheus exporter address
    pub(crate) prom_exporter: Option<std::net::SocketAddr>,
    /// Metadata Persistent Store (etcd) address
    pub(crate) meta_store_addr: String,
    /// User Namespaces to be created on boot
    pub(crate) bootstrap_namespaces: Vec<String>,
    /// Broker policies, that can be overwritten by namespace / topic policies
    pub(crate) policies: Policies,
    /// Storage message configuration
    pub(crate) storage: StorageConfig,
}

/// Implementing the TryFrom trait to transform LoadConfiguration into ServiceConfiguration
impl TryFrom<LoadConfiguration> for ServiceConfiguration {
    type Error = anyhow::Error;

    fn try_from(config: LoadConfiguration) -> Result<Self> {
        // Construct broker_addr from broker_host and broker_port
        let broker_addr: SocketAddr = format!("{}:{}", config.broker_host, config.broker_port)
            .parse()
            .context("Failed to create broker_addr")?;

        // Construct admin_addr from broker_host and admin_port
        let admin_addr: SocketAddr = format!("{}:{}", config.broker_host, config.admin_port)
            .parse()
            .context("Failed to create admin_addr")?;

        // Construct prom_exporter from broker_host and prom_port if provided
        let prom_exporter: Option<SocketAddr> = if let Some(prom_port) = config.prom_port {
            Some(
                format!("{}:{}", config.broker_host, prom_port)
                    .parse()
                    .context("Failed to create prom_exporter")?,
            )
        } else {
            None
        };

        // Construct meta_store_addr from meta_store_host and meta_store_port
        let meta_store_addr = format!("{}:{}", config.meta_store_host, config.meta_store_port);

        // Return the successfully created ServiceConfiguration
        Ok(ServiceConfiguration {
            cluster_name: config.cluster_name,
            broker_addr,
            advertised_addr: None,
            admin_addr,
            prom_exporter,
            meta_store_addr,
            bootstrap_namespaces: config.bootstrap_namespaces,
            policies: config.policies,
            storage: config.storage,
        })
    }
}
