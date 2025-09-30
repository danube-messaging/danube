use crate::{auth::AuthConfig, policies::Policies};

use anyhow::{Context, Result};
// Legacy StorageConfig removed in Phase D
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::collections::HashMap;

// For converting CloudConfig -> BackendConfig
use danube_persistent_storage::{BackendConfig, CloudBackend, LocalBackend};

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
    /// Optional: new wal_cloud configuration (Phase D)
    pub(crate) wal_cloud: Option<WalCloudConfig>,
    /// Authentication configuration
    pub(crate) auth: AuthConfig,
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
    /// Optional: new wal_cloud configuration (Phase D)
    pub(crate) wal_cloud: Option<WalCloudConfig>,
    /// Authentication configuration
    pub(crate) auth: AuthConfig,
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
            wal_cloud: config.wal_cloud,
            auth: config.auth,
        })
    }
}

/// wal_cloud configuration (WAL + Cloud + Metadata)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct WalCloudConfig {
    pub(crate) wal: WalNode,
    pub(crate) uploader: UploaderNode,
    pub(crate) cloud: CloudConfig,
    pub(crate) metadata: MetadataNode,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct WalNode {
    pub(crate) dir: Option<String>,
    pub(crate) file_name: Option<String>,
    pub(crate) cache_capacity: Option<usize>,
    pub(crate) file_sync: Option<WalFlushNode>,
    pub(crate) rotation: Option<WalRotationNode>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct WalFlushNode {
    pub(crate) interval_ms: Option<u64>,
    pub(crate) max_batch_bytes: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct WalRotationNode {
    pub(crate) max_bytes: Option<u64>,
    pub(crate) max_seconds: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct UploaderNode {
    pub(crate) interval_seconds: u64,
    /// Optional root prefix for uploader object/etcd paths (defaults to "/danube")
    pub(crate) root_prefix: Option<String>,
    /// Optional enable switch for uploader (defaults to true)
    pub(crate) enabled: Option<bool>,
}

/// Cloud configuration enum (tagged by `backend`)
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "backend")]
pub(crate) enum CloudConfig {
    #[serde(rename = "memory")]
    Memory { root: String },
    #[serde(rename = "fs")]
    Fs { root: String },
    #[serde(rename = "s3")]
    S3 {
        root: String,
        region: Option<String>,
        endpoint: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        profile: Option<String>,
        role_arn: Option<String>,
        session_token: Option<String>,
        anonymous: Option<bool>,
    },
    #[serde(rename = "gcs")]
    Gcs {
        root: String,
        project: Option<String>,
        credentials_json: Option<String>,
        credentials_path: Option<String>,
    },
}

/// Metadata configuration node
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct MetadataNode {
    pub(crate) etcd_endpoint: Option<String>,
    pub(crate) in_memory: Option<bool>,
}

// Provide a conversion from broker CloudConfig to storage BackendConfig
impl From<&CloudConfig> for BackendConfig {
    fn from(cfg: &CloudConfig) -> Self {
        match cfg {
            CloudConfig::Memory { root } => BackendConfig::Local {
                backend: LocalBackend::Memory,
                root: root.clone(),
            },
            CloudConfig::Fs { root } => BackendConfig::Local {
                backend: LocalBackend::Fs,
                root: root.clone(),
            },
            CloudConfig::S3 {
                root,
                region,
                endpoint,
                access_key,
                secret_key,
                profile,
                role_arn,
                session_token,
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
                root,
                project,
                credentials_json,
                credentials_path,
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
        }
    }
}
