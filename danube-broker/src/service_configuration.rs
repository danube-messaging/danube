use crate::danube_service::load_manager::config::LoadManagerConfig;
use crate::{auth::AuthConfig, policies::Policies};

use anyhow::{Context, Result};
// Legacy StorageConfig removed in Phase D
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

// For converting CloudConfig -> BackendConfig
use danube_persistent_storage::{BackendConfig, CloudBackend, LocalBackend};

/// configuration settings loaded from the config file
#[derive(Debug, Serialize, Deserialize)]
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
    /// Optional: new wal_cloud configuration (Phase D)
    pub(crate) wal_cloud: Option<WalCloudConfig>,
    /// Authentication configuration
    pub(crate) auth: AuthConfig,
    /// Load Manager configuration
    #[serde(default)]
    pub(crate) load_manager: Option<LoadManagerConfig>,
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
    /// Allow producers to auto-create topics when missing
    pub(crate) auto_create_topics: bool,
    /// Broker policies, that can be overwritten by namespace / topic policies
    pub(crate) policies: Policies,
    /// Optional: new wal_cloud configuration (Phase D)
    pub(crate) wal_cloud: Option<WalCloudConfig>,
    /// Authentication configuration
    pub(crate) auth: AuthConfig,
    /// Load Manager configuration
    pub(crate) load_manager: Option<LoadManagerConfig>,
}

/// Broker services configuration
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BrokerConfig {
    /// Hostname or IP address for all broker services
    pub(crate) host: String,
    /// Port configuration for broker services
    pub(crate) ports: BrokerPorts,
}

/// Broker port configuration
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BrokerPorts {
    /// Client connections port (producers/consumers)
    pub(crate) client: usize,
    /// Admin API port
    pub(crate) admin: usize,
    /// Prometheus metrics exporter port (optional)
    pub(crate) prometheus: Option<usize>,
}

/// Metadata store configuration
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MetaStoreConfig {
    /// Hostname or IP address of metadata store (etcd)
    pub(crate) host: String,
    /// Port for metadata store
    pub(crate) port: usize,
}

/// Implementing the TryFrom trait to transform LoadConfiguration into ServiceConfiguration
impl TryFrom<LoadConfiguration> for ServiceConfiguration {
    type Error = anyhow::Error;

    fn try_from(config: LoadConfiguration) -> Result<Self> {
        // Construct broker_addr from broker.host and broker.ports.client
        let broker_addr: SocketAddr = format!("{}:{}", config.broker.host, config.broker.ports.client)
            .parse()
            .context("Failed to create broker_addr")?;

        // Construct admin_addr from broker.host and broker.ports.admin
        let admin_addr: SocketAddr = format!("{}:{}", config.broker.host, config.broker.ports.admin)
            .parse()
            .context("Failed to create admin_addr")?;

        // Construct prom_exporter from broker.host and broker.ports.prometheus if provided
        let prom_exporter: Option<SocketAddr> = if let Some(prom_port) = config.broker.ports.prometheus {
            Some(
                format!("{}:{}", config.broker.host, prom_port)
                    .parse()
                    .context("Failed to create prom_exporter")?,
            )
        } else {
            None
        };

        // Construct meta_store_addr from meta_store.host and meta_store.port
        let meta_store_addr = format!("{}:{}", config.meta_store.host, config.meta_store.port);

        // Return the successfully created ServiceConfiguration
        Ok(ServiceConfiguration {
            cluster_name: config.cluster_name,
            broker_addr,
            advertised_addr: None,
            admin_addr,
            prom_exporter,
            meta_store_addr,
            bootstrap_namespaces: config.bootstrap_namespaces,
            auto_create_topics: config.auto_create_topics.unwrap_or(true),
            policies: config.policies,
            wal_cloud: config.wal_cloud,
            auth: config.auth,
            load_manager: config.load_manager,
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
    /// Optional per-topic retention policy (time + size based)
    pub(crate) retention: Option<WalRetentionNode>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct WalFlushNode {
    pub(crate) interval_ms: Option<u64>,
    pub(crate) max_batch_bytes: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct WalRotationNode {
    pub(crate) max_bytes: Option<u64>,
    /// Optional time-based rotation expressed in hours. If unset, rotation is size-only.
    pub(crate) max_hours: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct WalRetentionNode {
    /// Time-based retention in minutes; if None, time-based retention is disabled
    pub(crate) time_minutes: Option<u64>,
    /// Size-based retention in megabytes; if None, size-based retention is disabled
    pub(crate) size_mb: Option<u64>,
    /// How often to check for retention in minutes; defaults to 5 if not set
    pub(crate) check_interval_minutes: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct UploaderNode {
    pub(crate) interval_seconds: u64,
    /// Optional root prefix for uploader object/etcd paths (defaults to "/danube")
    pub(crate) root_prefix: Option<String>,
    /// Optional enable switch for uploader (defaults to true)
    pub(crate) enabled: Option<bool>,
    /// Optional max object size per tick in megabytes (Option A-lite). If None, no cap.
    pub(crate) max_object_mb: Option<u64>,
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
        /// When true, use virtual-hosted-style addressing (bucket.host). When false, use path-style.
        /// MinIO commonly requires path-style (set this to false).
        virtual_host_style: Option<bool>,
    },
    #[serde(rename = "gcs")]
    Gcs {
        root: String,
        project: Option<String>,
        credentials_json: Option<String>,
        credentials_path: Option<String>,
    },
    #[serde(rename = "azblob")]
    Azblob {
        /// Root format: "container" or "container/prefix"
        root: String,
        /// Example: http://127.0.0.1:10000/devstoreaccount1 or https://<account>.blob.core.windows.net
        endpoint: Option<String>,
        account_name: Option<String>,
        account_key: Option<String>,
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
                virtual_host_style,
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
                if let Some(v) = virtual_host_style {
                    options.insert("virtual_host_style".into(), v.to_string());
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
            CloudConfig::Azblob {
                root,
                endpoint,
                account_name,
                account_key,
            } => {
                let mut options: HashMap<String, String> = HashMap::new();
                if let Some(v) = endpoint {
                    options.insert("endpoint".into(), v.clone());
                }
                if let Some(v) = account_name {
                    options.insert("account_name".into(), v.clone());
                }
                if let Some(v) = account_key {
                    options.insert("account_key".into(), v.clone());
                }
                BackendConfig::Cloud {
                    backend: CloudBackend::Azblob,
                    root: root.clone(),
                    options,
                }
            }
        }
    }
}
