use crate::args_parse::NodeRole;
use crate::dispatch_config::DispatchConfig;
use crate::security::config::{AuthConfig, AuthMode};
use crate::danube_service::load_manager::config::LoadManagerConfig;
use crate::policies::Policies;
use crate::storage_configuration::StorageConfig;

use anyhow::{Context, Result};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

/// configuration settings for the Danube broker service
/// includes various parameters that control the behavior and performance of the broker
#[derive(Debug)]
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
    /// Raft inter-node gRPC transport port.
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
    /// Dispatch configuration (cluster-wide defaults for pipelining)
    pub(crate) dispatch_config: DispatchConfig,
    /// Edge replication configuration (edge mode only)
    pub(crate) edge_config: Option<EdgeConfig>,
    /// Workload role assigned to this node (broker, lakehouse, all)
    pub(crate) node_role: NodeRole,
}

/// Edge mode configuration (broker-side).
///
/// Edge settings (identity, replicator, MQTT) are constructed from CLI args (`edge_zero_yaml`),
/// dynamic cloud Raft heartbeat sync, or a local `--recovery-config` override.
#[derive(Debug, Clone)]
pub(crate) struct EdgeConfig {
    /// Inline edge config
    pub(crate) inline: danube_edge::config::EdgeConfig,
    /// Authentication token override (from --edge-token CLI arg)
    pub(crate) token_override: Option<String>,
}

/// Local break-glass recovery override configuration (`--recovery-config`).
///
/// Used when Raft quorum is lost or locked out so an operator can force seed nodes,
/// super admins, storage settings, load manager, auth, edge config, or bind addresses
/// from a local JSON/YAML file.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct RecoveryConfig {
    #[serde(default)]
    pub(crate) seed_nodes: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) super_admins: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) auto_create_topics: Option<bool>,
    #[serde(default)]
    pub(crate) policies: Option<Policies>,
    #[serde(default)]
    pub(crate) storage: Option<StorageConfig>,
    #[serde(default)]
    pub(crate) load_manager: Option<LoadManagerConfig>,
    #[serde(default)]
    pub(crate) auth: Option<AuthConfig>,
    #[serde(default)]
    pub(crate) edge: Option<danube_edge::config::EdgeConfig>,
    #[serde(default)]
    pub(crate) broker_addr: Option<String>,
    #[serde(default)]
    pub(crate) admin_addr: Option<String>,
}

/// Metadata store configuration (Raft-only, ETCD removed).
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

impl ServiceConfiguration {
    /// Creates a standalone configuration with sensible defaults.
    ///
    /// Standalone mode uses single-node Raft (auto-bootstraps), no LoadManager,
    /// and no rebalancing. Topics are loaded directly without cluster orchestration.
    pub(crate) fn standalone(base_dir: &Path) -> Result<Self> {
        let broker_addr: SocketAddr = "127.0.0.1:6650"
            .parse()
            .context("Failed to create standalone broker_addr")?;
        let admin_addr = broker_addr;

        Ok(Self {
            cluster_name: "STANDALONE".to_string(),
            broker_addr,
            broker_url: format!("http://{}", broker_addr),
            connect_url: format!("http://{}", broker_addr),
            proxy_enabled: false,
            admin_addr,
            prom_exporter: None,
            raft_port: 7650,
            meta_store: MetaStoreConfig {
                data_dir: base_dir.join("raft").to_string_lossy().into_owned(),
                seed_nodes: Vec::new(),
            },
            bootstrap_namespaces: vec!["default".to_string()],
            auto_create_topics: true,
            policies: Policies::new(),
            storage: StorageConfig::single_node(base_dir),
            auth: AuthConfig {
                mode: AuthMode::None,
                tls: None,
                jwt: None,
                super_admins: Vec::new(),
            },
            admin_tls: false,
            load_manager: None,
            dispatch_config: DispatchConfig::default(),
            edge_config: None,
            node_role: NodeRole::All,
        })
    }

    /// Creates a zero-YAML cluster configuration with sensible defaults rooted at `base_dir`.
    pub(crate) fn cluster_zero_yaml(base_dir: &Path) -> Result<Self> {
        let broker_addr: SocketAddr = "127.0.0.1:6650"
            .parse()
            .context("Failed to create zero-YAML cluster broker_addr")?;
        let admin_addr = broker_addr;

        Ok(Self {
            cluster_name: "DANUBE".to_string(),
            broker_addr,
            broker_url: format!("http://{}", broker_addr),
            connect_url: format!("http://{}", broker_addr),
            proxy_enabled: false,
            admin_addr,
            prom_exporter: None,
            raft_port: 7650,
            meta_store: MetaStoreConfig {
                data_dir: base_dir.join("raft").to_string_lossy().into_owned(),
                seed_nodes: Vec::new(),
            },
            bootstrap_namespaces: vec!["default".to_string()],
            auto_create_topics: true,
            policies: Policies::new(),
            storage: StorageConfig::single_node(base_dir),
            auth: AuthConfig {
                mode: AuthMode::None,
                tls: None,
                jwt: None,
                super_admins: Vec::new(),
            },
            admin_tls: false,
            load_manager: Some(LoadManagerConfig::default()),
            dispatch_config: DispatchConfig::default(),
            edge_config: None,
            node_role: NodeRole::All,
        })
    }

    /// Creates a zero-YAML edge configuration using `--cloud-endpoint` and optional `--edge-name` / `--mqtt-addr`.
    pub(crate) fn edge_zero_yaml(
        base_dir: &Path,
        edge_name: Option<String>,
        cloud_endpoint: String,
        mqtt_addr: Option<String>,
        token_override: Option<String>,
    ) -> Result<Self> {
        let resolved_edge_name = edge_name
            .filter(|n| !n.trim().is_empty())
            .or_else(|| {
                std::env::var("HOSTNAME")
                    .ok()
                    .filter(|h| !h.trim().is_empty())
            })
            .unwrap_or_else(|| "edge-node".to_string());
        let edge_cfg = danube_edge::config::EdgeConfig::zero_yaml(
            resolved_edge_name,
            cloud_endpoint,
            mqtt_addr.unwrap_or_else(|| "0.0.0.0:1883".to_string()),
            token_override.clone().unwrap_or_default(),
        );
        Self::edge_from_loaded(base_dir, edge_cfg, token_override)
    }

    fn edge_from_loaded(
        base_dir: &Path,
        edge_cfg: danube_edge::config::EdgeConfig,
        token_override: Option<String>,
    ) -> Result<Self> {
        let edge_name = edge_cfg.edge.edge_name.clone();

        let broker_addr: SocketAddr = "127.0.0.1:6650"
            .parse()
            .context("Failed to create edge broker_addr")?;
        let admin_addr = broker_addr;

        Ok(Self {
            cluster_name: format!("EDGE-{}", edge_name),
            broker_addr,
            broker_url: format!("http://{}", broker_addr),
            connect_url: format!("http://{}", broker_addr),
            proxy_enabled: false,
            admin_addr,
            prom_exporter: None,
            raft_port: 7650,
            meta_store: MetaStoreConfig {
                data_dir: base_dir.join("raft").to_string_lossy().into_owned(),
                seed_nodes: Vec::new(),
            },
            bootstrap_namespaces: vec!["default".to_string(), edge_name],
            auto_create_topics: true,
            policies: Policies::new(),
            storage: StorageConfig::single_node(base_dir),
            auth: AuthConfig {
                mode: AuthMode::None,
                tls: None,
                jwt: None,
                super_admins: Vec::new(),
            },
            admin_tls: false,
            load_manager: None,
            dispatch_config: DispatchConfig::default(),
            edge_config: Some(EdgeConfig {
                inline: edge_cfg,
                token_override,
            }),
            node_role: NodeRole::All,
        })
    }

    /// Apply a local break-glass `--recovery-config` JSON or YAML file.
    pub(crate) fn apply_recovery_config(&mut self, path: &Path) -> Result<()> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read recovery config {}", path.display()))?;
        let recovery: RecoveryConfig = serde_json::from_str(&raw)
            .or_else(|_| serde_yaml::from_str(&raw))
            .with_context(|| format!("failed to parse recovery config {} as JSON or YAML", path.display()))?;

        if let Some(seed_nodes) = recovery.seed_nodes {
            self.meta_store.seed_nodes = seed_nodes;
        }
        if let Some(super_admins) = recovery.super_admins {
            self.auth.super_admins = super_admins;
        }
        if let Some(auto_create) = recovery.auto_create_topics {
            self.auto_create_topics = auto_create;
        }
        if let Some(policies) = recovery.policies {
            self.policies = policies;
        }
        if let Some(storage) = recovery.storage {
            self.storage = storage;
        }
        if let Some(load_manager) = recovery.load_manager {
            self.load_manager = Some(load_manager);
        }
        if let Some(auth) = recovery.auth {
            let is_tls = auth.mode == AuthMode::Tls && auth.tls.is_some();
            self.auth = auth;
            self.admin_tls = is_tls;
            if is_tls {
                self.broker_url = self.broker_url.replace("http://", "https://");
                self.connect_url = self.connect_url.replace("http://", "https://");
            }
        }
        if let Some(edge_cfg) = recovery.edge {
            let edge_name = edge_cfg.edge.edge_name.clone();
            self.cluster_name = format!("EDGE-{}", edge_name);
            if !self.bootstrap_namespaces.iter().any(|n| n == &edge_name) {
                self.bootstrap_namespaces.push(edge_name);
            }
            let existing_override = self
                .edge_config
                .as_ref()
                .and_then(|existing| existing.token_override.clone());
            self.edge_config = Some(EdgeConfig {
                inline: edge_cfg,
                token_override: existing_override,
            });
        }
        if let Some(broker_addr) = recovery.broker_addr {
            let addr: SocketAddr = broker_addr
                .parse()
                .with_context(|| format!("invalid recovery broker_addr: {}", broker_addr))?;
            self.broker_addr = addr;
            if !self.proxy_enabled {
                let scheme = if self.broker_url.starts_with("https://") {
                    "https"
                } else {
                    "http"
                };
                let url = format!("{}://{}", scheme, addr);
                self.broker_url = url.clone();
                self.connect_url = url;
            }
        }
        if let Some(admin_addr) = recovery.admin_addr {
            self.admin_addr = admin_addr
                .parse()
                .with_context(|| format!("invalid recovery admin_addr: {}", admin_addr))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ServiceConfiguration;
    use crate::security::config::AuthMode;
    use crate::storage_configuration::StorageConfig;
    use std::path::Path;

    #[test]
    fn builds_standalone_service_configuration() {
        let config = ServiceConfiguration::standalone(Path::new("local-data"))
            .expect("standalone config");

        assert_eq!(config.cluster_name, "STANDALONE");
        assert_eq!(config.broker_addr.to_string(), "127.0.0.1:6650");
        assert_eq!(config.admin_addr.to_string(), "127.0.0.1:6650");
        assert_eq!(config.raft_port, 7650);
        assert_eq!(config.meta_store.seed_nodes, Vec::<String>::new());
        assert_eq!(config.meta_store.data_dir, Path::new("local-data").join("raft").to_string_lossy());
        assert_eq!(config.bootstrap_namespaces, vec!["default".to_string()]);
        assert!(config.auto_create_topics);
        assert_eq!(config.auth.mode, AuthMode::None);
        assert!(!config.admin_tls);
        assert!(config.prom_exporter.is_none());

        // Standalone mode has no load manager
        assert!(config.load_manager.is_none());

        match config.storage {
            StorageConfig::Local { local_wal_root, .. } => {
                assert_eq!(
                    local_wal_root,
                    Path::new("local-data").join("wal").to_string_lossy()
                );
            }
            _ => panic!("standalone storage must be local"),
        }
    }

    #[test]
    fn builds_cluster_zero_yaml_and_recovery_override() {
        let mut config = ServiceConfiguration::cluster_zero_yaml(Path::new("/tmp/danube-cluster"))
            .expect("zero-YAML cluster config");
        assert_eq!(config.cluster_name, "DANUBE");
        assert!(config.load_manager.is_some());
        assert_eq!(
            config.meta_store.data_dir,
            Path::new("/tmp/danube-cluster").join("raft").to_string_lossy()
        );

        let rec_path = std::env::temp_dir().join(format!("danube_recovery_{}.json", std::process::id()));
        std::fs::write(
            &rec_path,
            r#"{"seed_nodes": ["node1:7650"], "super_admins": ["break-glass-admin"]}"#,
        )
        .unwrap();
        config.apply_recovery_config(&rec_path).expect("apply recovery config");
        let _ = std::fs::remove_file(&rec_path);
        assert_eq!(config.meta_store.seed_nodes, vec!["node1:7650".to_string()]);
        assert_eq!(config.auth.super_admins, vec!["break-glass-admin".to_string()]);
    }

    #[test]
    fn builds_edge_zero_yaml_configuration() {
        let config = ServiceConfiguration::edge_zero_yaml(
            Path::new("/tmp/danube-edge"),
            Some("edge-custom".to_string()),
            "https://cloud.example:6650".to_string(),
            Some("0.0.0.0:1884".to_string()),
            Some("tok".to_string()),
        )
        .expect("edge zero-YAML config");

        assert!(config.load_manager.is_none());
        let edge_cfg = config.edge_config.expect("edge_config");
        assert_eq!(edge_cfg.token_override.as_deref(), Some("tok"));
        assert_eq!(edge_cfg.inline.edge.edge_name, "edge-custom");
        assert_eq!(edge_cfg.inline.edge.cluster_url, "https://cloud.example:6650");
        assert_eq!(
            edge_cfg.inline.mqtt.as_ref().map(|m| m.listener.as_str()),
            Some("0.0.0.0:1884")
        );
    }
}
