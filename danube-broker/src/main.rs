mod admin;
mod args_parse;
mod broker_metrics;
mod broker_server;
mod broker_service;
mod consumer;
mod danube_service;
mod dispatcher;
mod edge_service;
mod message;
mod metadata_storage;
mod policies;
mod producer;
mod rate_limiter;
mod replicator;
mod resources;
mod security;
mod service_configuration;
mod storage_configuration;
mod subscription;
mod topic;
mod topic_cluster;
mod topic_control;
mod topic_registry;
mod utils;

use std::{fs::read_to_string, path::Path, sync::Arc};

use crate::{
    args_parse::{Args, BrokerMode},
    broker_metrics::init_metrics,
    broker_service::BrokerService,
    danube_service::{ClusterServices, DanubeService, LeaderElection, LoadManager},
    resources::{Resources, LEADER_ELECTION_PATH},
    service_configuration::{LoadConfiguration, ServiceConfiguration},
    storage_configuration::{
        LocalRetentionNode, ObjectStoreNode, SharedFsDurableNode, StorageConfig, WalNode,
        WriteBufferNode,
    },
};

use anyhow::{Context, Result};
use danube_edge::edge_service::EdgeService;
use danube_edge::replicator::replicator::EdgeReplicator;
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::valkey::config::{WriteBufferConfig, WaitTimeoutPolicy};
use danube_persistent_storage::{
    ObjectStoreBackend, ObjectStoreConfig, RetentionConfig, StorageFactory, StorageFactoryConfig,
};
use danube_raft::node::{RaftNode, RaftNodeConfig};
use danube_raft::BootstrapResult;
use std::collections::HashMap;
use std::{net::SocketAddr, path::PathBuf};

use crate::metadata_storage::MetadataStorage;

use tracing::info;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    tracing_subscriber::EnvFilter::new(
                        "warn,danube_broker=info,danube_raft=info,danube_persistent_storage=info,danube_client=info",
                    )
                }),
        )
        .init();

    // Parse command line arguments
    let args = Args::parse()?;

    // Build ServiceConfiguration based on mode
    let mut service_config: ServiceConfiguration = match &args.mode {
        BrokerMode::Standalone => ServiceConfiguration::standalone(Path::new(
            args.data_dir
                .as_deref()
                .expect("standalone mode requires data-dir"),
        ))?,
        BrokerMode::Cluster => {
            let config_file = args
                .config_file
                .as_deref()
                .expect("cluster mode requires config file");
            let config_content = read_to_string(Path::new(config_file))?;
            let load_config: LoadConfiguration = serde_yaml::from_str(&config_content)?;
            load_config.try_into()?
        }
        BrokerMode::Edge => {
            let data_dir = args
                .data_dir
                .as_deref()
                .expect("edge mode requires data-dir");
            let edge_config_path = args
                .edge_config
                .clone()
                .expect("edge mode requires edge-config");
            ServiceConfiguration::edge(
                Path::new(data_dir),
                edge_config_path,
                args.edge_token.clone(),
            )?
        }
    };

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
    // (but not in standalone mode where it's already set via ServiceConfiguration::standalone)
    if args.mode != BrokerMode::Standalone {
        if let Some(data_dir) = args.data_dir {
            service_config.meta_store.data_dir = data_dir;
        }
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

    // =========================================================================
    // Initialize the Raft metadata storage layer
    // =========================================================================
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
            let host: &str = rest.split(':').next().unwrap_or("");
            if !host.is_empty() && host != "0.0.0.0" && host != "127.0.0.1" {
                Some(format!("{}:{}", host, service_config.raft_port))
            } else {
                None
            }
        } else {
            None
        }
    };

    // Build Raft TLS config from broker auth config
    let raft_tls = if service_config.auth.is_tls_enabled() {
        let tls = service_config.auth.tls.as_ref().unwrap();
        let cert_pem = std::fs::read(&tls.cert_file)
            .unwrap_or_else(|e| panic!("failed to read TLS cert {}: {}", tls.cert_file, e));
        let key_pem = std::fs::read(&tls.key_file)
            .unwrap_or_else(|e| panic!("failed to read TLS key {}: {}", tls.key_file, e));
        let ca_pem = std::fs::read(&tls.ca_file)
            .unwrap_or_else(|e| panic!("failed to read TLS CA {}: {}", tls.ca_file, e));
        Some(danube_raft::RaftTlsConfig {
            cert_pem,
            key_pem,
            ca_pem,
        })
    } else {
        None
    };

    let raft_node = RaftNode::start(RaftNodeConfig {
        data_dir: (&meta_cfg.data_dir).into(),
        raft_addr,
        advertised_addr: advertised_raft_addr,
        ttl_check_interval: std::time::Duration::from_secs(5),
        tls: raft_tls,
    })
    .await?;

    let node_id = raft_node.node_id;
    info!(node_id, %raft_addr, "Raft metadata store initialized");

    let (was_restart, join_cluster) = if args.join {
        // --join mode (manual override): skip bootstrap, node will be added to
        // an existing cluster via `danube-admin cluster add-node` + `promote-node`
        info!(
            node_id,
            "starting in --join mode (waiting to be added to cluster via admin CLI)"
        );
        (false, true)
    } else {
        // Bootstrap the cluster (config-driven, NATS-style):
        //   - seed_nodes empty  → single-node auto-init (zero config)
        //   - seed_nodes present → discovers peers, lowest node_id initializes
        //   - seed_nodes present + peers have leader → auto-join (scale-up)
        let result = raft_node
            .bootstrap_cluster(
                &raft_node.advertised_addr,
                &service_config.meta_store.seed_nodes,
            )
            .await?;
        match result {
            BootstrapResult::Restart => (true, false),
            BootstrapResult::Initialized => (false, false),
            BootstrapResult::JoinExisting => (false, true),
        }
    };

    let raft_handle = raft_node.raft.clone();
    let leadership_handle = raft_node.leadership_handle();
    let advertised_raft_addr_str = raft_node.advertised_addr.clone();
    let metadata_store = MetadataStorage::Raft(std::sync::Arc::new(raft_node.store));

    let store_arc: Arc<dyn danube_core::metadata::MetadataStore> = Arc::new(metadata_store.clone());
    let storage_factory_config =
        build_storage_factory_config(&service_config.storage, &service_config.meta_store.data_dir);
    let storage_factory = StorageFactory::new(storage_factory_config, store_arc);

    // convenient functions to handle the metadata and configurations required
    // for managing the cluster, namespaces & topics
    let resources = Resources::new(
        metadata_store.clone(),
        match args.mode {
            BrokerMode::Cluster => Some(leadership_handle.clone()),
            _ => None,
        },
        service_config.auth.super_admins.clone(),
    );

    // The broker_id IS the Raft node_id — a single stable identity.
    let broker_id = node_id;

    // Edge mode: create the EdgeService (owns replicator + MQTT gateway)
    let edge_service = if args.mode == BrokerMode::Edge {
        let edge_config = service_config
            .edge_config
            .as_ref()
            .expect("edge_config required in edge mode");

        let edge_cfg = danube_edge::config::EdgeConfig::from_file(&edge_config.config_path)
            .context("failed to load edge config")?;

        let edge_svc = EdgeService::new(
            edge_cfg,
            storage_factory.clone(),
            Arc::new(metadata_store.clone()),
            edge_config.token_override.clone(),
        )
        .await
        .context("failed to create EdgeService")?;

        Some(Arc::new(edge_svc))
    } else {
        None
    };

    // Extract edge replicator for BrokerService (topic create/delete coordination)
    let edge_replicator: Option<Arc<EdgeReplicator>> =
        edge_service.as_ref().map(|es| es.replicator().clone());

    // the broker service, is responsible to reliable deliver the messages from producers to consumers.
    let broker_service = BrokerService::new(
        broker_id,
        args.mode.clone(),
        resources.clone(),
        storage_factory,
        service_config.auto_create_topics,
        edge_replicator,
    );

    // Init metrics with or without prometheus exporter
    if let Some(prometheus_exporter) = service_config.prom_exporter.clone() {
        init_metrics(Some(prometheus_exporter), broker_id);
    } else {
        init_metrics(None, broker_id);
    }

    let broker: Arc<BrokerService> = Arc::new(broker_service);

    let broker_addr = service_config.broker_addr;
    info!(
        broker_addr = %broker_addr,
        broker_id = %broker_id,
        mode = ?args.mode,
        "initializing Danube message broker service"
    );

    // =========================================================================
    // Mode dispatch: build ClusterServices only in cluster mode
    // =========================================================================

    let cluster_services = match args.mode {
        BrokerMode::Cluster => {
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

            Some(ClusterServices {
                leader_election: leader_election_service,
                load_manager,
                raft: raft_handle,
                leadership: leadership_handle,
                raft_addr: advertised_raft_addr_str,
                join_cluster,
                was_restart,
            })
        }
        BrokerMode::Standalone => None,
        BrokerMode::Edge => None,
    };

    // DanubeService coordinate and start all the services
    let mut danube = DanubeService::new(
        broker_id,
        args.mode.clone(),
        Arc::clone(&broker),
        service_config,
        metadata_store,
        resources,
        edge_service,
    );

    danube
        .start(cluster_services)
        .await
        .expect("Danube Message Broker service unable to start");

    info!(
        broker_addr = %broker_addr,
        broker_id = %broker_id,
        "Danube message broker service started successfully"
    );

    Ok(())
}

fn build_storage_factory_config(
    storage: &StorageConfig,
    metadata_data_dir: &str,
) -> StorageFactoryConfig {
    let base = match storage {
        StorageConfig::Local {
            local_wal_root,
            metadata_prefix,
            local_retention,
            wal,
            ..
        } => StorageFactoryConfig::local(
            build_wal_config(Some(local_wal_root.clone()), wal),
            metadata_prefix
                .clone()
                .unwrap_or_else(|| "/danube".to_string()),
            build_local_retention_config(local_retention.as_ref(), wal),
        ),
        StorageConfig::SharedFs {
            local_wal_root,
            metadata_prefix,
            durable,
            legacy_root,
            local_retention,
            wal,
            ..
        } => StorageFactoryConfig::shared_fs(
            build_wal_config(
                Some(resolve_local_wal_root(
                    local_wal_root.as_ref(),
                    metadata_data_dir,
                    "shared-fs-cache",
                )),
                wal,
            ),
            metadata_prefix
                .clone()
                .unwrap_or_else(|| "/danube".to_string()),
            resolve_shared_fs_durable_root(durable.as_ref(), legacy_root.as_ref()),
            build_local_retention_config(local_retention.as_ref(), wal),
        ),
        StorageConfig::ObjectStore {
            local_wal_root,
            metadata_prefix,
            durable,
            legacy_object_store,
            local_retention,
            wal,
            ..
        } => StorageFactoryConfig::object_store(
            build_wal_config(
                Some(resolve_local_wal_root(
                    local_wal_root.as_ref(),
                    metadata_data_dir,
                    "object-store-cache",
                )),
                wal,
            ),
            metadata_prefix
                .clone()
                .unwrap_or_else(|| "/danube".to_string()),
            resolve_object_store_config(durable.as_ref(), legacy_object_store.as_ref()),
            build_local_retention_config(local_retention.as_ref(), wal),
        ),
    };

    // Apply write_buffer config if present
    let write_buffer_node = match storage {
        StorageConfig::Local { write_buffer, .. } => write_buffer.as_ref(),
        StorageConfig::SharedFs { write_buffer, .. } => write_buffer.as_ref(),
        StorageConfig::ObjectStore { write_buffer, .. } => write_buffer.as_ref(),
    };

    if let Some(wb) = write_buffer_node {
        base.with_write_buffer(build_write_buffer_config(wb))
    } else {
        base
    }
}

fn build_wal_config(root: Option<String>, wal: &WalNode) -> WalConfig {
    WalConfig {
        dir: root.or_else(|| wal.dir.clone()).map(Into::into),
        file_name: wal.file_name.clone(),
        cache_capacity: wal.cache_capacity(),
        flush_interval_ms: wal.file_sync().and_then(|f| f.interval_ms),
        flush_max_batch_bytes: wal.file_sync().and_then(|f| f.max_batch_bytes),
        rotate_max_bytes: wal.rotate_max_bytes(),
        rotate_max_seconds: wal.rotate_max_hours().map(|h| h.saturating_mul(3600)),
        ..Default::default()
    }
}

fn build_local_retention_config(
    local_retention: Option<&LocalRetentionNode>,
    wal: &WalNode,
) -> Option<RetentionConfig> {
    local_retention
        .or_else(|| wal.legacy_local_retention())
        .map(|ret| RetentionConfig {
            check_interval_minutes: ret.check_interval_minutes.unwrap_or(5),
            time_minutes: ret.time_minutes,
            size_mb: ret.size_mb,
        })
}

fn resolve_local_wal_root(
    local_wal_root: Option<&String>,
    metadata_data_dir: &str,
    suffix: &str,
) -> String {
    if let Some(local_wal_root) = local_wal_root {
        return local_wal_root.clone();
    }
    let mut base = PathBuf::from(metadata_data_dir);
    if base.file_name().is_some() {
        base.pop();
    }
    base.push(suffix);
    base.to_string_lossy().into_owned()
}

fn resolve_shared_fs_durable_root(
    durable: Option<&SharedFsDurableNode>,
    legacy_root: Option<&String>,
) -> String {
    durable
        .map(|cfg| cfg.root.clone())
        .or_else(|| legacy_root.cloned())
        .expect("shared_fs storage requires durable.root or legacy root")
}

fn resolve_object_store_config(
    durable: Option<&ObjectStoreNode>,
    legacy_object_store: Option<&ObjectStoreNode>,
) -> ObjectStoreConfig {
    object_store_node_to_config(
        durable
            .or(legacy_object_store)
            .expect("object_store storage requires durable backend or legacy object_store"),
    )
}

fn object_store_node_to_config(cfg: &ObjectStoreNode) -> ObjectStoreConfig {
    match cfg {
        ObjectStoreNode::S3 {
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
            let mut options = HashMap::new();
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
            ObjectStoreConfig::new(ObjectStoreBackend::S3, root.clone()).with_options(options)
        }
        ObjectStoreNode::Gcs {
            root,
            project,
            credentials_json,
            credentials_path,
        } => {
            let mut options = HashMap::new();
            if let Some(v) = project {
                options.insert("project".into(), v.clone());
            }
            if let Some(v) = credentials_json {
                options.insert("credentials_json".into(), v.clone());
            }
            if let Some(v) = credentials_path {
                options.insert("credentials_path".into(), v.clone());
            }
            ObjectStoreConfig::new(ObjectStoreBackend::Gcs, root.clone()).with_options(options)
        }
        ObjectStoreNode::Azblob {
            root,
            endpoint,
            account_name,
            account_key,
        } => {
            let mut options = HashMap::new();
            if let Some(v) = endpoint {
                options.insert("endpoint".into(), v.clone());
            }
            if let Some(v) = account_name {
                options.insert("account_name".into(), v.clone());
            }
            if let Some(v) = account_key {
                options.insert("account_key".into(), v.clone());
            }
            ObjectStoreConfig::new(ObjectStoreBackend::Azblob, root.clone()).with_options(options)
        }
    }
}

fn build_write_buffer_config(node: &WriteBufferNode) -> WriteBufferConfig {
    WriteBufferConfig {
        endpoints: node.endpoints.clone(),
        wait_replicas: node.wait_replicas.unwrap_or(1),
        wait_timeout_ms: node.wait_timeout_ms.unwrap_or(100),
        on_wait_timeout: node
            .on_wait_timeout
            .as_deref()
            .map(WaitTimeoutPolicy::from_str_lossy)
            .unwrap_or_default(),
        max_cached_closed_segments: node.max_cached_closed_segments.unwrap_or(5),
    }
}
