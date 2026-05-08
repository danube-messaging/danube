use crate::metadata_storage::MetadataStorage;
mod broker_register;
mod broker_watcher;
mod leader_election;
pub(crate) mod load_manager;
pub(crate) mod load_report;
pub(crate) mod metrics_collector;
mod resource_monitor;
mod syncronizer;

pub(crate) use broker_register::register_broker;
pub(crate) use leader_election::LeaderElection;
pub(crate) use load_manager::LoadManager;
pub(crate) use load_report::LoadReport;

use crate::args_parse::BrokerMode;

use anyhow::{Context, Result};
use danube_client::DanubeClient;
use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_edge::edge::replicator::EdgeReplicator;
use danube_raft::leadership::LeadershipHandle;
use danube_raft::Raft;
use std::sync::Arc;

use tokio::time::{self, Duration};
use tracing::{debug, info, warn};

use crate::edge_service::{EdgeReplicationStorage, EdgeReplicatorServiceImpl};

use crate::{
    admin::{ClusterAdmin, DanubeAdminImpl},
    broker_server::{self, SchemaRegistryService},
    broker_service::BrokerService,
    policies::Policies,
    resources::{Resources, BASE_BROKER_LOAD_PATH, DEFAULT_NAMESPACE, SYSTEM_NAMESPACE},
    security::config::AuthMode,
    service_configuration::ServiceConfiguration,
    topic::SYSTEM_TOPIC,
    utils::join_path,
};

// =============================================================================
// ClusterServices — injected by main.rs only in cluster mode
// =============================================================================

/// Cluster-only services that are constructed in `main.rs` and passed to
/// `DanubeService::start()` only when running in cluster mode.
pub(crate) struct ClusterServices {
    pub leader_election: LeaderElection,
    pub load_manager: LoadManager,
    pub raft: Raft<danube_raft::typ::TypeConfig>,
    pub leadership: LeadershipHandle,
    pub raft_addr: String,
    /// When true, the broker was started with --join and should wait
    /// for cluster membership before registering. Joins as Drained.
    pub join_cluster: bool,
    /// True if bootstrap_cluster found persisted Raft state (restart, not first boot).
    pub was_restart: bool,
}

// =============================================================================
// DanubeService — top-level coordinator
// =============================================================================

/// Top-level coordinator for the Danube message broker.
///
/// Manages cluster/standalone lifecycle including:
/// - Broker registration and state management
/// - Namespace and metadata initialization
/// - gRPC server startup (admin + broker)
/// - Cluster orchestration (leader election, load manager, rebalancing)
///   only in cluster mode
pub(crate) struct DanubeService {
    broker_id: u64,
    mode: BrokerMode,
    broker: Arc<BrokerService>,
    service_config: ServiceConfiguration,
    meta_store: MetadataStorage,
    resources: Resources,
    edge_replicator: Option<Arc<EdgeReplicator>>,
}

impl std::fmt::Debug for DanubeService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DanubeService")
            .field("broker_id", &self.broker_id)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl DanubeService {
    pub(crate) fn new(
        broker_id: u64,
        mode: BrokerMode,
        broker: Arc<BrokerService>,
        service_config: ServiceConfiguration,
        meta_store: MetadataStorage,
        resources: Resources,
        edge_replicator: Option<Arc<danube_edge::edge::replicator::EdgeReplicator>>,
    ) -> Self {
        DanubeService {
            broker_id,
            mode,
            broker,
            service_config,
            meta_store,
            resources,
            edge_replicator,
        }
    }

    // =========================================================================
    // Public entry point
    // =========================================================================

    pub(crate) async fn start(&mut self, cluster: Option<ClusterServices>) -> Result<()> {
        info!(
            cluster = %self.service_config.cluster_name,
            broker_id = %self.broker_id,
            mode = ?self.mode,
            "initializing Danube broker"
        );

        // ----- Common: admin gRPC (must be up before --join so add-node can reach us)
        let schema_registry = Arc::new(SchemaRegistryService::new(
            self.meta_store.clone(),
            self.broker.topic_manager.clone(),
            self.resources.security.clone(),
        ));

        let cluster_admin = cluster.as_ref().map(|c| ClusterAdmin {
            load_manager: c.load_manager.clone(),
            raft: c.raft.clone(),
            leadership: c.leadership.clone(),
            raft_addr: c.raft_addr.clone(),
        });

        let admin_handle = DanubeAdminImpl::new(
            self.service_config.admin_addr.clone(),
            Arc::clone(&self.broker),
            self.resources.clone(),
            self.service_config.auth.clone(),
            self.service_config.admin_tls,
            schema_registry.clone(),
            cluster_admin,
        )
        .start()
        .await;

        info!(
            admin_addr = %self.service_config.admin_addr,
            broker_id = %self.broker_id,
            "admin gRPC server listening"
        );

        // ----- Build edge replicator gRPC service (only for Cluster/Standalone).
        // Edge brokers are the *client* side — they don't host this service.
        // Auth is handled by the standard gRPC interceptor.
        let edge_service = if self.mode != BrokerMode::Edge {
            let storage = Arc::new(EdgeReplicationStorage::new(
                self.broker.topic_manager.storage_factory.clone(),
                self.meta_store.clone(),
            ));
            let topic_cluster =
                crate::topic_cluster::TopicCluster::new(self.broker.resources.clone());
            Some(EdgeReplicatorServiceImpl::new(storage, topic_cluster))
        } else {
            None
        };

        // ----- Mode-specific startup
        match self.mode {
            BrokerMode::Cluster => {
                self.start_cluster_mode(
                    cluster.expect("ClusterServices required for cluster mode"),
                    schema_registry,
                    edge_service,
                )
                .await?;
            }
            BrokerMode::Standalone => {
                self.start_standalone_mode(schema_registry, edge_service)
                    .await?;
            }
            BrokerMode::Edge => {
                self.start_edge_mode(
                    schema_registry,
                    edge_service,
                    self.edge_replicator
                        .clone()
                        .expect("edge_replicator must be set for Edge mode"),
                )
                .await?;
            }
        }

        // ----- Wait for admin server to complete
        if let Err(e) = admin_handle.await {
            eprintln!("Danube Admin failed: {:?}", e);
        }

        Ok(())
    }

    // =========================================================================
    // Cluster mode
    // =========================================================================

    /// Cluster startup sequence (order matters):
    ///
    /// 1. Wait for Raft membership (--join only)
    /// 2. Create cluster record
    /// 3. Register broker
    /// 4. Determine initial state (active/drained)
    /// 5. Reconcile pre-existing topic assignments
    /// 6. Initialize namespaces + system topic
    /// 7. Load authorization cache
    /// 8. Start broker gRPC + replicator client
    /// 9. Start cluster orchestration (leader election, load manager, etc.)
    async fn start_cluster_mode(
        &mut self,
        mut cluster: ClusterServices,
        schema_registry: Arc<SchemaRegistryService>,
        edge_service: Option<EdgeReplicatorServiceImpl>,
    ) -> Result<()> {
        // 1. Wait for Raft membership (--join only)
        if cluster.join_cluster {
            self.wait_for_cluster_membership(&cluster.raft).await;
        }

        // 2. Create cluster record
        let _ = self
            .resources
            .cluster
            .create_cluster(&self.service_config.cluster_name)
            .await;

        // 3. Register broker
        self.register_broker(32).await?; // TTL = 32s for cluster

        // 4. Determine and set initial broker state
        self.determine_initial_state(&cluster).await;

        // 5. Reconcile pre-existing topic assignments
        self.reconcile_topic_assignments().await;

        // 6–7. Namespaces + auth cache
        self.initialize_metadata_and_auth().await?;

        // 8. Broker gRPC + replicator
        let server_handle = self
            .start_broker_grpc(schema_registry, edge_service)
            .await?;

        // 9. Start cluster orchestration services
        self.start_orchestration_services(&mut cluster).await?;

        // Block until broker gRPC shuts down
        if let Err(e) = server_handle.await {
            eprintln!("Broker Server failed: {:?}", e);
        }

        Ok(())
    }

    // =========================================================================
    // Standalone mode
    // =========================================================================

    /// Standalone startup sequence (order matters):
    ///
    /// 1. Register broker (no TTL)
    /// 2. Set state to active
    /// 3. Initialize namespaces + system topic
    /// 4. Load authorization cache
    /// 5. Start broker gRPC + replicator client
    async fn start_standalone_mode(
        &mut self,
        schema_registry: Arc<SchemaRegistryService>,
        edge_service: Option<EdgeReplicatorServiceImpl>,
    ) -> Result<()> {
        // 1–2. Register + immediate active state
        self.register_broker(0).await?; // TTL = 0 for standalone (no renewal)

        if let Err(e) = self
            .resources
            .cluster
            .set_broker_state(&self.broker_id.to_string(), "active", Some("standalone"))
            .await
        {
            warn!(error = %e, "failed to set standalone broker state");
        }

        // Create cluster record (single-node Raft auto-commits)
        let _ = self
            .resources
            .cluster
            .create_cluster(&self.service_config.cluster_name)
            .await;

        // 3–4. Namespaces + auth cache
        self.initialize_metadata_and_auth().await?;

        // 5. Broker gRPC + replicator
        let server_handle = self
            .start_broker_grpc(schema_registry, edge_service)
            .await?;

        info!(
            broker_id = %self.broker_id,
            "standalone mode: broker active (no leader election, no load manager, no broker watcher)"
        );

        // Block until broker gRPC shuts down
        if let Err(e) = server_handle.await {
            eprintln!("Broker Server failed: {:?}", e);
        }

        Ok(())
    }

    // =========================================================================
    // Edge mode
    // =========================================================================

    /// Edge broker startup sequence:
    ///
    /// 1. Register broker (single-node Raft, no TTL renewal)
    /// 2. Set broker state to active
    /// 3. Create cluster record
    /// 4. Initialize default namespaces + auth cache
    /// 5. Start broker gRPC (local producers/consumers still work)
    /// 6. Reconcile pre-existing topics and start WAL replication
    async fn start_edge_mode(
        &mut self,
        schema_registry: Arc<SchemaRegistryService>,
        edge_service: Option<EdgeReplicatorServiceImpl>,
        replicator: Arc<danube_edge::edge::replicator::EdgeReplicator>,
    ) -> Result<()> {
        // 1–2. Register + immediate active state (same as standalone)
        self.register_broker(0).await?;

        if let Err(e) = self
            .resources
            .cluster
            .set_broker_state(&self.broker_id.to_string(), "active", Some("edge"))
            .await
        {
            warn!(error = %e, "failed to set edge broker state");
        }

        // Create cluster record (single-node Raft auto-commits)
        let _ = self
            .resources
            .cluster
            .create_cluster(&self.service_config.cluster_name)
            .await;

        // 3–4. Namespaces + auth cache
        self.initialize_metadata_and_auth().await?;

        // 5. Broker gRPC (local producers/consumers for edge MQTT/client usage)
        // Edge replicator was already created and installed in start() before
        // the Arc was shared.
        let server_handle = self
            .start_broker_grpc(schema_registry, edge_service)
            .await?;

        // 6. Reconcile: any topics from a previous run that already exist locally
        // need to be registered on the cloud and start replicating again.
        let topics = self.broker.topic_registry.get_all_topics();
        for topic_name in &topics {
            if let Err(e) = replicator.cloud_client().create_topic(topic_name).await {
                warn!(
                    topic = %topic_name,
                    error = %e,
                    "failed to create pre-existing topic on cloud, will retry later"
                );
                continue;
            }
            replicator.add_topic(topic_name).await;
        }

        let edge_config = self
            .service_config
            .edge_config
            .as_ref()
            .expect("edge_config required in edge mode");

        let edge_name = &edge_config.edge_name;

        info!(
            broker_id = %self.broker_id,
            edge_name = %edge_name,
            active_topics = topics.len(),
            "edge mode: broker active, replicating to cloud"
        );

        // 7. MQTT gateway (if --mqtt-config was provided)
        if let Some(ref mqtt_config_path) = edge_config.mqtt_config_path.clone() {
            info!(
                config = %mqtt_config_path,
                "loading MQTT gateway configuration"
            );

            match danube_edge::mqtt::config::MqttConfig::from_file(mqtt_config_path) {
                Ok(mqtt_config) => {
                    let storage_factory = self.broker.topic_manager.storage_factory.clone();

                    let ingester_config = danube_edge::mqtt::ingester::MqttIngesterConfig {
                        batch_size: mqtt_config.mqtt.ingestion.batch_size,
                        batch_timeout: mqtt_config.mqtt.ingestion.batch_timeout(),
                    };

                    let ingester = std::sync::Arc::new(
                        danube_edge::mqtt::ingester::MqttIngester::new(
                            storage_factory,
                            ingester_config,
                        ),
                    );

                    // Pre-provision Danube topics from config mapping
                    let danube_topics = mqtt_config.danube_topics();
                    for topic_name in &danube_topics {
                        // Ensure the topic exists in the broker's topic registry
                        if let Err(e) = self.broker.topic_manager.ensure_local(topic_name).await {
                            warn!(
                                topic = %topic_name,
                                error = %e,
                                "failed to ensure local topic for MQTT mapping"
                            );
                        }

                        // Provision WAL handle in the ingester
                        if let Err(e) = ingester.provision_topic(topic_name).await {
                            warn!(
                                topic = %topic_name,
                                error = %e,
                                "failed to provision MQTT ingester topic"
                            );
                            continue;
                        }

                        // Register with EdgeReplicator for cloud replication
                        if let Err(e) = replicator.cloud_client().create_topic(topic_name).await {
                            warn!(
                                topic = %topic_name,
                                error = %e,
                                "failed to create MQTT topic on cloud, will retry later"
                            );
                        }
                        replicator.add_topic(topic_name).await;
                    }

                    info!(
                        topics = danube_topics.len(),
                        "MQTT ingester: topics provisioned"
                    );

                    // Build topic router
                    let router = std::sync::Arc::new(
                        danube_edge::mqtt::bridge::TopicRouter::new(
                            &mqtt_config.mqtt.topic_mappings,
                        ),
                    );

                    // Spawn background flush loop
                    let ingester_flush = ingester.clone();
                    tokio::spawn(async move {
                        ingester_flush.run_flush_loop().await;
                    });

                    // Spawn MQTT TCP listener
                    let listener_addr = mqtt_config.mqtt.listener.clone();
                    tokio::spawn(async move {
                        danube_edge::mqtt::server::start_listener(
                            &listener_addr,
                            router,
                            ingester,
                        )
                        .await;
                    });
                }
                Err(e) => {
                    warn!(
                        config = %mqtt_config_path,
                        error = %e,
                        "failed to load MQTT config, MQTT gateway disabled"
                    );
                }
            }
        }

        // Block until broker gRPC shuts down
        if let Err(e) = server_handle.await {
            eprintln!("Broker Server failed: {:?}", e);
        }

        Ok(())
    }

    // =========================================================================
    // Shared helpers
    // =========================================================================

    /// Registers this broker in the metadata store.
    async fn register_broker(&self, ttl: i64) -> Result<()> {
        let is_secure = self.service_config.auth.mode == AuthMode::Tls;
        let admin_addr = self.service_config.admin_addr.to_string();
        let metrics_addr = self
            .service_config
            .prom_exporter
            .as_ref()
            .map(|a| a.to_string());

        register_broker(
            self.meta_store.clone(),
            &self.broker_id.to_string(),
            &self.service_config.broker_url,
            &self.service_config.connect_url,
            &admin_addr,
            metrics_addr.as_deref(),
            ttl,
            is_secure,
        )
        .await
    }

    /// Creates default namespaces, system topic, bootstrap namespaces,
    /// and initializes the authorization cache.
    async fn initialize_metadata_and_auth(&mut self) -> Result<()> {
        // Namespaces
        create_namespace_if_absent(
            &mut self.resources,
            DEFAULT_NAMESPACE,
            &self.service_config.policies,
        )
        .await?;

        create_namespace_if_absent(
            &mut self.resources,
            SYSTEM_NAMESPACE,
            &self.service_config.policies,
        )
        .await?;

        // System topic
        if !self.resources.topic.topic_exists(SYSTEM_TOPIC).await? {
            self.resources.topic.create_topic(SYSTEM_TOPIC, 0).await?;
        }

        // Bootstrap namespaces from config
        for namespace in &self.service_config.bootstrap_namespaces {
            create_namespace_if_absent(
                &mut self.resources,
                namespace,
                &self.service_config.policies,
            )
            .await?;
        }

        info!(
            broker_id = %self.broker_id,
            cluster = %self.service_config.cluster_name,
            "metadata initialization completed"
        );

        // Authorization cache
        self.resources
            .security
            .load_cache()
            .await
            .expect("failed to load authorization cache");
        self.resources.security.start_watcher().await;

        Ok(())
    }

    /// Starts the broker gRPC server and initializes the replicator's internal
    /// DanubeClient. Returns the server JoinHandle.
    async fn start_broker_grpc(
        &self,
        schema_registry: Arc<SchemaRegistryService>,
        edge_service: Option<EdgeReplicatorServiceImpl>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let grpc_server = broker_server::DanubeServerImpl::new(
            self.broker.clone(),
            schema_registry,
            self.service_config.broker_addr.clone(),
            self.service_config.broker_url.clone(),
            self.service_config.connect_url.clone(),
            self.service_config.proxy_enabled,
            self.service_config.auth.clone(),
            edge_service,
        );

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let server_handle = grpc_server.start(ready_tx).await;

        info!(
            broker_addr = %self.service_config.broker_addr,
            broker_id = %self.broker_id,
            "broker gRPC server listening"
        );

        // Wait for the server to bind, then create the replicator's internal client
        ready_rx.await?;

        let broker_client_url = self.service_config.broker_url.clone();
        let danube_client = loop {
            let mut builder = DanubeClient::builder()
                .service_url(&broker_client_url)
                .with_internal_broker(format!("broker/{}", self.broker_id));

            if let Some(ref tls_config) = self.service_config.auth.tls {
                builder = match builder.with_mtls(
                    &tls_config.ca_file,
                    &tls_config.cert_file,
                    &tls_config.key_file,
                ) {
                    Ok(b) => b,
                    Err(err) => {
                        return Err(anyhow::anyhow!(err)).context(format!(
                            "Failed to configure mTLS for internal broker client (ca_file={})",
                            tls_config.ca_file
                        ));
                    }
                };
            }

            match builder.build().await {
                Ok(client) => break client,
                Err(err) => {
                    if broker_client_url.starts_with("https://") {
                        return Err(err).context(format!(
                            "Failed to connect internal broker client to {}",
                            broker_client_url
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        };

        self.broker.replicator.set_client(danube_client).await;
        info!("replicator DanubeClient initialized");

        Ok(server_handle)
    }

    // =========================================================================
    // Cluster-only helpers
    // =========================================================================

    /// Blocks until this node detects a Raft leader (meaning it has been added
    /// to the cluster via `danube-admin cluster add-node`).
    async fn wait_for_cluster_membership(&self, raft: &Raft<danube_raft::typ::TypeConfig>) {
        info!(
            broker_id = %self.broker_id,
            "waiting for cluster membership (use `danube-admin cluster add-node`)..."
        );
        loop {
            let metrics = raft.metrics().borrow().clone();
            if metrics.current_leader.is_some() {
                info!(
                    broker_id = %self.broker_id,
                    leader = ?metrics.current_leader,
                    "cluster membership detected — node has joined the cluster"
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Determines the initial broker state based on how it was started:
    ///
    /// - `--join` mode       → **drained** (wait for admin to activate)
    /// - restart < TTL       → **active** (fast restart, still registered)
    /// - full cluster restart → **active** (all registrations expired, safe)
    /// - restart > TTL       → **drained** (registration expired, admin must investigate)
    /// - first boot          → **active**
    async fn determine_initial_state(&self, cluster: &ClusterServices) {
        let (initial_state, state_reason) = if cluster.join_cluster {
            ("drained", "join")
        } else if cluster.was_restart {
            let my_reg_path = join_path(&[
                crate::resources::BASE_REGISTER_PATH,
                &self.broker_id.to_string(),
            ]);
            let my_reg_exists = self
                .meta_store
                .get(&my_reg_path, MetaOptions::None)
                .await
                .ok()
                .flatten()
                .is_some();

            if my_reg_exists {
                info!(
                    broker_id = %self.broker_id,
                    "restart detected: registration still valid (within TTL)"
                );
                ("active", "fast_restart")
            } else {
                let all_regs = self
                    .meta_store
                    .get_childrens(crate::resources::BASE_REGISTER_PATH)
                    .await
                    .unwrap_or_default();

                if all_regs.is_empty() {
                    info!(
                        broker_id = %self.broker_id,
                        "full cluster restart detected (no brokers registered). Registering as active."
                    );
                    ("active", "full_cluster_restart")
                } else {
                    warn!(
                        broker_id = %self.broker_id,
                        registered_brokers = all_regs.len(),
                        "broker registration expired (unavailable > TTL). \
                         Registering as drained. Use `danube-admin brokers activate` to resume."
                    );
                    ("drained", "stale_restart")
                }
            }
        } else {
            ("active", "boot")
        };

        if let Err(e) = self
            .resources
            .cluster
            .set_broker_state(
                &self.broker_id.to_string(),
                initial_state,
                Some(state_reason),
            )
            .await
        {
            warn!(
                state = initial_state,
                reason = state_reason,
                error = %e,
                "failed to set initial broker state"
            );
        }
        if initial_state == "drained" {
            info!(
                broker_id = %self.broker_id,
                reason = state_reason,
                "broker registered as drained (use `danube-admin brokers activate` to enable topic assignment)"
            );
        }
    }

    /// On restart, the in-memory TopicManager is empty but metadata may still
    /// have topics assigned to this broker. The broker_watcher only fires on
    /// *new* events, so pre-existing assignments are invisible. Scan and
    /// recreate them now.
    async fn reconcile_topic_assignments(&self) {
        let broker_path = join_path(&[
            crate::resources::BASE_BROKER_PATH,
            &self.broker_id.to_string(),
        ]);

        let children = match self.meta_store.get_childrens(&broker_path).await {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    broker_id = %self.broker_id,
                    error = %e,
                    "failed to scan broker assignments for startup reconciliation"
                );
                return;
            }
        };

        let mut reconciled = 0u32;
        for full_path in &children {
            let parts: Vec<&str> = full_path.split('/').collect();
            // Expected: /cluster/brokers/{id}/{ns}/{topic}
            if parts.len() < 6 || parts[4] == "state" {
                continue;
            }
            let topic_name = format!("/{}/{}", parts[4], parts[5]);
            match self.broker.topic_manager.ensure_local(&topic_name).await {
                Ok(_) => {
                    reconciled += 1;
                    info!(topic = %topic_name, "reconciled topic on startup");
                }
                Err(e) => {
                    warn!(
                        topic = %topic_name,
                        error = %e,
                        "failed to reconcile topic on startup"
                    );
                }
            }
        }

        if reconciled > 0 {
            info!(
                broker_id = %self.broker_id,
                count = reconciled,
                "startup topic reconciliation complete"
            );
        }
    }

    /// Starts the cluster orchestration background tasks:
    /// leader election, load manager, rebalancing, load reports, broker watcher.
    async fn start_orchestration_services(&self, cluster: &mut ClusterServices) -> Result<()> {
        // Leader election
        let leader_check_interval = time::interval(Duration::from_secs(10));
        let mut leader_election_cloned = cluster.leader_election.clone();
        tokio::spawn(async move {
            leader_election_cloned.start(leader_check_interval).await;
        });
        info!("leader election service started");

        // Load manager
        let rx_event = cluster.load_manager.bootstrap(self.broker_id).await?;
        let mut load_manager_cloned = cluster.load_manager.clone();
        let broker_id = self.broker_id;
        let leader_election_cloned = cluster.leader_election.clone();
        tokio::spawn(async move {
            load_manager_cloned
                .start(rx_event, broker_id, leader_election_cloned)
                .await
        });
        info!("load manager service started");

        // Automated rebalancing
        if let Some(ref lm_config) = self.service_config.load_manager {
            if lm_config.rebalancing.enabled {
                info!(
                    check_interval_seconds = lm_config.rebalancing.check_interval_seconds,
                    aggressiveness = ?lm_config.rebalancing.aggressiveness,
                    max_moves_per_hour = lm_config.rebalancing.max_moves_per_hour,
                    "starting automated rebalancing loop"
                );
                let load_manager = cluster.load_manager.clone();
                let rebalancing_config = lm_config.rebalancing.clone();
                let leader_election = cluster.leader_election.clone();
                tokio::spawn(async move {
                    let _handle =
                        load_manager.start_rebalancing_loop(rebalancing_config, leader_election);
                });
            } else {
                info!("automated rebalancing is disabled");
            }
        } else {
            debug!("no load manager configuration, rebalancing disabled");
        }

        // Periodic load reports
        let load_report_interval = self
            .service_config
            .load_manager
            .as_ref()
            .map(|c| c.load_report_interval_seconds)
            .unwrap_or(30);

        let broker_service = Arc::clone(&self.broker);
        let meta_store = self.meta_store.clone();
        tokio::spawn(async move {
            post_broker_load_report(broker_service, meta_store, load_report_interval).await
        });

        // Broker watcher (watches for topic assignment/unload events)
        let broker_service = Arc::clone(&self.broker);
        let meta_store = self.meta_store.clone();
        broker_watcher::watch_events_for_broker(meta_store, broker_service, self.broker_id).await;

        Ok(())
    }
}

// =============================================================================
// Free functions
// =============================================================================

async fn create_namespace_if_absent(
    resources: &mut Resources,
    namespace_name: &str,
    policies: &Policies,
) -> Result<()> {
    if !resources.namespace.namespace_exist(namespace_name).await? {
        resources
            .namespace
            .create_namespace(namespace_name, Some(policies))
            .await?;
    } else {
        info!(namespace = %namespace_name, "namespace already exists");
    }
    Ok(())
}

/// Periodically publishes broker load reports to the metadata store.
///
/// The LoadManager uses these reports for load balancing decisions across
/// the cluster. Published at `/cluster/load/{broker_id}`.
async fn post_broker_load_report(
    broker_service: Arc<BrokerService>,
    meta_store: MetadataStorage,
    interval_seconds: u64,
) {
    let mut interval = time::interval(Duration::from_secs(interval_seconds));
    loop {
        interval.tick().await;
        let topics = broker_service.get_topics();
        let broker_id = broker_service.broker_id;
        let load_report: LoadReport = load_report::generate_load_report(
            broker_id,
            topics,
            broker_service.metrics_collector(),
        )
        .await;
        if let Ok(value) = serde_json::to_value(&load_report) {
            let path = join_path(&[BASE_BROKER_LOAD_PATH, &broker_id.to_string()]);
            match meta_store.put(&path, value, MetaOptions::None).await {
                Ok(_) => debug!(
                    broker_id = %broker_id,
                    "broker posted a new load report: {:?}",
                    &load_report
                ),
                Err(err) => debug!(
                    broker_id = %broker_id,
                    error = %err,
                    "unable to post load report"
                ),
            }
        }
    }
}
