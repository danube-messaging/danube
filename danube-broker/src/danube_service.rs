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

use crate::args_parse::{BrokerMode, NodeRole};

use anyhow::{Context, Result};
use danube_client::DanubeClient;
use danube_core::metadata::{MetaOptions, MetadataStore, WatchEvent};
use danube_edge::edge_service::EdgeService;
use danube_raft::leadership::LeadershipHandle;
use danube_raft::Raft;
use futures::StreamExt;
use std::collections::HashMap;
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
    edge_service: Option<Arc<EdgeService>>,
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
        edge_service: Option<Arc<EdgeService>>,
    ) -> Self {
        DanubeService {
            broker_id,
            mode,
            broker,
            service_config,
            meta_store,
            resources,
            edge_service,
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

        let admin_impl = DanubeAdminImpl::new(
            self.service_config.admin_addr.clone(),
            Arc::clone(&self.broker),
            self.resources.clone(),
            self.service_config.auth.clone(),
            self.service_config.admin_tls,
            schema_registry.clone(),
            cluster_admin,
        );

        let same_port_multiplex = self.service_config.admin_addr == self.service_config.broker_addr
            || (self.service_config.admin_addr.port() == self.service_config.broker_addr.port()
                && (self.service_config.broker_addr.ip().is_unspecified()
                    || self.service_config.admin_addr.ip().is_unspecified()));
        let admin_handle = if !same_port_multiplex {
            let handle = admin_impl.clone().start().await;
            info!(
                admin_addr = %self.service_config.admin_addr,
                broker_id = %self.broker_id,
                "dedicated admin gRPC server listening"
            );
            Some(handle)
        } else {
            info!(
                admin_addr = %self.service_config.broker_addr,
                broker_id = %self.broker_id,
                "admin gRPC multiplexed on broker listener"
            );
            None
        };

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
            Some(EdgeReplicatorServiceImpl::new(
                storage,
                topic_cluster,
                self.meta_store.clone(),
                self.broker.resources.schema.clone(),
            ))
        } else {
            None
        };

        let lakehouse_runtime =
            start_lakehouse_runtime_if_enabled(self.service_config.node_role).await?;

        // ----- Mode-specific startup
        match self.mode {
            BrokerMode::Cluster => {
                self.start_cluster_mode(
                    cluster.expect("ClusterServices required for cluster mode"),
                    schema_registry,
                    edge_service,
                    admin_impl,
                )
                .await?;
            }
            BrokerMode::Standalone => {
                self.start_standalone_mode(schema_registry, edge_service, admin_impl)
                    .await?;
            }
            BrokerMode::Edge => {
                self.start_edge_mode(
                    schema_registry,
                    edge_service,
                    self.edge_service
                        .clone()
                        .expect("edge_service must be set for Edge mode"),
                    admin_impl,
                )
                .await?;
            }
        }

        shutdown_lakehouse_runtime(lakehouse_runtime).await;

        // ----- Wait for dedicated admin server to complete (if started)
        if let Some(handle) = admin_handle {
            if let Err(e) = handle.await {
                eprintln!("Danube Admin failed: {:?}", e);
            }
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
        admin_impl: DanubeAdminImpl,
    ) -> Result<()> {
        // 1. Wait for Raft membership (--join or scale-up JoinExisting)
        let auto_joined = if cluster.join_cluster {
            self.wait_for_cluster_membership(&cluster.raft, &cluster.raft_addr)
                .await
        } else {
            false
        };

        // 2. Create cluster record
        let _ = self
            .resources
            .cluster
            .create_cluster(&self.service_config.cluster_name)
            .await;

        // 3. Register broker
        self.register_broker(32).await?; // TTL = 32s for cluster

        // 4. Determine and set initial broker state
        self.determine_initial_state(&cluster, auto_joined).await;

        // 4b. Reconcile cluster object store configuration before loading topics
        self.reconcile_cluster_storage_config().await;

        // 5. Reconcile pre-existing topic assignments
        self.reconcile_topic_assignments().await;

        // 6–7. Namespaces + auth cache
        self.initialize_metadata_and_auth().await?;

        // 8. Broker gRPC + replicator
        let server_handle = self
            .start_broker_grpc(schema_registry, edge_service, Some(admin_impl))
            .await?;

        // 9. Start cluster orchestration services
        self.start_orchestration_services(&mut cluster).await?;

        spawn_graceful_sigterm_drain(self.broker_id, self.resources.clone());
        spawn_dead_node_reaper(
            cluster.raft.clone(),
            cluster.leadership.clone(),
            self.meta_store.clone(),
        );
        spawn_cluster_storage_config_watcher(
            self.meta_store.clone(),
            self.broker.storage_factory().clone(),
        )
        .await;
        spawn_object_store_credential_refresher(self.broker.storage_factory().clone());

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
        admin_impl: DanubeAdminImpl,
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

        // 4b. Reconcile cluster object store configuration if already persisted
        self.reconcile_cluster_storage_config().await;

        // 5. Broker gRPC + replicator
        let server_handle = self
            .start_broker_grpc(schema_registry, edge_service, Some(admin_impl))
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
        edge_svc: Arc<EdgeService>,
        admin_impl: DanubeAdminImpl,
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
        let server_handle = self
            .start_broker_grpc(schema_registry, edge_service, Some(admin_impl))
            .await?;

        // 6. Ensure MQTT-mapped topics exist locally in the broker
        let mqtt_topics = edge_svc.mqtt_danube_topics();
        for topic_name in &mqtt_topics {
            if let Err(e) = self.broker.topic_manager.ensure_local(topic_name).await {
                warn!(
                    topic = %topic_name,
                    error = %e,
                    "failed to ensure local topic for MQTT mapping"
                );
            }
        }

        // 7. Start all edge background tasks (replicator + MQTT gateway).
        // EdgeService handles: topic reconciliation, MQTT provisioning,
        // flush loop, and TCP listener — all in one call.
        let existing_topics = self.broker.topic_registry.get_all_topics();
        edge_svc.start(&existing_topics).await?;

        info!(
            broker_id = %self.broker_id,
            edge_name = %edge_svc.edge_name(),
            existing_topics = existing_topics.len(),
            mqtt_topics = mqtt_topics.len(),
            "edge mode: broker active"
        );

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
        admin_impl: Option<DanubeAdminImpl>,
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
            admin_impl,
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

    /// Attempts `JoinCluster` against seed admin endpoints (when `seed_nodes` are configured)
    /// and waits until this node detects a Raft leader.
    ///
    /// Returns `true` if `JoinCluster` succeeded or seed discovery auto-joined the node.
    async fn wait_for_cluster_membership(
        &self,
        raft: &Raft<danube_raft::typ::TypeConfig>,
        raft_addr: &str,
    ) -> bool {
        info!(
            broker_id = %self.broker_id,
            seeds = ?self.service_config.meta_store.seed_nodes,
            "waiting for cluster membership..."
        );
        let mut auto_joined = false;
        let mut attempt = 0u64;
        loop {
            let metrics = raft.metrics().borrow().clone();
            if metrics.current_leader.is_some() {
                info!(
                    broker_id = %self.broker_id,
                    leader = ?metrics.current_leader,
                    auto_joined,
                    "cluster membership detected — node has joined the cluster"
                );
                return auto_joined;
            }

            if !auto_joined && !self.service_config.meta_store.seed_nodes.is_empty() && attempt % 4 == 0
            {
                if self.try_join_cluster_via_seeds(raft_addr).await {
                    auto_joined = true;
                }
            }

            attempt = attempt.wrapping_add(1);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn try_join_cluster_via_seeds(&self, raft_addr: &str) -> bool {
        use danube_core::admin_proto::cluster_admin_client::ClusterAdminClient;
        use danube_core::admin_proto::JoinClusterRequest;

        let scheme = if self.service_config.admin_tls {
            "https"
        } else {
            "http"
        };
        let local_admin_port = self.service_config.admin_addr.port();
        let mut candidates: Vec<String> = Vec::new();
        for seed in &self.service_config.meta_store.seed_nodes {
            let trimmed = seed.trim();
            if trimmed.is_empty() {
                continue;
            }
            let normalized = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
                trimmed.to_string()
            } else {
                format!("{scheme}://{trimmed}")
            };
            candidates.push(normalized);

            let host_port = trimmed
                .strip_prefix("http://")
                .or_else(|| trimmed.strip_prefix("https://"))
                .unwrap_or(trimmed);
            if let Some((host, _port)) = host_port.rsplit_once(':') {
                for port in [local_admin_port, 50051] {
                    let alt = format!("{scheme}://{host}:{port}");
                    if !candidates.contains(&alt) {
                        candidates.push(alt);
                    }
                }
            }
        }

        for endpoint_url in candidates {
            let Ok(mut ep) = tonic::transport::Endpoint::from_shared(endpoint_url.clone()) else {
                continue;
            };
            ep = ep.connect_timeout(Duration::from_secs(3));
            if endpoint_url.starts_with("https://") {
                if let Some(ref tls_cfg) = self.service_config.auth.tls {
                    if let (Ok(ca), Ok(cert), Ok(key)) = (
                        std::fs::read(&tls_cfg.ca_file),
                        std::fs::read(&tls_cfg.cert_file),
                        std::fs::read(&tls_cfg.key_file),
                    ) {
                        let tls = tonic::transport::ClientTlsConfig::new()
                            .ca_certificate(tonic::transport::Certificate::from_pem(ca))
                            .identity(tonic::transport::Identity::from_pem(cert, key));
                        if let Ok(configured) = ep.clone().tls_config(tls) {
                            ep = configured;
                        }
                    }
                }
            }

            let Ok(channel) = ep.connect().await else {
                continue;
            };

            let mut client = ClusterAdminClient::new(channel);
            let mut req = tonic::Request::new(JoinClusterRequest {
                node_id: self.broker_id,
                raft_addr: raft_addr.to_string(),
                target_voter_count: 0,
            });
            if let Ok(val) = format!("broker/{}", self.broker_id).parse() {
                req.metadata_mut().insert("x-danube-internal-broker", val);
            }

            match client.join_cluster(req).await {
                Ok(resp) => {
                    let inner = resp.into_inner();
                    info!(
                        endpoint = %endpoint_url,
                        role = %inner.role,
                        promoted = inner.promoted_to_voter,
                        message = %inner.message,
                        "auto-joined cluster via JoinCluster"
                    );
                    return true;
                }
                Err(status) => {
                    debug!(
                        endpoint = %endpoint_url,
                        error = %status,
                        "JoinCluster attempt on candidate endpoint failed"
                    );
                }
            }
        }
        false
    }

    /// Determines the initial broker state based on how it was started:
    ///
    /// - `--join` mode       → **active** (reason `"joined"`) once Raft membership is established
    /// - `--node-role lakehouse` → **drained** (does not serve client topics)
    /// - restart < TTL       → **active** (fast restart, still registered)
    /// - full cluster restart → **active** (all registrations expired, safe)
    /// - restart > TTL       → **drained** (registration expired, admin must investigate)
    /// - first boot          → **active**
    async fn determine_initial_state(&self, cluster: &ClusterServices, _auto_joined: bool) {
        let (initial_state, state_reason) = if self.service_config.node_role == NodeRole::Lakehouse {
            ("drained", "lakehouse_only")
        } else if cluster.join_cluster {
            ("active", "joined")
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

    /// Reads `/cluster/config/storage` from the metadata store and configures the
    /// broker's `StorageFactory` before assigned topics are loaded.
    async fn reconcile_cluster_storage_config(&self) {
        match self
            .meta_store
            .get("/cluster/config/storage", MetaOptions::None)
            .await
        {
            Ok(Some(val)) => {
                apply_stored_cluster_storage_config(&self.broker.storage_factory(), &val).await;
            }
            Ok(None) => {}
            Err(err) => {
                warn!(error = %err, "failed to read /cluster/config/storage on startup");
            }
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
            // Expected: /cluster/brokers/{id}/{ns}/{topic...}
            if parts.len() < 6 || parts[4] == "state" {
                continue;
            }
            let topic_name = format!("/{}/{}", parts[4], parts[5..].join("/"));
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

const DEFAULT_DEAD_NODE_GRACE_PERIOD_SECS: u64 = 3600;
const MIN_QUORUM_VOTERS_FLOOR: usize = 3;

/// Returns `true` when an absent Raft node has exceeded the grace period and
/// removing it does not drop the cluster voter count below `MIN_QUORUM_VOTERS_FLOOR`.
pub(crate) fn should_reap_dead_node(
    missing_for_secs: u64,
    grace_period_secs: u64,
    is_voter: bool,
    current_voter_count: usize,
) -> bool {
    if missing_for_secs < grace_period_secs {
        return false;
    }
    if is_voter && current_voter_count <= MIN_QUORUM_VOTERS_FLOOR {
        return false;
    }
    true
}

/// On SIGTERM / SIGINT, drain this broker gracefully (mark `"draining"` and schedule
/// hosted topics for unload) WITHOUT removing the node from Raft membership.
fn spawn_graceful_sigterm_drain(broker_id: u64, resources: Resources) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
                tokio::select! {
                    _ = sigterm.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            } else {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }

        info!(
            broker_id,
            "shutdown signal received: draining broker topics gracefully without removing Raft member"
        );
        let broker_key = broker_id.to_string();
        if let Err(err) = resources
            .cluster
            .set_broker_state(&broker_key, "draining", Some("sigterm_drain"))
            .await
        {
            warn!(broker_id, error = %err, "failed to mark broker draining on SIGTERM");
        }
        let topics = resources.cluster.get_topics_for_broker(&broker_key).await;
        let had_topics = !topics.is_empty();
        for topic in topics {
            if let Err(err) = resources.cluster.mark_topic_for_unload(&topic, broker_id).await {
                warn!(broker_id, topic = %topic, error = %err, "failed to mark topic for unload on SIGTERM");
            }
            if let Err(err) = resources.cluster.schedule_topic_deletion(&broker_key, &topic).await {
                warn!(broker_id, topic = %topic, error = %err, "failed to schedule topic deletion on SIGTERM");
            }
        }
        if had_topics {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let _ = resources
            .cluster
            .set_broker_state(&broker_key, "drained", Some("sigterm_drain"))
            .await;
        info!(broker_id, "graceful drain complete, exiting");
        std::process::exit(0);
    });
}

/// Leader background loop that tracks missing registrations and only reaps dead
/// nodes from Raft membership after `DEFAULT_DEAD_NODE_GRACE_PERIOD_SECS` (3600s),
/// while never dropping below 3 voters.
fn spawn_dead_node_reaper(
    raft: Raft<danube_raft::typ::TypeConfig>,
    leadership: LeadershipHandle,
    meta_store: MetadataStorage,
) {
    tokio::spawn(async move {
        let mut missing_since: std::collections::HashMap<u64, std::time::Instant> =
            std::collections::HashMap::new();
        let mut interval = time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            if !leadership.is_leader() {
                missing_since.clear();
                continue;
            }
            let registered: std::collections::HashSet<u64> = meta_store
                .get_childrens(crate::resources::BASE_REGISTER_PATH)
                .await
                .unwrap_or_default()
                .into_iter()
                .filter_map(|p| p.rsplit('/').next().and_then(|s| s.parse::<u64>().ok()))
                .collect();

            let metrics = raft.metrics().borrow().clone();
            let voters: std::collections::BTreeSet<u64> = metrics
                .membership_config
                .membership()
                .get_joint_config()
                .first()
                .cloned()
                .unwrap_or_default();
            let all_nodes: Vec<u64> = metrics
                .membership_config
                .membership()
                .nodes()
                .map(|(id, _)| *id)
                .collect();

            missing_since.retain(|id, _| all_nodes.contains(id) && !registered.contains(id));

            let now = std::time::Instant::now();
            for node_id in all_nodes {
                if registered.contains(&node_id) {
                    continue;
                }
                let first_seen = *missing_since.entry(node_id).or_insert(now);
                let elapsed = now.duration_since(first_seen).as_secs();
                let is_voter = voters.contains(&node_id);
                if should_reap_dead_node(
                    elapsed,
                    DEFAULT_DEAD_NODE_GRACE_PERIOD_SECS,
                    is_voter,
                    voters.len(),
                ) {
                    let mut next_voters = voters.clone();
                    next_voters.remove(&node_id);
                    if !next_voters.is_empty() {
                        if let Err(err) = raft.change_membership(next_voters, false).await {
                            warn!(node_id, error = %err, "failed to reap dead Raft node");
                        } else {
                            warn!(node_id, missing_secs = elapsed, "reaped dead Raft node after grace period");
                            missing_since.remove(&node_id);
                        }
                    }
                }
            }
        }
    });
}

fn spawn_object_store_credential_refresher(
    storage_factory: danube_persistent_storage::StorageFactory,
) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1800));
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(err) = storage_factory.refresh_object_store_credentials() {
                warn!(error = %err, "proactive object store credential refresh failed");
            }
        }
    });
}

async fn apply_stored_cluster_storage_config(
    storage_factory: &danube_persistent_storage::StorageFactory,
    val: &serde_json::Value,
) {
    let Some(backend) = val.get("backend").and_then(|v| v.as_str()) else {
        warn!("ignoring /cluster/config/storage without 'backend' field");
        return;
    };
    let Some(root) = val.get("root").and_then(|v| v.as_str()) else {
        warn!("ignoring /cluster/config/storage without 'root' field");
        return;
    };
    let options: HashMap<String, String> = val
        .get("options")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let cfg = match crate::admin::desired_state::build_object_store_config(
        backend,
        root.to_string(),
        options,
    ) {
        Ok(cfg) => cfg,
        Err(err) => {
            warn!(error = %err, "failed to parse /cluster/config/storage");
            return;
        }
    };

    if let Err(err) = storage_factory.configure_object_store(cfg, true).await {
        warn!(error = %err, backend, root, "failed to apply cluster object store config");
    } else {
        info!(backend, root, "reconciled cluster object store configuration");
    }
}

async fn spawn_cluster_storage_config_watcher(
    meta_store: MetadataStorage,
    storage_factory: danube_persistent_storage::StorageFactory,
) {
    match meta_store.watch("/cluster/config/storage").await {
        Ok(mut watch_stream) => {
            tokio::spawn(async move {
                while let Some(result) = watch_stream.next().await {
                    match result {
                        Ok(WatchEvent::Put { value, .. }) => {
                            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&value) {
                                apply_stored_cluster_storage_config(&storage_factory, &val).await;
                            }
                        }
                        Ok(WatchEvent::Delete { .. }) => {}
                        Err(err) => {
                            warn!(error = %err, "cluster storage config watch error");
                        }
                    }
                }
            });
        }
        Err(err) => {
            warn!(error = %err, "failed to watch /cluster/config/storage");
        }
    }
}

#[cfg(feature = "lakehouse")]
async fn start_lakehouse_runtime_if_enabled(
    node_role: NodeRole,
) -> Result<Option<danube_iceberg::LakehouseRuntime>> {
    if node_role == NodeRole::Broker {
        debug!("skipping embedded lakehouse runtime (--node-role broker)");
        return Ok(None);
    }

    if let Ok(path) = std::env::var("DANUBE_LAKEHOUSE_CONFIG") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            let cfg = danube_iceberg::config::Config::load(trimmed)
                .with_context(|| format!("failed to load lakehouse config '{}'", trimmed))?;
            let runtime = danube_iceberg::LakehouseRuntime::start(cfg)
                .context("failed to start embedded LakehouseRuntime")?;
            info!(config = trimmed, role = ?node_role, "embedded lakehouse runtime started");
            return Ok(Some(runtime));
        }
    }

    let runtime = danube_iceberg::LakehouseRuntime::new_idle(
        danube_iceberg::DEFAULT_LAKEHOUSE_WORKER_THREADS,
        danube_iceberg::DEFAULT_LAKEHOUSE_MEMORY_CAP_BYTES,
    )
    .context("failed to initialize idle embedded LakehouseRuntime")?;
    info!(
        role = ?node_role,
        "embedded lakehouse runtime initialized in idle standby (set DANUBE_LAKEHOUSE_CONFIG to enable pipelines)"
    );
    Ok(Some(runtime))
}

#[cfg(not(feature = "lakehouse"))]
async fn start_lakehouse_runtime_if_enabled(node_role: NodeRole) -> Result<Option<()>> {
    if node_role == NodeRole::Lakehouse {
        warn!(
            "--node-role lakehouse requested, but danube-broker was compiled without the 'lakehouse' feature"
        );
    }
    Ok(None)
}

#[cfg(feature = "lakehouse")]
async fn shutdown_lakehouse_runtime(runtime: Option<danube_iceberg::LakehouseRuntime>) {
    if let Some(rt) = runtime {
        rt.shutdown();
    }
}

#[cfg(not(feature = "lakehouse"))]
async fn shutdown_lakehouse_runtime(_runtime: Option<()>) {}

#[cfg(test)]
mod dead_node_reap_tests {
    use super::should_reap_dead_node;

    #[test]
    fn dead_node_reap_respects_grace_period_and_quorum_floor() {
        assert!(!should_reap_dead_node(120, 3600, false, 5));
        assert!(should_reap_dead_node(3600, 3600, false, 3));
        assert!(!should_reap_dead_node(4000, 3600, true, 3));
        assert!(should_reap_dead_node(4000, 3600, true, 5));
    }
}
