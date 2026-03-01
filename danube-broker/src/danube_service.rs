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
pub(crate) use syncronizer::Syncronizer;

use anyhow::Result;
use danube_client::DanubeClient;
use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_raft::leadership::LeadershipHandle;
use danube_raft::Raft;
use std::sync::Arc;

use tokio::time::{self, Duration};
use tracing::{debug, info, warn};

use crate::{
    admin::DanubeAdminImpl,
    auth::AuthMode,
    broker_server::{self, SchemaRegistryService},
    broker_service::BrokerService,
    policies::Policies,
    resources::{Resources, BASE_BROKER_LOAD_PATH, DEFAULT_NAMESPACE, SYSTEM_NAMESPACE},
    service_configuration::ServiceConfiguration,
    topic::SYSTEM_TOPIC,
    utils::join_path,
};

// Danube Service has cluster and local Broker management & coordination responsabilities
//
// Namespace Creation and Management:
// Allow users to create & delete namespaces within the cluster.
// This includes specifying policies and configurations specific to each namespace.
//
// LookUp Service:
// Provide a mechanism for clients to discover the brokers that own the desired topics.
// This is essential for producers to know where to send messages and for consumers to know where to fetch messages from.
// Handle client redirections if the broker ownership of a topic or partition changes.
//
// Leader Election:
// Leader Election service is needed for critical tasks such as topic assignment to brokers and partitioning.
// Load Manager is using this service, as only one broker is selected to make the load usage calculations and post the results.
// Should be selected one broker leader per cluster, who takes the decissions.
//
// Load Manager:
// The Load Manager monitors and distributes load across brokers by managing topic and partition assignments.
// It implements rebalancing logic to redistribute topics/partitions when brokers join or leave the cluster
// and is responsible for failover mechanisms to handle broker failures.
//
// Syncronizer
// The synchronizer ensures that metadata and configuration settings across different brokers remain consistent.
// It propagates changes to metadata and configuration settings using client Producers and Consumers.
// This is in addition to Metadata Storage watch events, allowing brokers to process metadata updates
// even if there was a communication glitch or the broker was unavailable for a short period, potentially missing the Store Watch events.
//
// Monitoring and Metrics:
// Collect and provide metrics related to namespace usage, such as message rates, storage usage, and throughput.
// Integrate with monitoring and alerting systems to provide insights into namespace performance and detect anomalies.
//
// Resource Quotas:
// Implement and enforce resource quotas to ensure fair usage of resources among different namespaces.
// This includes limiting the number of topics, message rates, and storage usage.
pub(crate) struct DanubeService {
    broker_id: u64,
    broker: Arc<BrokerService>,
    service_config: ServiceConfiguration,
    meta_store: MetadataStorage,
    resources: Resources,
    leader_election: LeaderElection,
    syncronizer: Syncronizer,
    load_manager: LoadManager,
    raft: Raft<danube_raft::typ::TypeConfig>,
    leadership: LeadershipHandle,
    raft_addr: String,
    /// When true, the broker was started with --join and should wait
    /// for cluster membership before registering. Joins as Drained.
    join_cluster: bool,
    /// True if bootstrap_cluster found persisted Raft state (restart, not first boot).
    was_restart: bool,
}

impl std::fmt::Debug for DanubeService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DanubeService")
            .field("broker_id", &self.broker_id)
            .field("leadership", &self.leadership)
            .finish_non_exhaustive()
    }
}

// DanubeService act as a a coordinator for managing clusters, including storage and brokers.
impl DanubeService {
    pub(crate) fn new(
        broker_id: u64,
        broker: Arc<BrokerService>,
        service_config: ServiceConfiguration,
        meta_store: MetadataStorage,
        resources: Resources,
        leader_election: LeaderElection,
        syncronizer: Syncronizer,
        load_manager: LoadManager,
        raft: Raft<danube_raft::typ::TypeConfig>,
        leadership: LeadershipHandle,
        raft_addr: String,
        join_cluster: bool,
        was_restart: bool,
    ) -> Self {
        DanubeService {
            broker_id,
            broker,
            service_config,
            meta_store,
            resources,
            leader_election,
            syncronizer,
            load_manager,
            raft,
            leadership,
            raft_addr,
            join_cluster,
            was_restart,
        }
    }

    pub(crate) async fn start(&mut self) -> Result<()> {
        info!(
            cluster = %self.service_config.cluster_name,
            broker_id = %self.broker_id,
            "initializing Danube cluster"
        );

        // Initialize Schema Registry Service (needed by both admin and broker gRPC)
        let schema_registry = Arc::new(SchemaRegistryService::new(
            self.meta_store.clone(),
            self.broker.topic_manager.clone(),
        ));
        info!("schema registry service initialized");

        // Start the Danube Admin GRPC server early
        //==========================================================================
        // Admin server must be available before cluster membership so that
        // `danube-admin cluster add-node` can discover this node via ClusterStatus.

        let admin_server = DanubeAdminImpl::new(
            self.service_config.admin_addr.clone(),
            Arc::clone(&self.broker),
            self.resources.clone(),
            self.service_config.auth.clone(),
            self.service_config.admin_tls,
            schema_registry.clone(),
            self.load_manager.clone(),
            self.raft.clone(),
            self.leadership.clone(),
            self.raft_addr.clone(),
        );

        let admin_handle: tokio::task::JoinHandle<()> = admin_server.start().await;

        info!(
            admin_addr = %self.service_config.admin_addr,
            broker_id = %self.broker_id,
            "admin gRPC server listening"
        );

        // --join mode: wait for Raft cluster membership
        //==========================================================================
        // When started with --join, this node is not yet part of any cluster.
        // The admin must run `danube-admin cluster add-node` + `promote-node`
        // to add this node. We poll Raft metrics until we detect a leader,
        // which means we've been added and replication has started.
        if self.join_cluster {
            info!(
                broker_id = %self.broker_id,
                "waiting for cluster membership (use `danube-admin cluster add-node` to add this node)..."
            );
            loop {
                let metrics = self.raft.metrics().borrow().clone();
                if metrics.current_leader.is_some() {
                    info!(
                        broker_id = %self.broker_id,
                        leader = ?metrics.current_leader,
                        "cluster membership detected — node has joined the cluster"
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }

        // Cluster metadata setup
        //==========================================================================
        let _ = self
            .resources
            .cluster
            .create_cluster(&self.service_config.cluster_name)
            .await;

        // register the local broker to cluster
        let broker_url = &self.service_config.broker_url;
        let connect_url = &self.service_config.connect_url;

        //check it is a secure connection
        let is_secure = self.service_config.auth.mode == AuthMode::Tls
            || self.service_config.auth.mode == AuthMode::TlsWithJwt;

        let ttl = 32; // Time to live for the lease in seconds
        let admin_addr = self.service_config.admin_addr.to_string();
        let metrics_addr = self
            .service_config
            .prom_exporter
            .as_ref()
            .map(|a| a.to_string());

        register_broker(
            self.meta_store.clone(),
            &self.broker_id.to_string(),
            broker_url,
            connect_url,
            &admin_addr,
            metrics_addr.as_deref(),
            ttl,
            is_secure,
        )
        .await?;

        // Determine initial broker state based on startup context:
        //   --join mode       → "drained" (wait for admin to activate)
        //   restart > TTL     → "drained" (registration expired, admin must investigate)
        //   full cluster restart → "active" (all registrations expired, safe to auto-activate)
        //   restart < TTL     → "active" (fast restart, still registered)
        //   first boot        → "active"
        let (initial_state, state_reason) = if self.join_cluster {
            ("drained", "join")
        } else if self.was_restart {
            // This is a restart (persisted Raft state). Check if our registration survived.
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
                // Registration still valid → restart was within TTL
                info!(
                    broker_id = %self.broker_id,
                    "restart detected: registration still valid (within TTL)"
                );
                ("active", "fast_restart")
            } else {
                // Registration expired. Check if this is a full cluster restart
                // (all registrations gone) vs single broker failure.
                let all_regs = self
                    .meta_store
                    .get_childrens(crate::resources::BASE_REGISTER_PATH)
                    .await
                    .unwrap_or_default();

                if all_regs.is_empty() {
                    // No brokers registered → full cluster restart → safe to go active
                    info!(
                        broker_id = %self.broker_id,
                        "full cluster restart detected (no brokers registered). Registering as active."
                    );
                    ("active", "full_cluster_restart")
                } else {
                    // Other brokers are alive but we were declared dead
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
            // First boot
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

        // Startup topic reconciliation
        //==========================================================================
        // On restart, the in-memory TopicManager is empty but metadata may still
        // have topics assigned to this broker. The broker_watcher only fires on
        // *new* events, so pre-existing assignments are invisible. Scan and
        // recreate them now, before accepting client connections.
        {
            let broker_path = join_path(&[
                crate::resources::BASE_BROKER_PATH,
                &self.broker_id.to_string(),
            ]);
            match self.meta_store.get_childrens(&broker_path).await {
                Ok(children) => {
                    let mut reconciled = 0u32;
                    for full_path in &children {
                        let parts: Vec<&str> = full_path.split('/').collect();
                        // Expected: /cluster/brokers/{id}/{ns}/{topic}
                        if parts.len() < 6 {
                            continue;
                        }
                        // Skip non-topic keys (e.g. /cluster/brokers/{id}/state)
                        if parts[4] == "state" {
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
                Err(e) => {
                    warn!(
                        broker_id = %self.broker_id,
                        error = %e,
                        "failed to scan broker assignments for startup reconciliation"
                    );
                }
            }
        }

        //create the default Namespace
        create_namespace_if_absent(
            &mut self.resources,
            DEFAULT_NAMESPACE,
            &self.service_config.policies,
        )
        .await?;

        //create system Namespace
        create_namespace_if_absent(
            &mut self.resources,
            SYSTEM_NAMESPACE,
            &self.service_config.policies,
        )
        .await?;

        //create system topic
        if !self.resources.topic.topic_exists(SYSTEM_TOPIC).await? {
            self.resources.topic.create_topic(SYSTEM_TOPIC, 0).await?;
        }

        //create bootstrap namespaces
        for namespace in &self.service_config.bootstrap_namespaces {
            create_namespace_if_absent(
                &mut self.resources,
                &namespace,
                &self.service_config.policies,
            )
            .await?;
        }

        info!(
            broker_id = %self.broker_id,
            cluster = %self.service_config.cluster_name,
            "cluster metadata initialization completed successfully"
        );

        // Start the Broker GRPC server
        //==========================================================================

        let grpc_server = broker_server::DanubeServerImpl::new(
            self.broker.clone(),
            schema_registry.clone(),
            self.service_config.broker_addr.clone(),
            self.service_config.broker_url.clone(),
            self.service_config.connect_url.clone(),
            self.service_config.proxy_enabled,
            self.service_config.auth.clone(),
        );

        // Create a oneshot channel for readiness signaling
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        let server_handle = grpc_server.start(ready_tx).await;

        info!(
            broker_addr = %self.service_config.broker_addr,
            broker_id = %self.broker_id,
            "broker gRPC server listening"
        );

        // Start the Syncronizer
        //==========================================================================

        // Wait for the server to signal that it has started
        ready_rx.await?;

        // it is needed by syncronizer in order to publish messages on meta_topic
        let danube_client = DanubeClient::builder()
            .service_url("http://127.0.0.1:6650")
            .build()
            .await?;

        let _ = self.syncronizer.with_client(danube_client);

        // TODO! create producer / consumer and use it

        // Start the Leader Election Service
        //==========================================================================

        // to be configurable
        let leader_check_interval = time::interval(Duration::from_secs(10));

        let mut leader_election_cloned = self.leader_election.clone();

        tokio::spawn(async move {
            leader_election_cloned.start(leader_check_interval).await;
        });
        info!("leader election service initialized and ready");

        // Start the Load Manager Service
        //==========================================================================
        // at this point the broker will become visible to the rest of the brokers
        // by creating the registration and also

        let rx_event = self.load_manager.bootstrap(self.broker_id).await?;

        let mut load_manager_cloned = self.load_manager.clone();

        let broker_id_cloned = self.broker_id;
        let leader_election_cloned = self.leader_election.clone();
        // Process the ETCD Watch events
        tokio::spawn(async move {
            load_manager_cloned
                .start(rx_event, broker_id_cloned, leader_election_cloned)
                .await
        });

        let broker_service_cloned = Arc::clone(&self.broker);
        let meta_store_cloned = self.meta_store.clone();

        info!("load manager service initialized and ready");

        // Start the Automated Rebalancing Loop
        //==========================================================================
        if let Some(ref load_manager_config) = self.service_config.load_manager {
            if load_manager_config.rebalancing.enabled {
                info!(
                    check_interval_seconds = load_manager_config.rebalancing.check_interval_seconds,
                    aggressiveness = ?load_manager_config.rebalancing.aggressiveness,
                    max_moves_per_hour = load_manager_config.rebalancing.max_moves_per_hour,
                    "starting automated rebalancing loop (moves 1 topic per cycle)"
                );

                let load_manager_for_rebalancing = self.load_manager.clone();
                let rebalancing_config = load_manager_config.rebalancing.clone();
                let leader_election_for_rebalancing = self.leader_election.clone();

                tokio::spawn(async move {
                    let _handle = load_manager_for_rebalancing.start_rebalancing_loop(
                        rebalancing_config,
                        leader_election_for_rebalancing,
                    );
                    // Loop runs forever in background
                });

                info!("automated rebalancing loop started successfully");
            } else {
                info!("automated rebalancing is disabled in configuration");
            }
        } else {
            debug!("load manager configuration not found, rebalancing disabled by default");
        }

        // Publish periodic Load Reports
        // This enable the broker to register with Load Manager
        let load_report_interval = if let Some(ref lm_config) = self.service_config.load_manager {
            lm_config.load_report_interval_seconds
        } else {
            30 // Default to 30 seconds if no config
        };

        tokio::spawn(async move {
            post_broker_load_report(
                broker_service_cloned,
                meta_store_cloned,
                load_report_interval,
            )
            .await
        });

        // Watch for events of Broker's interest
        let broker_service_cloned = Arc::clone(&self.broker);
        let meta_store_cloned = self.meta_store.clone();
        broker_watcher::watch_events_for_broker(
            meta_store_cloned,
            broker_service_cloned,
            self.broker_id,
        )
        .await;

        // Wait for server tasks to complete
        //==========================================================================
        let (result_server, result_admin) = tokio::join!(server_handle, admin_handle);

        if let Err(e) = result_server {
            eprintln!("Broker Server failed: {:?}", e);
        }

        if let Err(e) = result_admin {
            eprintln!("Danube Admin failed: {:?}", e);
        }

        Ok(())
    }

    // Checks whether the broker owns a specific topic
    #[allow(dead_code)]
    pub(crate) async fn check_topic_ownership(&self, topic_name: &str) -> bool {
        self.load_manager
            .check_ownership(self.broker_id, topic_name)
            .await
    }
}

pub(crate) async fn create_namespace_if_absent(
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
        // ensure that the policies are in place for the Default Namespace
        // wrong line below as the local cache have not yet loaded all the info
        //let _policies = resources.namespace.get_policies(DEFAULT_NAMESPACE)?;
    }
    Ok(())
}

/// Periodically publishes broker load reports to the metadata store
///
/// ## Purpose:
/// Continuously reports broker resource utilization and topic assignments
/// to enable LoadManager load balancing decisions across the cluster.
///
/// ## Reporting Cycle:
/// - **Interval**: Configurable via `load_report_interval_seconds` (default: 30 seconds)
/// - **Data Collection**: Current topic count and assignments
/// - **Publication**: Stores report at `/cluster/load/{broker_id}`
///
/// ## Load Report Contents:
/// Generated by `generate_load_report()` containing:
/// - Number of assigned topics
/// - List of topic names
/// - Resource utilization metrics
async fn post_broker_load_report(
    broker_service: Arc<BrokerService>,
    meta_store: MetadataStorage,
    interval_seconds: u64,
) {
    let mut topics: Vec<String>;
    let mut broker_id;
    let mut interval = time::interval(Duration::from_secs(interval_seconds));
    loop {
        interval.tick().await;
        topics = broker_service.get_topics();
        broker_id = broker_service.broker_id;
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

#[allow(dead_code)]
pub(crate) enum LookupResult {
    BrokerUrl(String),
    RedirectUrl(String),
}
