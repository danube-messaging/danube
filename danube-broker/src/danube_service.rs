mod broker_register;
mod broker_watcher;
mod leader_election;
mod load_manager;
mod local_cache;
mod syncronizer;

pub(crate) use broker_register::register_broker;
pub(crate) use leader_election::{LeaderElection, LeaderElectionState};
pub(crate) use load_manager::load_report::{generate_load_report, LoadReport};
pub(crate) use load_manager::LoadManager;
pub(crate) use local_cache::LocalCache;
pub(crate) use syncronizer::Syncronizer;

use anyhow::Result;
use danube_client::DanubeClient;
use danube_metadata_store::{MetaOptions, MetadataStorage, MetadataStore};
use std::sync::Arc;

use tokio::time::{self, Duration};
use tracing::{info, trace, warn};

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
#[derive(Debug)]
pub(crate) struct DanubeService {
    broker_id: u64,
    broker: Arc<BrokerService>,
    service_config: ServiceConfiguration,
    meta_store: MetadataStorage,
    local_cache: LocalCache,
    resources: Resources,
    leader_election: LeaderElection,
    syncronizer: Syncronizer,
    load_manager: LoadManager,
}

// DanubeService act as a a coordinator for managing clusters, including storage and brokers.
impl DanubeService {
    pub(crate) fn new(
        broker_id: u64,
        broker: Arc<BrokerService>,
        service_config: ServiceConfiguration,
        meta_store: MetadataStorage,
        local_cache: LocalCache,
        resources: Resources,
        leader_election: LeaderElection,
        syncronizer: Syncronizer,
        load_manager: LoadManager,
    ) -> Self {
        DanubeService {
            broker_id,
            broker,
            service_config,
            meta_store,
            local_cache,
            resources,
            leader_election,
            syncronizer,
            load_manager,
        }
    }

    pub(crate) async fn start(&mut self) -> Result<()> {
        info!(
            "Initializing Danube cluster '{}'",
            self.service_config.cluster_name
        );

        // Start the Local Cache
        //==========================================================================

        // Fetch initial data, populate cache & watch for Events to update local cache

        let local_cache_cloned = self.local_cache.clone();
        let watch_stream = self.local_cache.populate_start_local_cache().await?;
        // Process the ETCD Watch events
        tokio::spawn(async move { local_cache_cloned.process_event(watch_stream).await });

        info!("Local Cache service initialized and ready");

        // Cluster metadata setup
        //==========================================================================
        let _ = self
            .resources
            .cluster
            .create_cluster(&self.service_config.cluster_name)
            .await;

        // register the local broker to cluster
        let advertised_addr = if let Some(advertised_addr) = &self.service_config.advertised_addr {
            advertised_addr.to_string()
        } else {
            self.service_config.broker_addr.clone().to_string()
        };

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
            &advertised_addr,
            &admin_addr,
            metrics_addr.as_deref(),
            ttl,
            is_secure,
        )
        .await?;

        // Initialize broker state to active
        if let Err(e) = self
            .resources
            .cluster
            .set_broker_state(&self.broker_id.to_string(), "active", Some("boot"))
            .await
        {
            warn!("Failed to set initial broker state to active: {}", e);
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

        info!("Cluster metadata initialization completed successfully");

        // Start the Broker GRPC server
        //==========================================================================

        // Initialize Schema Registry Service
        let schema_registry = Arc::new(SchemaRegistryService::new(
            self.local_cache.clone(),
            self.meta_store.clone(),
            self.broker.topic_manager.clone(),
        ));

        info!("Schema Registry Service initialized");

        let grpc_server = broker_server::DanubeServerImpl::new(
            self.broker.clone(),
            schema_registry.clone(),
            self.service_config.broker_addr.clone(),
            self.service_config.auth.clone(),
        );

        // Create a oneshot channel for readiness signaling
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        let server_handle = grpc_server.start(ready_tx).await;

        info!(
            "Broker gRPC server listening on {}",
            self.service_config.broker_addr
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
        info!("Leader Election service initialized and ready");

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

        info!("Load Manager service initialized and ready");

        // Publish periodic Load Reports
        // This enable the broker to register with Load Manager
        tokio::spawn(async move {
            post_broker_load_report(broker_service_cloned, meta_store_cloned).await
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

        // Start the Danube Admin GRPC server
        //==========================================================================

        let broker_service_cloned = Arc::clone(&self.broker);

        let admin_server = DanubeAdminImpl::new(
            self.service_config.admin_addr.clone(),
            broker_service_cloned,
            self.resources.clone(),
            self.service_config.auth.clone(),
            schema_registry,
        );

        let admin_handle: tokio::task::JoinHandle<()> = admin_server.start().await;

        info!(
            "Admin gRPC server listening on {}",
            self.service_config.admin_addr
        );

        // Wait for the server task to complete
        // Await both tasks concurrently
        let (result_server, result_admin) = tokio::join!(server_handle, admin_handle);

        // Handle the results
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
        info!("Namespace {} already exists.", namespace_name);
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
/// - **Interval**: Every 30 seconds
/// - **Data Collection**: Current topic count and assignments
/// - **Publication**: Stores report at `/cluster/load/{broker_id}`
///
/// ## Load Report Contents:
/// Generated by `generate_load_report()` containing:
/// - Number of assigned topics
/// - List of topic names
/// - Resource utilization metrics
async fn post_broker_load_report(broker_service: Arc<BrokerService>, meta_store: MetadataStorage) {
    let mut topics: Vec<String>;
    let mut broker_id;
    let mut interval = time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        topics = broker_service.get_topics();
        broker_id = broker_service.broker_id;
        let topics_len = topics.len();
        let load_report: LoadReport = generate_load_report(topics_len, topics);
        if let Ok(value) = serde_json::to_value(&load_report) {
            let path = join_path(&[BASE_BROKER_LOAD_PATH, &broker_id.to_string()]);
            match meta_store.put(&path, value, MetaOptions::None).await {
                Ok(_) => trace!(
                    "Broker {} posted a new Load Report: {:?}",
                    broker_id,
                    &load_report
                ),
                Err(err) => trace!(
                    "Broker {} unable to post Load Report dues to this issue {}",
                    broker_id,
                    err
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
