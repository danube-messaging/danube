//! Edge service orchestrator.
//!
//! Single entry point that owns and coordinates all edge-side functionality:
//! - **Registration**: registers with the cluster via `RegisterEdge` at startup
//! - **Heartbeat**: periodic health check + schema sync via `EdgeHeartbeat`
//! - **Replicator**: WAL tailing → cloud cluster (via `EdgeReplicator`)
//! - **MQTT gateway**: device ingestion → batched WAL writes (via `MqttIngester`)
//!
//! The broker creates an `EdgeService` once at startup and calls `start()`
//! to spin up all background tasks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use danube_core::metadata::MetadataStore;
use danube_persistent_storage::StorageFactory;
use tracing::{error, info, warn};

use crate::config::EdgeConfig;
use crate::mqtt::bridge::TopicRouter;
use crate::mqtt::ingester::{MqttIngester, MqttIngesterConfig};
use crate::readiness::{CachedSchema, TopicReadiness};
use crate::replicator::checkpoint::CheckpointStore;
use crate::replicator::cluster_client::EdgeCloudClient;
use crate::replicator::replicator::{EdgeReplicator, EdgeReplicatorConfig};

/// Unified edge service orchestrator.
///
/// Encapsulates the WAL-to-cloud replicator, the optional MQTT ingestion
/// gateway, topic readiness tracking, and the cluster heartbeat loop.
/// The broker only needs to hold this single handle.
pub struct EdgeService {
    config: EdgeConfig,
    replicator: Arc<EdgeReplicator>,
    ingester: Option<Arc<MqttIngester>>,
    router: Option<Arc<TopicRouter>>,
    /// Per-topic readiness tracker (shared with ingester).
    readiness: TopicReadiness,
    /// Current config version from the cluster (set by RegisterEdge, updated by heartbeat).
    config_version: Arc<AtomicU64>,
}

impl std::fmt::Debug for EdgeService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeService")
            .field("edge_name", &self.config.edge.edge_name)
            .finish_non_exhaustive()
    }
}

impl EdgeService {
    /// Build the edge service from a config and shared dependencies.
    ///
    /// This creates the `EdgeReplicator`, `TopicReadiness`, and (if the
    /// `mqtt` config section is present) the `MqttIngester` and `TopicRouter`.
    /// Nothing is started yet — call `start()` after the broker gRPC is up.
    ///
    /// `token_override` allows the `--edge-token` CLI arg to take precedence
    /// over the config file's `edge.token` field.
    pub async fn new(
        config: EdgeConfig,
        storage_factory: StorageFactory,
        metadata_store: Arc<dyn MetadataStore>,
        token_override: Option<String>,
    ) -> Result<Self> {
        // Resolve token: CLI override > config file
        let token = token_override.unwrap_or_else(|| config.edge.token.clone());

        // --- Replicator ---
        let cloud_client = Arc::new(
            EdgeCloudClient::new(&config.edge.cluster_url, &config.edge.edge_name, &token)
                .await
                .map_err(|e| anyhow::anyhow!("failed to create edge cloud client: {}", e))?,
        );

        let replicator_config = EdgeReplicatorConfig {
            batch_size: config.replicator.batch_size,
            batch_timeout: config.replicator.batch_timeout(),
        };

        let checkpoint = Arc::new(CheckpointStore::new(metadata_store));

        let replicator = Arc::new(EdgeReplicator::new(
            cloud_client,
            storage_factory.clone(),
            checkpoint,
            replicator_config,
        ));

        // --- Readiness tracker ---
        let readiness = TopicReadiness::new();

        // --- MQTT (if configured) ---
        let (ingester, router) = if let Some(ref mqtt) = config.mqtt {
            let ingester_config = MqttIngesterConfig {
                batch_size: mqtt.ingestion.batch_size,
                batch_timeout: mqtt.ingestion.batch_timeout(),
            };

            let ingester = Arc::new(MqttIngester::new(
                storage_factory.clone(),
                ingester_config,
                readiness.clone(),
            ));

            let router = Arc::new(TopicRouter::new(&mqtt.topic_mappings));

            (Some(ingester), Some(router))
        } else {
            (None, None)
        };

        Ok(Self {
            config,
            replicator,
            ingester,
            router,
            readiness,
            config_version: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Start all edge background tasks.
    ///
    /// This should be called after the broker gRPC server is up.
    /// Implements a 6-phase bootstrap:
    ///
    /// 1. **Namespace validation** — all topics must be under `/{edge_name}/`
    /// 2. **Local provisioning** — create WAL handles for each topic
    /// 3. **Cluster registration** — `RegisterEdge` RPC (creates topics, resolves schemas)
    /// 4. **Readiness marking** — mark topics as ready based on cluster response
    /// 5. **Spawn replication** — start WAL tailing for all topics
    /// 6. **Spawn MQTT + heartbeat** — start listener, flush loop, and heartbeat
    ///
    /// `existing_topics` are topics already in the broker's topic registry
    /// from a previous run — they need replication restarted.
    pub async fn start(&self, existing_topics: &[String]) -> Result<()> {
        let edge_name = &self.config.edge.edge_name;
        let edge_prefix = format!("/{}/", edge_name);

        // --- Phase 1: Namespace validation ---
        if let Some(ref mqtt) = self.config.mqtt {
            for mapping in &mqtt.topic_mappings {
                if !mapping.danube_topic.starts_with(&edge_prefix) {
                    error!(
                        topic = %mapping.danube_topic,
                        expected_prefix = %edge_prefix,
                        "topic must be under the edge's namespace — skipping this mapping"
                    );
                }
            }
        }

        // --- Reconcile pre-existing topics (replication only, no MQTT) ---
        for topic_name in existing_topics {
            self.replicator.add_topic(topic_name).await;
        }

        info!(
            edge_name = %edge_name,
            existing_topics = existing_topics.len(),
            "edge replicator: reconciled pre-existing topics"
        );

        // --- MQTT gateway ---
        if let (Some(ingester), Some(router)) = (&self.ingester, &self.router) {
            let mqtt_config = self.config.mqtt.as_ref().unwrap();

            // Phase 2: Local provisioning — create WAL handles
            let danube_topics = self.config.mqtt_danube_topics();
            for topic_name in &danube_topics {
                // Validate namespace
                if !topic_name.starts_with(&edge_prefix) {
                    error!(
                        topic = %topic_name,
                        expected_prefix = %edge_prefix,
                        "skipping topic not under edge namespace"
                    );
                    continue;
                }

                // Track in readiness
                self.readiness.track_topic(topic_name).await;

                // Provision WAL handle
                if let Err(e) = ingester.provision_topic(topic_name).await {
                    warn!(
                        topic = %topic_name,
                        error = %e,
                        "failed to provision MQTT ingester topic"
                    );
                    continue;
                }
                self.readiness.mark_local_provisioned(topic_name).await;
            }

            info!(
                topics = danube_topics.len(),
                "MQTT ingester: local topics provisioned"
            );

            // Phase 3: Register with cluster (single RPC)
            let declarations = self.config.topic_declarations();
            match self
                .replicator
                .cloud_client()
                .register_edge(edge_name, declarations)
                .await
            {
                Ok(response) => {
                    self.config_version
                        .store(response.config_version, Ordering::Relaxed);

                    // Phase 4: Process registration results
                    for result in &response.topics {
                        if !result.error.is_empty() {
                            error!(
                                topic = %result.topic_name,
                                error = %result.error,
                                "cluster rejected topic registration"
                            );
                            continue;
                        }

                        self.readiness
                            .mark_cluster_registered(&result.topic_name)
                            .await;

                        if result.schema_resolved {
                            let cached = result.schema.as_ref().map(|s| CachedSchema {
                                subject: s.subject.clone(),
                                schema_id: s.schema_id,
                                schema_version: s.schema_version,
                                schema_type: s.schema_type.clone(),
                                fingerprint: s.fingerprint.clone(),
                                schema_definition: s.schema_definition.clone(),
                            });

                            // Look up validation_policy from config for this topic
                            let enforce = mqtt_config.topic_mappings.iter()
                                .find(|m| m.danube_topic == result.topic_name)
                                .and_then(|m| m.validation_policy.as_deref())
                                .map(|p| p.eq_ignore_ascii_case("enforce"))
                                .unwrap_or(false);

                            self.readiness
                                .mark_schema_resolved(&result.topic_name, cached, enforce)
                                .await;
                        } else {
                            warn!(
                                topic = %result.topic_name,
                                "schema not resolved on cluster — topic NOT READY"
                            );
                        }

                        info!(
                            topic = %result.topic_name,
                            created = result.topic_created,
                            schema_resolved = result.schema_resolved,
                            "topic registered on cluster"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        error = %e,
                        "failed to register edge with cluster — \
                         topics will stay NOT READY until heartbeat succeeds"
                    );
                }
            }

            // Log readiness summary
            let not_ready = self.readiness.not_ready_topics().await;
            if not_ready.is_empty() {
                info!("all MQTT topics are READY");
            } else {
                warn!(
                    not_ready = ?not_ready,
                    "some topics are NOT READY — MQTT messages for these will be dropped"
                );
            }

            // Phase 5: Start replication for all MQTT topics
            for topic_name in &danube_topics {
                self.replicator.add_topic(topic_name).await;
            }

            // Phase 6: Spawn background tasks

            // Create a shutdown channel for graceful MQTT teardown.
            // Sender stays in EdgeService (or signal handler); receivers in tasks.
            let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

            // Spawn background flush loop (shutdown-aware)
            let ingester_flush = ingester.clone();
            let flush_shutdown = shutdown_rx.clone();
            tokio::spawn(async move {
                ingester_flush.run_flush_loop(flush_shutdown).await;
            });

            // Spawn MQTT TCP listener (with connection limit + shutdown)
            let listener_addr = mqtt_config.listener.clone();
            let max_payload_size = mqtt_config.max_payload_size;
            let max_connections = mqtt_config.max_connections;
            let router_clone = router.clone();
            let ingester_clone = ingester.clone();
            let listener_shutdown = shutdown_rx.clone();
            tokio::spawn(async move {
                crate::mqtt::server::start_listener(
                    &listener_addr,
                    router_clone,
                    ingester_clone,
                    max_payload_size,
                    max_connections,
                    listener_shutdown,
                )
                .await;
            });

            info!(
                edge_name = %edge_name,
                listener = %mqtt_config.listener,
                max_payload_size,
                max_connections,
                "MQTT gateway started"
            );

            // Spawn heartbeat loop
            let heartbeat_interval = self.config.edge.heartbeat_interval();
            let cloud_client = self.replicator.cloud_client().clone();
            let readiness = self.readiness.clone();
            let edge_name_owned = edge_name.to_string();
            let config_version = self.config_version.clone();

            tokio::spawn(async move {
                Self::run_heartbeat_loop(
                    &edge_name_owned,
                    heartbeat_interval,
                    cloud_client,
                    readiness,
                    config_version,
                )
                .await;
            });
        }

        Ok(())
    }

    /// Background heartbeat loop.
    ///
    /// Periodically pings the cluster with our `config_version`. If the
    /// cluster reports changes (schema updates, topic removals), updates
    /// the local readiness state accordingly.
    async fn run_heartbeat_loop(
        edge_name: &str,
        interval: std::time::Duration,
        cloud_client: Arc<EdgeCloudClient>,
        readiness: TopicReadiness,
        config_version: Arc<AtomicU64>,
    ) {
        info!(
            edge_name = %edge_name,
            interval_ms = interval.as_millis() as u64,
            "starting edge heartbeat loop"
        );

        loop {
            tokio::time::sleep(interval).await;

            let current_version = config_version.load(Ordering::Relaxed);

            match cloud_client
                .edge_heartbeat(edge_name, current_version)
                .await
            {
                Ok(response) if !response.changed => {
                    // Fast path: nothing changed
                }
                Ok(response) => {
                    info!(
                        old_version = current_version,
                        new_version = response.config_version,
                        changes = response.changes.len(),
                        "heartbeat detected changes from cluster"
                    );

                    for change in &response.changes {
                        match change.change_type {
                            EdgeChangeType::SchemaUpdated => {
                                if let Some(ref schema) = change.schema {
                                    let cached = CachedSchema {
                                        subject: schema.subject.clone(),
                                        schema_id: schema.schema_id,
                                        schema_version: schema.schema_version,
                                        schema_type: schema.schema_type.clone(),
                                        fingerprint: schema.fingerprint.clone(),
                                        schema_definition: schema.schema_definition.clone(),
                                    };
                                    readiness.update_schema(&change.topic_name, cached).await;
                                }
                            }
                            EdgeChangeType::SchemaRemoved => {
                                warn!(
                                    topic = %change.topic_name,
                                    "schema removed on cluster — topic now NOT READY"
                                );
                                readiness.mark_schema_unresolved(&change.topic_name).await;
                            }
                            EdgeChangeType::TopicRemoved => {
                                warn!(
                                    topic = %change.topic_name,
                                    "topic removed from cluster — topic now NOT READY"
                                );
                                readiness.mark_schema_unresolved(&change.topic_name).await;
                            }
                        }
                    }

                    config_version.store(response.config_version, Ordering::Relaxed);
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "heartbeat failed — will retry next interval"
                    );
                }
            }
        }
    }

    /// Access the replicator (needed by BrokerService for topic create/delete).
    pub fn replicator(&self) -> &Arc<EdgeReplicator> {
        &self.replicator
    }

    /// Get the edge name from config.
    pub fn edge_name(&self) -> &str {
        &self.config.edge.edge_name
    }

    /// Get the list of Danube topics required by MQTT mappings.
    /// The broker uses this to pre-ensure topics exist locally.
    pub fn mqtt_danube_topics(&self) -> Vec<String> {
        self.config.mqtt_danube_topics()
    }

    /// Get the readiness tracker (for testing or external queries).
    pub fn readiness(&self) -> &TopicReadiness {
        &self.readiness
    }
}

/// Change types returned by the cluster heartbeat.
/// These mirror the proto `EdgeChangeType` enum but are used in the
/// edge service logic without proto dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeChangeType {
    SchemaUpdated,
    SchemaRemoved,
    TopicRemoved,
}

/// Parsed heartbeat response used by the edge service.
#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    pub changed: bool,
    pub config_version: u64,
    pub changes: Vec<HeartbeatChange>,
}

/// A single change from a heartbeat response.
#[derive(Debug, Clone)]
pub struct HeartbeatChange {
    pub topic_name: String,
    pub change_type: EdgeChangeType,
    pub schema: Option<ResolvedSchemaInfo>,
}

/// Resolved schema info from the cluster (used in both RegisterEdge and heartbeat).
#[derive(Debug, Clone)]
pub struct ResolvedSchemaInfo {
    pub subject: String,
    pub schema_id: u64,
    pub schema_version: u32,
    pub schema_type: String,
    pub schema_definition: Vec<u8>,
    pub fingerprint: String,
}

/// Registration response from the cluster.
#[derive(Debug, Clone)]
pub struct RegisterEdgeResult {
    pub config_version: u64,
    pub topics: Vec<TopicRegistrationInfo>,
}

/// Per-topic result from RegisterEdge.
#[derive(Debug, Clone)]
pub struct TopicRegistrationInfo {
    pub topic_name: String,
    pub topic_created: bool,
    pub schema_resolved: bool,
    pub schema: Option<ResolvedSchemaInfo>,
    pub error: String,
}
