//! Edge service orchestrator.
//!
//! Single entry point that owns and coordinates all edge-side functionality:
//! - **Replicator**: WAL tailing → cloud cluster (via `EdgeReplicator`)
//! - **MQTT gateway**: device ingestion → batched WAL writes (via `MqttIngester`)
//!
//! The broker creates an `EdgeService` once at startup and calls `start()`
//! to spin up all background tasks. It can also access the inner
//! `EdgeReplicator` for topic lifecycle coordination.

use std::sync::Arc;

use anyhow::Result;
use danube_core::metadata::MetadataStore;
use danube_persistent_storage::StorageFactory;
use tracing::{info, warn};

use crate::config::EdgeConfig;
use crate::mqtt::bridge::TopicRouter;
use crate::mqtt::ingester::{MqttIngester, MqttIngesterConfig};
use crate::replicator::checkpoint::CheckpointStore;
use crate::replicator::cluster_client::EdgeCloudClient;
use crate::replicator::replicator::{EdgeReplicator, EdgeReplicatorConfig};

/// Unified edge service orchestrator.
///
/// Encapsulates both the WAL-to-cloud replicator and the optional MQTT
/// ingestion gateway. The broker only needs to hold this single handle.
pub struct EdgeService {
    config: EdgeConfig,
    replicator: Arc<EdgeReplicator>,
    ingester: Option<Arc<MqttIngester>>,
    router: Option<Arc<TopicRouter>>,
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
    /// This creates the `EdgeReplicator` and (if the `mqtt` config section
    /// is present) the `MqttIngester` and `TopicRouter`. Nothing is started
    /// yet — call `start()` after the broker gRPC is up.
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
        let token = token_override
            .unwrap_or_else(|| config.edge.token.clone());

        // --- Replicator ---
        let cloud_client = Arc::new(
            EdgeCloudClient::new(
                &config.edge.cluster_url,
                &config.edge.edge_name,
                &token,
            )
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

        // --- MQTT (if configured) ---
        let (ingester, router) = if let Some(ref mqtt) = config.mqtt {
            let ingester_config = MqttIngesterConfig {
                batch_size: mqtt.ingestion.batch_size,
                batch_timeout: mqtt.ingestion.batch_timeout(),
            };

            let ingester = Arc::new(MqttIngester::new(
                storage_factory.clone(),
                ingester_config,
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
        })
    }

    /// Start all edge background tasks.
    ///
    /// This should be called after the broker gRPC server is up:
    /// 1. Reconcile pre-existing topics (start replication)
    /// 2. Provision MQTT topics (if mqtt section present)
    /// 3. Spawn MQTT flush loop + TCP listener
    ///
    /// `existing_topics` are topics already in the broker's topic registry
    /// from a previous run — they need replication restarted.
    pub async fn start(&self, existing_topics: &[String]) -> Result<()> {
        let edge_name = &self.config.edge.edge_name;

        // --- Reconcile pre-existing topics ---
        for topic_name in existing_topics {
            if let Err(e) = self.replicator.cloud_client().create_topic(topic_name).await {
                warn!(
                    topic = %topic_name,
                    error = %e,
                    "failed to create pre-existing topic on cluster, will retry later"
                );
                continue;
            }
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

            // Pre-provision Danube topics from config mapping
            let danube_topics = self.config.mqtt_danube_topics();
            for topic_name in &danube_topics {
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
                if let Err(e) = self.replicator.cloud_client().create_topic(topic_name).await {
                    warn!(
                        topic = %topic_name,
                        error = %e,
                        "failed to create MQTT topic on cluster, will retry later"
                    );
                }
                self.replicator.add_topic(topic_name).await;
            }

            info!(
                topics = danube_topics.len(),
                "MQTT ingester: topics provisioned"
            );

            // Spawn background flush loop
            let ingester_flush = ingester.clone();
            tokio::spawn(async move {
                ingester_flush.run_flush_loop().await;
            });

            // Spawn MQTT TCP listener
            let listener_addr = mqtt_config.listener.clone();
            let router_clone = router.clone();
            let ingester_clone = ingester.clone();
            tokio::spawn(async move {
                crate::mqtt::server::start_listener(
                    &listener_addr,
                    router_clone,
                    ingester_clone,
                )
                .await;
            });

            info!(
                edge_name = %edge_name,
                listener = %mqtt_config.listener,
                "MQTT gateway started"
            );
        }

        Ok(())
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
}
