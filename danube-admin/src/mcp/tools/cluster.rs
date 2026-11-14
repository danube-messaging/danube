//! Cluster management tools

use crate::core::AdminGrpcClient;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub async fn list_brokers(client: &Arc<AdminGrpcClient>) -> String {
    match client.list_brokers().await {
        Ok(response) => {
            if response.brokers.is_empty() {
                return "No brokers found in the cluster.".to_string();
            }

            let mut output = format!("Found {} broker(s):\n\n", response.brokers.len());

            for broker in &response.brokers {
                output.push_str(&format!(
                    "Broker ID: {}\n\
                     Status: {}\n\
                     Role: {}\n\
                     Address: {}\n\
                     Admin: {}\n\
                     Metrics: {}\n\n",
                    broker.broker_id,
                    broker.broker_status,
                    broker.broker_role,
                    broker.broker_addr,
                    broker.admin_addr,
                    broker.metrics_addr,
                ));
            }

            output
        }
        Err(e) => format!("Error listing brokers: {}", e),
    }
}

pub async fn get_leader(client: &Arc<AdminGrpcClient>) -> String {
    match client.get_leader().await {
        Ok(response) => {
            format!("Current leader broker: {}", response.leader)
        }
        Err(e) => format!("Error getting leader: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct RebalanceParams {
    /// Preview mode - show proposed moves without executing them.
    /// Set to true to see which topics will move before actually rebalancing.
    /// Default: false (execute moves)
    #[serde(default)]
    pub dry_run: bool,

    /// Maximum number of topic moves to execute in this rebalancing operation.
    /// Use this to limit the scope of rebalancing (e.g., max_moves=5 for gradual rebalancing).
    /// If not specified, all necessary moves to achieve balance will be executed.
    /// Example: 10
    pub max_moves: Option<u32>,
}

pub async fn trigger_rebalance(client: &Arc<AdminGrpcClient>, params: RebalanceParams) -> String {
    let req = danube_core::admin_proto::RebalanceRequest {
        dry_run: params.dry_run,
        max_moves: params.max_moves,
    };

    match client.trigger_rebalance(req).await {
        Ok(response) => {
            if !response.success {
                return format!("Rebalancing failed: {}", response.error_message);
            }

            let mut output = String::new();

            if params.dry_run {
                output.push_str("DRY RUN - No changes were made\n\n");
            } else if response.proposed_moves.len() > 1 {
                output.push_str("Rebalancing started in background!\n");
                output.push_str(
                    "First move initiated, remaining moves will execute automatically.\n\n",
                );
            } else if response.moves_executed > 0 {
                output.push_str("Rebalancing completed!\n\n");
            } else {
                output.push_str("Cluster is already balanced.\n\n");
            }

            output.push_str(&format!(
                "Moves initiated: {}\n\
                 Total proposed: {}\n\n",
                response.moves_executed,
                response.proposed_moves.len()
            ));

            if !response.proposed_moves.is_empty() {
                output.push_str("Topic movements:\n");
                for (i, mv) in response.proposed_moves.iter().enumerate() {
                    output.push_str(&format!(
                        "  {}. {} (broker {} -> broker {})\n     Reason: {}\n     Est. load: {:.2}\n",
                        i + 1,
                        mv.topic_name,
                        mv.from_broker,
                        mv.to_broker,
                        mv.reason,
                        mv.estimated_load
                    ));
                }
            }

            output
        }
        Err(e) => format!("Error triggering rebalance: {}", e),
    }
}

pub async fn get_cluster_balance(client: &Arc<AdminGrpcClient>) -> String {
    let req = danube_core::admin_proto::ClusterBalanceRequest {};

    match client.get_cluster_balance(req).await {
        Ok(response) => {
            let mut output = String::new();
            output.push_str("Cluster Balance Metrics:\n\n");
            output.push_str(&format!(
                "Status: {}\n\
                 Coefficient of Variation: {:.4}\n\
                 Assignment Strategy: {}\n\
                 Mean Load: {:.2}\n\
                 Max Load: {:.2}\n\
                 Min Load: {:.2}\n\
                 Std Deviation: {:.2}\n\
                 Active Brokers: {}\n\n",
                if response.coefficient_of_variation < 0.2 {
                    "✓ Well Balanced"
                } else if response.coefficient_of_variation < 0.3 {
                    "✓ Balanced"
                } else if response.coefficient_of_variation < 0.4 {
                    "⚠ Imbalanced"
                } else {
                    "✗ Severely Imbalanced"
                },
                response.coefficient_of_variation,
                response.assignment_strategy,
                response.mean_load,
                response.max_load,
                response.min_load,
                response.std_deviation,
                response.broker_count
            ));

            if !response.brokers.is_empty() {
                output.push_str("Broker Load Distribution:\n");
                for broker in &response.brokers {
                    let status = if broker.is_overloaded {
                        "⚠ OVERLOADED"
                    } else if broker.is_underloaded {
                        "↓ UNDERLOADED"
                    } else {
                        "✓ OK"
                    };

                    output.push_str(&format!(
                        "  Broker {}: {:.2} load ({} topics) - {}\n",
                        broker.broker_id, broker.load, broker.topic_count, status
                    ));
                }
            }

            if response.coefficient_of_variation >= 0.3 {
                output.push_str(
                    "\nRecommendation: Consider running 'trigger_rebalance' to improve balance.\n",
                );
            }

            output
        }
        Err(e) => format!("Error getting cluster balance: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct UnloadBrokerParams {
    /// Broker ID to unload topics from.
    /// Use list_brokers to discover available broker IDs.
    /// Example: "4541396853251852833"
    pub broker_id: String,

    /// Maximum number of topics to unload in parallel.
    /// Higher values speed up the operation but increase load on the cluster.
    /// Range: 1-10. Default: 1 (sequential unloading)
    /// Example: 3
    #[serde(default = "default_max_parallel")]
    pub max_parallel: u32,

    /// Preview mode - show which topics would be unloaded without actually doing it.
    /// Always use true first to verify the operation before executing.
    /// Default: false (execute unload)
    #[serde(default)]
    pub dry_run: bool,

    /// Must be true to execute broker unload when dry_run is false.
    #[serde(default)]
    pub confirm_destructive: bool,
}

fn default_max_parallel() -> u32 {
    1
}

pub async fn unload_broker(client: &Arc<AdminGrpcClient>, params: UnloadBrokerParams) -> String {
    if !params.dry_run {
        if let Some(refused) =
            super::require_destructive_confirmation(params.confirm_destructive, "unload_broker")
        {
            return refused;
        }
    }
    let req = danube_core::admin_proto::UnloadBrokerRequest {
        broker_id: params.broker_id.clone(),
        max_parallel: params.max_parallel,
        namespaces_include: vec![],
        namespaces_exclude: vec![],
        dry_run: params.dry_run,
        timeout_seconds: 60,
    };

    match client.unload_broker(req).await {
        Ok(response) => {
            let mut output = String::new();

            if params.dry_run {
                output.push_str("DRY RUN - No topics were actually unloaded\n\n");
            }

            output.push_str(&format!(
                "Unload Results for Broker {}:\n\
                 Total topics: {}\n\
                 Succeeded: {}\n\
                 Failed: {}\n\
                 Pending: {}\n",
                params.broker_id,
                response.total,
                response.succeeded,
                response.failed,
                response.pending
            ));

            if !response.failed_topics.is_empty() {
                output.push_str("\nFailed topics:\n");
                for topic in &response.failed_topics {
                    output.push_str(&format!("  - {}\n", topic));
                }
            }

            output
        }
        Err(e) => format!("Error unloading broker: {}", e),
    }
}

pub async fn list_namespaces(client: &Arc<AdminGrpcClient>) -> String {
    match client.list_namespaces().await {
        Ok(response) => {
            if response.namespaces.is_empty() {
                return "No namespaces found.".to_string();
            }

            let mut output = format!("Found {} namespace(s):\n\n", response.namespaces.len());
            for (i, ns) in response.namespaces.iter().enumerate() {
                output.push_str(&format!("  {}. {}\n", i + 1, ns));
            }

            output
        }
        Err(e) => format!("Error listing namespaces: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ActivateBrokerParams {
    /// Broker ID to activate and bring back into service.
    /// Use list_brokers to discover available broker IDs.
    /// Example: "4541396853251852833"
    pub broker_id: String,

    /// Reason for activation - used for audit trail and operational documentation.
    /// Describe why the broker is being activated (e.g., "Maintenance complete", "Added capacity").
    /// Default: "admin_activate"
    /// Example: "Maintenance window completed successfully"
    #[serde(default = "default_reason")]
    pub reason: String,
}

fn default_reason() -> String {
    "admin_activate".to_string()
}

pub async fn activate_broker(
    client: &Arc<AdminGrpcClient>,
    params: ActivateBrokerParams,
) -> String {
    let req = danube_core::admin_proto::ActivateBrokerRequest {
        broker_id: params.broker_id.clone(),
        reason: params.reason,
    };

    match client.activate_broker(req).await {
        Ok(response) => {
            if response.success {
                format!("✓ Successfully activated broker '{}'", params.broker_id)
            } else {
                format!("✗ Failed to activate broker '{}'", params.broker_id)
            }
        }
        Err(e) => format!("Error activating broker: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct NamespaceParams {
    /// Namespace name.
    /// Use list_namespaces to discover existing namespaces.
    /// Namespace names should be lowercase alphanumeric with hyphens (e.g., "production", "dev-team-1").
    /// The "default" namespace is always present.
    /// Example: "production", "staging", "team-analytics"
    pub namespace: String,
    /// Must be true when calling delete_namespace.
    #[serde(default)]
    pub confirm_destructive: bool,
}

pub async fn get_namespace_policies(
    client: &Arc<AdminGrpcClient>,
    params: NamespaceParams,
) -> String {
    let req = danube_core::admin_proto::NamespaceRequest {
        name: params.namespace.clone(),
    };

    match client.get_namespace_policies(req).await {
        Ok(response) => {
            format!(
                "Policies for namespace '{}':\n\n{}",
                params.namespace, response.policies
            )
        }
        Err(e) => format!("Error getting namespace policies: {}", e),
    }
}

pub async fn create_namespace(client: &Arc<AdminGrpcClient>, params: NamespaceParams) -> String {
    let req = danube_core::admin_proto::NamespaceRequest {
        name: params.namespace.clone(),
    };

    match client.create_namespace(req).await {
        Ok(response) => {
            if response.success {
                format!("✓ Successfully created namespace '{}'", params.namespace)
            } else {
                format!("✗ Failed to create namespace '{}'", params.namespace)
            }
        }
        Err(e) => format!("Error creating namespace: {}", e),
    }
}

pub async fn delete_namespace(client: &Arc<AdminGrpcClient>, params: NamespaceParams) -> String {
    if let Some(refused) =
        super::require_destructive_confirmation(params.confirm_destructive, "delete_namespace")
    {
        return refused;
    }
    let req = danube_core::admin_proto::NamespaceRequest {
        name: params.namespace.clone(),
    };

    match client.delete_namespace(req).await {
        Ok(response) => {
            if response.success {
                format!("✓ Successfully deleted namespace '{}'", params.namespace)
            } else {
                format!(
                    "✗ Failed to delete namespace '{}'. Note: namespace must be empty (no topics).",
                    params.namespace
                )
            }
        }
        Err(e) => format!("Error deleting namespace: {}", e),
    }
}

// ===== RAFT CLUSTER TOOLS =====

pub async fn cluster_status(client: &Arc<AdminGrpcClient>) -> String {
    match client.cluster_status().await {
        Ok(status) => {
            let voters_str = status
                .voters
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");

            let learners_str = if status.learners.is_empty() {
                "none".to_string()
            } else {
                status
                    .learners
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            };

            let leader_str = if status.leader_id == 0 {
                "none (election in progress)".to_string()
            } else {
                status.leader_id.to_string()
            };

            format!(
                "Raft Cluster Status:\n\n\
                 Self Node ID:  {}\n\
                 Raft Address:  {}\n\
                 Leader:        {}\n\
                 Term:          {}\n\
                 Last Applied:  {}\n\
                 Voters:        [{}] ({} node(s))\n\
                 Learners:      [{}]",
                status.self_node_id,
                status.raft_addr,
                leader_str,
                status.current_term,
                status.last_applied,
                voters_str,
                status.voters.len(),
                learners_str,
            )
        }
        Err(e) => format!("Error getting cluster status: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct AddClusterNodeParams {
    /// Admin endpoint of the new broker to add to the Raft cluster.
    /// The broker must already be running with the --join flag.
    /// Example: "http://new-broker:50054"
    pub node_addr: String,
}

pub async fn add_cluster_node(
    client: &Arc<AdminGrpcClient>,
    params: AddClusterNodeParams,
) -> String {
    // Step 1: Discover the new node's identity via its admin port
    let discover_client =
        match crate::core::AdminGrpcClient::connect(crate::core::GrpcClientConfig {
            endpoint: params.node_addr.clone(),
            request_timeout_ms: 5000,
            ..Default::default()
        })
        .await
        {
            Ok(c) => c,
            Err(e) => {
                return format!(
                    "Error connecting to new node at {}: {}",
                    params.node_addr, e
                )
            }
        };

    let status = match discover_client.cluster_status().await {
        Ok(s) => s,
        Err(e) => {
            return format!(
                "Error discovering node identity at {}: {}",
                params.node_addr, e
            )
        }
    };

    let node_id = status.self_node_id;
    let raft_addr = status.raft_addr;

    // Step 2: Add as learner via the leader
    let req = danube_core::admin_proto::AddNodeRequest {
        addr: raft_addr.clone(),
        node_id,
    };

    match client.add_node(req).await {
        Ok(response) => {
            if response.success {
                format!(
                    "✓ Node added as Raft learner\n\n\
                     Node ID:     {}\n\
                     Raft Addr:   {}\n\
                     Source:      {}\n\n\
                     Next step: Use promote_cluster_node with node_id={} to make it a voter.",
                    node_id, raft_addr, params.node_addr, node_id
                )
            } else {
                format!("✗ Failed to add node: {}", response.message)
            }
        }
        Err(e) => format!("Error adding node: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct PromoteClusterNodeParams {
    /// Node ID of the Raft learner to promote to a full voting member.
    /// Use cluster_status to discover current learners.
    /// Example: 12345678
    pub node_id: u64,
    /// Must be true to mutate Raft voter quorum.
    #[serde(default)]
    pub confirm_destructive: bool,
}

pub async fn promote_cluster_node(
    client: &Arc<AdminGrpcClient>,
    params: PromoteClusterNodeParams,
) -> String {
    if let Some(refused) = super::require_destructive_confirmation(
        params.confirm_destructive,
        "promote_cluster_node",
    ) {
        return refused;
    }
    let req = danube_core::admin_proto::PromoteNodeRequest {
        node_id: params.node_id,
    };

    match client.promote_node(req).await {
        Ok(response) => {
            if response.success {
                format!(
                    "✓ Node {} promoted to voter\n\n{}",
                    params.node_id, response.message
                )
            } else {
                format!("✗ Failed to promote node: {}", response.message)
            }
        }
        Err(e) => format!("Error promoting node: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct RemoveClusterNodeParams {
    /// Node ID to remove from the Raft cluster.
    /// Use cluster_status to discover current voters and learners.
    /// Example: 12345678
    pub node_id: u64,
    /// Must be true to remove the node from Raft membership.
    #[serde(default)]
    pub confirm_destructive: bool,
}

pub async fn remove_cluster_node(
    client: &Arc<AdminGrpcClient>,
    params: RemoveClusterNodeParams,
) -> String {
    if let Some(refused) =
        super::require_destructive_confirmation(params.confirm_destructive, "remove_cluster_node")
    {
        return refused;
    }
    let req = danube_core::admin_proto::RemoveNodeRequest {
        node_id: params.node_id,
    };

    match client.remove_node(req).await {
        Ok(response) => {
            if response.success {
                format!(
                    "✓ Node {} removed from cluster\n\n{}",
                    params.node_id, response.message
                )
            } else {
                format!("✗ Failed to remove node: {}", response.message)
            }
        }
        Err(e) => format!("Error removing node: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ConfigureObjectStoreParams {
    /// Object store backend: "s3", "gcs", "azblob", or "filesystem"
    pub backend: String,
    /// Object store URI root (e.g. "s3://my-bucket/danube")
    pub root: String,
    /// Optional backend options (region, endpoint, access_key, secret_key, etc.)
    #[serde(default)]
    pub options: std::collections::HashMap<String, String>,
    /// Allow replacing an already active object store when cloud-tiered topics exist
    #[serde(default)]
    pub force: bool,
}

pub async fn configure_object_store(
    client: &Arc<AdminGrpcClient>,
    params: ConfigureObjectStoreParams,
) -> String {
    let req = danube_core::admin_proto::ConfigureObjectStoreRequest {
        backend: params.backend,
        root: params.root,
        options: params.options,
        force: params.force,
    };
    match client.configure_object_store(req).await {
        Ok(resp) => {
            if resp.success {
                resp.message
            } else {
                format!("Failed to configure object store: {}", resp.message)
            }
        }
        Err(e) => format!("Error configuring object store: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ApplyDesiredStateParams {
    /// YAML or JSON desired-state manifest string
    pub manifest: String,
    /// Preview actions without applying changes
    #[serde(default)]
    pub dry_run: bool,
    /// Opt-in deletion of unlisted user topics
    #[serde(default)]
    pub prune: bool,
    /// Allow pruning non-empty or active topics
    #[serde(default)]
    pub force: bool,
    /// Must be true when executing with prune=true and dry_run=false.
    #[serde(default)]
    pub confirm_destructive: bool,
}

pub async fn apply_desired_state(
    client: &Arc<AdminGrpcClient>,
    params: ApplyDesiredStateParams,
) -> String {
    if !params.dry_run && params.prune {
        if let Some(refused) = super::require_destructive_confirmation(
            params.confirm_destructive,
            "apply_desired_state (prune=true)",
        ) {
            return refused;
        }
    }
    let req = danube_core::admin_proto::DesiredStateRequest {
        manifest: params.manifest,
        prune: params.prune,
        force_delete_non_empty: params.force,
    };
    if params.dry_run {
        match client.plan_desired_state(req).await {
            Ok(plan) => {
                if plan.actions.is_empty() {
                    "Plan: no changes required.".to_string()
                } else {
                    let lines: Vec<String> = plan
                        .actions
                        .iter()
                        .map(|a| format!("- [{}] {} ({})", a.kind, a.resource, a.details))
                        .collect();
                    format!("Planned actions (dry-run):\n{}", lines.join("\n"))
                }
            }
            Err(e) => format!("Error planning desired state: {}", e),
        }
    } else {
        match client.apply_desired_state(req).await {
            Ok(res) => res.message,
            Err(e) => format!("Error applying desired state: {}", e),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConfigureClusterStorageParams {
    /// Storage mode: "local", "shared_fs", or "object_store"
    pub mode: String,
    /// Backend for object_store mode ("s3", "gcs", "azblob", "filesystem")
    #[serde(default)]
    pub backend: String,
    /// Root path (shared_fs) or bucket/container URI (object_store)
    #[serde(default)]
    pub root: String,
    /// Additional OpenDAL backend options (e.g. region, endpoint)
    #[serde(default)]
    pub options: std::collections::HashMap<String, String>,
    /// Allow replacing active durable storage when cloud-tiered topics exist
    #[serde(default)]
    pub force: bool,
}

pub async fn configure_cluster_storage(
    client: &Arc<AdminGrpcClient>,
    params: ConfigureClusterStorageParams,
) -> String {
    let req = danube_core::admin_proto::ConfigureClusterStorageRequest {
        mode: params.mode,
        backend: params.backend,
        root: params.root,
        options: params.options,
        force: params.force,
    };
    match client.configure_cluster_storage(req).await {
        Ok(resp) => resp.message,
        Err(e) => format!("Error configuring cluster storage: {}", e),
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConfigureWriteBufferParams {
    /// Enable or disable the Valkey warm write buffer
    pub enabled: bool,
    /// Valkey/Redis endpoints (required when enabled=true unless already configured)
    #[serde(default)]
    pub endpoints: Vec<String>,
    /// Optional profile preset: "ha" or "dev"
    #[serde(default)]
    pub profile: Option<String>,
    /// Override WAIT replica count
    #[serde(default)]
    pub wait_replicas: Option<u32>,
    /// Override WAIT timeout in milliseconds
    #[serde(default)]
    pub wait_timeout_ms: Option<u64>,
    /// Timeout policy: "fail" or "ack"
    #[serde(default)]
    pub on_wait_timeout: Option<String>,
    /// Max closed WAL segments cached in Valkey
    #[serde(default)]
    pub max_cached_closed_segments: Option<u32>,
}

pub async fn configure_write_buffer(
    client: &Arc<AdminGrpcClient>,
    params: ConfigureWriteBufferParams,
) -> String {
    let req = danube_core::admin_proto::ConfigureWriteBufferRequest {
        enabled: params.enabled,
        endpoints: params.endpoints,
        profile: params.profile,
        wait_replicas: params.wait_replicas,
        wait_timeout_ms: params.wait_timeout_ms,
        on_wait_timeout: params.on_wait_timeout,
        max_cached_closed_segments: params.max_cached_closed_segments,
    };
    match client.configure_write_buffer(req).await {
        Ok(resp) => resp.message,
        Err(e) => format!("Error configuring write buffer: {}", e),
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct ConfigureWalRetentionParams {
    #[serde(default)]
    pub retention_time_minutes: Option<u64>,
    #[serde(default)]
    pub retention_size_mb: Option<u64>,
    #[serde(default)]
    pub check_interval_minutes: Option<u64>,
    #[serde(default)]
    pub rotate_max_bytes: Option<u64>,
    #[serde(default)]
    pub rotate_max_hours: Option<u64>,
    #[serde(default)]
    pub cache_capacity: Option<u64>,
    #[serde(default)]
    pub flush_interval_ms: Option<u64>,
    #[serde(default)]
    pub flush_max_batch_bytes: Option<u64>,
    #[serde(default)]
    pub clear_retention: bool,
}

pub async fn configure_wal_retention(
    client: &Arc<AdminGrpcClient>,
    params: ConfigureWalRetentionParams,
) -> String {
    let req = danube_core::admin_proto::ConfigureWalRetentionRequest {
        retention_time_minutes: params.retention_time_minutes,
        retention_size_mb: params.retention_size_mb,
        check_interval_minutes: params.check_interval_minutes,
        rotate_max_bytes: params.rotate_max_bytes,
        rotate_max_hours: params.rotate_max_hours,
        cache_capacity: params.cache_capacity,
        flush_interval_ms: params.flush_interval_ms,
        flush_max_batch_bytes: params.flush_max_batch_bytes,
        clear_retention: params.clear_retention,
    };
    match client.configure_wal_retention(req).await {
        Ok(resp) => resp.message,
        Err(e) => format!("Error configuring WAL and retention: {}", e),
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct ConfigureLoadManagerParams {
    /// Placement strategy: "fair", "balanced", or "weighted_load"
    #[serde(default)]
    pub assignment_strategy: Option<String>,
    /// Broker load report interval in seconds
    #[serde(default)]
    pub load_report_interval_seconds: Option<u64>,
    /// Enable or disable automated rebalancing
    #[serde(default)]
    pub rebalancing_enabled: Option<bool>,
    /// Rebalancing profile preset: "conservative", "balanced", or "aggressive"
    #[serde(default)]
    pub rebalancing_profile: Option<String>,
    #[serde(default)]
    pub check_interval_seconds: Option<u64>,
    #[serde(default)]
    pub max_moves_per_hour: Option<u32>,
    #[serde(default)]
    pub cooldown_seconds: Option<u64>,
    #[serde(default)]
    pub min_brokers_for_rebalance: Option<u32>,
    #[serde(default)]
    pub min_topic_age_seconds: Option<u64>,
    #[serde(default)]
    pub blacklist_topics: Vec<String>,
    #[serde(default)]
    pub replace_blacklist: bool,
}

pub async fn configure_load_manager(
    client: &Arc<AdminGrpcClient>,
    params: ConfigureLoadManagerParams,
) -> String {
    let req = danube_core::admin_proto::ConfigureLoadManagerRequest {
        assignment_strategy: params.assignment_strategy,
        load_report_interval_seconds: params.load_report_interval_seconds,
        rebalancing_enabled: params.rebalancing_enabled,
        rebalancing_profile: params.rebalancing_profile,
        check_interval_seconds: params.check_interval_seconds,
        max_moves_per_hour: params.max_moves_per_hour,
        cooldown_seconds: params.cooldown_seconds,
        min_brokers_for_rebalance: params.min_brokers_for_rebalance,
        min_topic_age_seconds: params.min_topic_age_seconds,
        blacklist_topics: params.blacklist_topics,
        replace_blacklist: params.replace_blacklist,
    };
    match client.configure_load_manager(req).await {
        Ok(resp) => resp.message,
        Err(e) => format!("Error configuring LoadManager: {}", e),
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct ExportClusterConfigParams {
    /// Optional broker ID to include node bind/admin addresses
    #[serde(default)]
    pub broker_id: Option<u64>,
    /// Output format: "yaml" (default) or "json"
    #[serde(default)]
    pub format: Option<String>,
}

pub async fn export_cluster_config(
    client: &Arc<AdminGrpcClient>,
    params: ExportClusterConfigParams,
) -> String {
    let req = danube_core::admin_proto::ExportClusterConfigRequest {
        broker_id: params.broker_id,
        format: params.format.unwrap_or_else(|| "yaml".to_string()),
    };
    match client.export_cluster_config(req).await {
        Ok(resp) => resp.content,
        Err(e) => format!("Error exporting cluster config: {}", e),
    }
}
