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
    /// Perform a dry run without actually moving topics
    #[serde(default)]
    pub dry_run: bool,
    /// Maximum number of topic moves to execute
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
            } else {
                output.push_str("Rebalancing completed successfully!\n\n");
            }

            output.push_str(&format!(
                "Moves executed: {}\n\
                 Proposed moves: {}\n\n",
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
            let is_balanced = response.coefficient_of_variation < 0.2;

            let mut output = String::new();
            output.push_str("Cluster Balance Metrics:\n\n");
            output.push_str(&format!(
                "Status: {}\n\
                 Coefficient of Variation: {:.4}\n\
                 Mean Load: {:.2}\n\
                 Max Load: {:.2}\n\
                 Min Load: {:.2}\n\
                 Std Deviation: {:.2}\n\
                 Active Brokers: {}\n\n",
                if is_balanced {
                    "✓ Balanced"
                } else {
                    "⚠ Imbalanced"
                },
                response.coefficient_of_variation,
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

            if !is_balanced {
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
    /// Broker ID to unload
    pub broker_id: String,
    /// Maximum parallel unloads
    #[serde(default = "default_max_parallel")]
    pub max_parallel: u32,
    /// Perform dry run
    #[serde(default)]
    pub dry_run: bool,
}

fn default_max_parallel() -> u32 {
    1
}

pub async fn unload_broker(client: &Arc<AdminGrpcClient>, params: UnloadBrokerParams) -> String {
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
    /// Broker ID to activate
    pub broker_id: String,
    /// Reason for activation (for audit trail)
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
    /// Namespace name
    pub namespace: String,
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
