//! Diagnostic and health check tools

use crate::core::AdminGrpcClient;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ConsumerLagParams {
    /// Topic to diagnose
    pub topic: String,
    /// Subscription name
    pub subscription: String,
}

pub async fn diagnose_consumer_lag(
    client: &Arc<AdminGrpcClient>,
    params: ConsumerLagParams,
) -> String {
    let mut output = format!(
        "Diagnosing consumer lag for:\n\
         Topic: {}\n\
         Subscription: {}\n\n",
        params.topic, params.subscription
    );

    // 1. Check topic details
    let topic_req = danube_core::admin_proto::DescribeTopicRequest {
        name: params.topic.clone(),
    };

    match client.describe_topic(topic_req).await {
        Ok(topic_info) => {
            output.push_str(&format!(
                "Topic Info:\n\
                 - Broker: {}\n\
                 - Delivery: {}\n\
                 - Subscriptions: {}\n\n",
                topic_info.broker_id,
                topic_info.delivery,
                topic_info.subscriptions.len()
            ));

            // Check if subscription exists
            if !topic_info.subscriptions.contains(&params.subscription) {
                output.push_str("⚠ WARNING: Subscription not found on this topic!\n");
                output.push_str("This could explain why no messages are being consumed.\n\n");
            }
        }
        Err(e) => {
            output.push_str(&format!("✗ Error getting topic info: {}\n\n", e));
        }
    }

    // 2. Check cluster balance
    let balance_req = danube_core::admin_proto::ClusterBalanceRequest {};
    match client.get_cluster_balance(balance_req).await {
        Ok(balance) => {
            output.push_str("Cluster Balance:\n");
            output.push_str(&format!(
                "- Coefficient of Variation: {:.4}\n",
                balance.coefficient_of_variation
            ));

            if balance.coefficient_of_variation > 0.4 {
                output.push_str("⚠ Cluster is severely imbalanced - this may affect performance\n");
            } else if balance.coefficient_of_variation > 0.3 {
                output.push_str("⚠ Cluster is imbalanced - consider rebalancing\n");
            }
            output.push_str("\n");
        }
        Err(_) => {}
    }

    output.push_str("Possible Causes of Consumer Lag:\n");
    output.push_str("1. Consumer processing is too slow (check consumer application logs)\n");
    output.push_str("2. Not enough consumers (check if subscription is 'shared' type)\n");
    output.push_str("3. Broker overload (check cluster balance metrics)\n");
    output.push_str("4. Network issues between consumer and broker\n");
    output.push_str("5. Consumer group rebalancing (temporary lag spikes)\n\n");

    output.push_str("Recommended Actions:\n");
    output.push_str("1. Run 'get_cluster_balance' to check broker load\n");
    output.push_str("2. Check if topic has multiple partitions for parallelism\n");
    output.push_str("3. Scale consumer group if using 'shared' subscription\n");
    output.push_str("4. Monitor broker metrics for throughput limits\n");

    output
}

pub async fn health_check(client: &Arc<AdminGrpcClient>) -> String {
    let mut output = String::from("Danube Cluster Health Check:\n\n");
    let mut issues = Vec::new();

    // 1. Check brokers
    match client.list_brokers().await {
        Ok(brokers_resp) => {
            output.push_str(&format!(
                "✓ Brokers: {} active\n",
                brokers_resp.brokers.len()
            ));

            if brokers_resp.brokers.is_empty() {
                issues.push("No brokers found - cluster is DOWN".to_string());
            }

            for broker in &brokers_resp.brokers {
                if broker.broker_status != "active" {
                    issues.push(format!(
                        "Broker {} is not active: {}",
                        broker.broker_id, broker.broker_status
                    ));
                }
            }
        }
        Err(e) => {
            output.push_str("✗ Brokers: UNREACHABLE\n");
            issues.push(format!("Cannot connect to cluster: {}", e));
        }
    }

    // 2. Check leader
    match client.get_leader().await {
        Ok(leader) => {
            output.push_str(&format!("✓ Leader: {}\n", leader.leader));
        }
        Err(_) => {
            output.push_str("✗ Leader: UNKNOWN\n");
            issues.push("No leader elected - cluster may be in election".to_string());
        }
    }

    // 3. Check cluster balance
    match client
        .get_cluster_balance(danube_core::admin_proto::ClusterBalanceRequest {})
        .await
    {
        Ok(balance) => {
            let status = if balance.coefficient_of_variation < 0.2 {
                "✓ Well Balanced"
            } else if balance.coefficient_of_variation < 0.3 {
                "✓ Balanced"
            } else if balance.coefficient_of_variation < 0.4 {
                "⚠ Imbalanced"
            } else {
                "✗ Severely Imbalanced"
            };

            output.push_str(&format!(
                "{} (CoV: {:.4})\n",
                status, balance.coefficient_of_variation
            ));

            if balance.coefficient_of_variation > 0.4 {
                issues.push("Cluster is severely imbalanced - rebalancing recommended".to_string());
            } else if balance.coefficient_of_variation > 0.3 {
                issues.push("Cluster is imbalanced - consider rebalancing".to_string());
            }
        }
        Err(_) => {
            output.push_str("⚠ Balance: Unable to check\n");
        }
    }

    // 4. Check namespaces
    match client.list_namespaces().await {
        Ok(ns_resp) => {
            output.push_str(&format!("✓ Namespaces: {}\n", ns_resp.namespaces.len()));
        }
        Err(_) => {
            output.push_str("⚠ Namespaces: Unable to list\n");
        }
    }

    output.push_str("\n");

    if issues.is_empty() {
        output.push_str("Overall Status: ✓ HEALTHY\n");
    } else {
        output.push_str("Overall Status: ⚠ ISSUES DETECTED\n\n");
        output.push_str("Issues:\n");
        for (i, issue) in issues.iter().enumerate() {
            output.push_str(&format!("  {}. {}\n", i + 1, issue));
        }
    }

    output
}

pub async fn get_recommendations(client: &Arc<AdminGrpcClient>) -> String {
    let mut recommendations: Vec<(String, String, String)> = Vec::new();

    // Check cluster balance
    if let Ok(balance) = client
        .get_cluster_balance(danube_core::admin_proto::ClusterBalanceRequest {})
        .await
    {
        if balance.coefficient_of_variation > 0.3 {
            recommendations.push((
                "High Priority".to_string(),
                "Cluster Load Imbalance".to_string(),
                format!(
                    "Coefficient of variation is {:.2}. Run 'trigger_rebalance' with dry_run=true to preview rebalancing.",
                    balance.coefficient_of_variation
                )
            ));
        }

        // Check for overloaded brokers
        for broker in &balance.brokers {
            if broker.is_overloaded {
                recommendations.push((
                    "High Priority".to_string(),
                    format!("Broker {} Overloaded", broker.broker_id),
                    format!(
                        "Broker has load of {:.2} with {} topics. Consider unloading some topics or adding more brokers.",
                        broker.load,
                        broker.topic_count
                    )
                ));
            }
        }
    }

    // Check broker count
    if let Ok(brokers) = client.list_brokers().await {
        if brokers.brokers.len() < 3 {
            recommendations.push((
                "Medium Priority".to_string(),
                "Low Broker Count".to_string(),
                format!(
                    "Only {} broker(s) running. For high availability, consider running at least 3 brokers.",
                    brokers.brokers.len()
                )
            ));
        }
    }

    let mut output = String::from("Cluster Optimization Recommendations:\n\n");

    if recommendations.is_empty() {
        output.push_str("✓ No optimization recommendations at this time.\n");
        output.push_str("Your cluster appears to be running optimally.\n");
    } else {
        for (i, (priority, category, recommendation)) in recommendations.iter().enumerate() {
            output.push_str(&format!(
                "{}. [{}] {}\n   {}\n\n",
                i + 1,
                priority,
                category,
                recommendation
            ));
        }
    }

    output
}
