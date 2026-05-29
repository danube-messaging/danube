//! Troubleshooting prompts for Danube
//!
//! Contains the cluster health check prompt — a comprehensive,
//! multi-step assessment workflow encoding Raft, load-balancing,
//! and security domain knowledge.

use rmcp::model::{
    GetPromptRequestParams, GetPromptResult, Prompt, PromptMessage, PromptMessageRole,
};

/// Get all troubleshooting prompt definitions
pub fn prompts() -> Vec<Prompt> {
    vec![
        Prompt::new(
            "cluster_health_check",
            Some("Comprehensive cluster health assessment covering Raft consensus, load balance, broker status, and security configuration"),
            None::<Vec<rmcp::model::PromptArgument>>,
        )
        .with_title("Cluster Health Check"),
    ]
}

/// Try to get a troubleshooting prompt by name
pub fn get_prompt(params: &GetPromptRequestParams) -> Option<GetPromptResult> {
    match params.name.as_str() {
        "cluster_health_check" => Some(
            GetPromptResult::new(vec![PromptMessage::new_text(
                PromptMessageRole::User,
                build_cluster_health_prompt(),
            )])
            .with_description("Comprehensive cluster health check"),
        ),
        _ => None,
    }
}

fn build_cluster_health_prompt() -> String {
    r#"I need a comprehensive health check of the Danube cluster.

Please follow these steps in order — each step depends on the previous:

## Step 1: Raft Consensus Health (Critical)
Use `cluster_status` to verify:
- **Leader elected**: `leader_id` must not be 0
- **Voter count**: all expected brokers should be voters
- **Quorum safety**: check fault tolerance (3 voters = can lose 1, 5 voters = can lose 2)
- **Learner set**: should be empty unless a scale-up is in progress
- **Term stability**: a rapidly incrementing term indicates election storms (split-brain risk)

**ALERT if no leader, voter count is wrong, or learners are unexpected.**

## Step 2: Broker Health
Use `list_brokers` and check:
- All brokers are online and in "active" status
- No brokers stuck in "drained" state unexpectedly
- Leader/follower roles are assigned correctly

**ALERT if any broker is missing or in an unexpected state.**

## Step 3: Load Distribution
Use `get_cluster_balance` to assess topic distribution:
- **CV < 20%**: well balanced — no action needed
- **CV 20-30%**: acceptable — monitor
- **CV 30-40%**: imbalanced — consider `prepare_cluster_rebalance` prompt
- **CV > 40%**: severely imbalanced — rebalance recommended

Check per-broker load for overloaded/underloaded outliers.

## Step 4: Cluster-Wide Metrics
Use `get_cluster_metrics` for the overall picture:
- Total topics, producers, consumers
- Cluster-wide message rates
- Any anomalies in throughput

## Step 5: Automated Diagnostics
Use `health_check` for automated issue detection:
- Connectivity problems
- Resource constraints
- Configuration issues

## Step 6: Security Configuration (if applicable)
Use `list_roles` and `list_bindings` (scope="cluster") to verify:
- Roles are defined (at minimum: producer, consumer, admin roles)
- Bindings exist connecting service accounts to roles
- No overly broad cluster-scoped bindings that should be namespace-scoped

Skip this step if the cluster runs in `auth.mode: none`.

## Step 7: Actionable Recommendations
Use `get_recommendations` for AI-generated suggestions on:
- Rebalancing needs
- Capacity planning
- Configuration optimizations

## Step 8: Summary Report
Provide a structured summary:
- **Overall health**: HEALTHY / WARNING / CRITICAL
- **Raft consensus**: leader ID, term, voter count, quorum tolerance
- **Broker status**: active/total, any issues
- **Load balance**: CV percentage and interpretation
- **Security**: roles and bindings configured (yes/no)
- **Top 3 action items** (if any issues found)"#
        .to_string()
}
