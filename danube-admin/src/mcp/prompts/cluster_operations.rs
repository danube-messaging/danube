//! Cluster operational workflow prompts
//!
//! Contains prompts for cluster rebalancing and management.

use rmcp::model::{
    GetPromptRequestParams, GetPromptResult, Prompt, PromptArgument, PromptMessage,
    PromptMessageRole,
};

/// Get cluster operational prompt definitions
pub fn prompts() -> Vec<Prompt> {
    vec![Prompt::new(
        "prepare_cluster_rebalance",
        Some("Safe workflow for rebalancing cluster load with dry-run validation"),
        Some(vec![
            PromptArgument::new("max_moves")
                .with_description("Maximum number of topic moves to execute (optional)")
                .with_required(false),
            PromptArgument::new("target_broker")
                .with_description("Specific broker to rebalance (optional, rebalances all if omitted)")
                .with_required(false),
        ]),
    )
    .with_title("Prepare Cluster Rebalance")]
}

/// Try to get a cluster operational prompt by name
pub fn get_prompt(params: &GetPromptRequestParams) -> Option<GetPromptResult> {
    let args = params.arguments.as_ref();

    match params.name.as_str() {
        "prepare_cluster_rebalance" => {
            let max_moves = args
                .and_then(|a| a.get("max_moves"))
                .and_then(|v| v.as_str());
            let target_broker = args
                .and_then(|a| a.get("target_broker"))
                .and_then(|v| v.as_str());

            Some(GetPromptResult::new(vec![PromptMessage::new_text(
                PromptMessageRole::User,
                build_rebalance_prompt(max_moves, target_broker),
            )])
            .with_description("Prepare and execute cluster rebalance"))
        }
        _ => None,
    }
}

fn build_rebalance_prompt(max_moves: Option<&str>, target_broker: Option<&str>) -> String {
    let max_moves_note = max_moves
        .map(|m| format!(" (limited to {} moves)", m))
        .unwrap_or_default();

    let target_note = target_broker
        .map(|b| format!("\n**Target Broker**: Focusing on broker {}", b))
        .unwrap_or_else(|| "\n**Scope**: Rebalancing entire cluster".to_string());

    format!(
        r#"I need to rebalance the cluster load{}.{}

Please follow this safety workflow:

## Step 1: Pre-Rebalance Health Check
Use `health_check` to verify cluster is healthy:
- All brokers active
- No critical issues
- Leader election stable

**ABORT if health check shows critical issues.**

## Step 2: Check Current Balance
Use `get_cluster_balance` to see:
- Coefficient of Variation (CV)
- Topic distribution across brokers
- Recommended action

**Only proceed if CV > 0.3** (imbalanced) or specific broker is overloaded.

## Step 3: Dry Run First (CRITICAL)
Use `trigger_rebalance` with:
- dry_run: true{}

This shows which topics will move WITHOUT executing changes.

## Step 4: Review Proposed Moves
Analyze the dry run output:
- How many topics will move?
- Which brokers are source/destination?
- Is the redistribution reasonable?

**Ask for approval before proceeding.**

## Step 5: Execute Rebalance (if approved)
Use `trigger_rebalance` with:
- dry_run: false{}

Monitor the output for any errors.

## Step 6: Monitor Progress
After rebalance, use:
- `get_cluster_balance` - Verify CV improved
- `get_cluster_metrics` - Check message rates stable
- `health_check` - Ensure no new issues

## Step 7: Verify Success
Confirm:
- CV decreased (better balance)
- All topics accessible
- No message loss
- Producers/consumers still functioning

## Safety Notes
- **Graceful**: Rebalancing moves topics without downtime or message loss
- **Gradual**: Use max_moves to limit scope (e.g., max_moves=5 for cautious approach)
- **Reversible**: Can rebalance again if needed
- **Timing**: Best during low-traffic periods
- **Monitoring**: Watch metrics for 15-30 minutes post-rebalance"#,
        max_moves_note,
        target_note,
        target_broker
            .map(|b| format!("\n- target_broker: {}", b))
            .unwrap_or_default(),
        max_moves
            .map(|m| format!("\n- max_moves: {}", m))
            .unwrap_or_default()
    )
}
