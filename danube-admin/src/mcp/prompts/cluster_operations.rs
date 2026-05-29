//! Cluster operational workflow prompts
//!
//! Contains the cluster rebalance prompt — a safety-gated workflow
//! for redistributing topics with dry-run validation.

use rmcp::model::{
    GetPromptRequestParams, GetPromptResult, Prompt, PromptArgument, PromptMessage,
    PromptMessageRole,
};

/// Get cluster operational prompt definitions
pub fn prompts() -> Vec<Prompt> {
    vec![Prompt::new(
        "prepare_cluster_rebalance",
        Some("Safe workflow for rebalancing cluster load with dry-run validation and post-rebalance verification"),
        Some(vec![
            PromptArgument::new("max_moves")
                .with_description("Maximum number of topic moves to execute (optional, default: from config)")
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

            Some(
                GetPromptResult::new(vec![PromptMessage::new_text(
                    PromptMessageRole::User,
                    build_rebalance_prompt(max_moves),
                )])
                .with_description("Prepare and execute cluster rebalance"),
            )
        }
        _ => None,
    }
}

fn build_rebalance_prompt(max_moves: Option<&str>) -> String {
    let max_moves_note = max_moves
        .map(|m| format!(" (limited to {} moves)", m))
        .unwrap_or_default();

    let max_moves_param = max_moves
        .map(|m| format!("\n- max_moves: {}", m))
        .unwrap_or_default();

    format!(
        r#"I need to rebalance the cluster load{max_moves_note}.

Please follow this safety workflow — each step has a gate condition:

## Step 1: Quorum Safety Check
Use `cluster_status` to verify:
- A leader is elected (leader_id ≠ 0)
- All expected voters are present
- Quorum tolerance: 3 voters = can lose 1, 5 voters = can lose 2

**ABORT if quorum is at risk (e.g., only 2 voters in a 3-node cluster).**

## Step 2: Pre-Rebalance Health Check
Use `health_check` to verify:
- All brokers active
- No critical issues
- No ongoing maintenance

**ABORT if health check shows critical issues.**

## Step 3: Check Current Balance
Use `get_cluster_balance` to assess:
- Coefficient of Variation (CV) — the primary imbalance metric
- Per-broker topic counts and load scores

| CV Range | Status | Action |
|----------|--------|--------|
| < 20%    | Well balanced | Rebalancing not needed |
| 20-30%   | Acceptable | Monitor, rebalance optional |
| 30-40%   | Imbalanced | Rebalance recommended |
| > 40%    | Severely imbalanced | Rebalance strongly recommended |

**Only proceed if CV > 30%** or a specific broker is significantly overloaded.

## Step 4: Dry Run (CRITICAL — never skip)
Use `trigger_rebalance` with:
- dry_run: true{max_moves_param}

This shows which topics will move WITHOUT making any changes.

## Step 5: Review Proposed Moves
Analyze the dry run output:
- How many topics will move?
- Which brokers are source (overloaded) vs destination (underloaded)?
- Is the redistribution reasonable?

Note: The load manager moves one topic per cycle to prevent overshooting.
Topics younger than 5 minutes and blacklisted topics are excluded automatically.

**Present the plan and ask for explicit approval before proceeding.**

## Step 6: Execute Rebalance (only if approved)
Use `trigger_rebalance` with:
- dry_run: false{max_moves_param}

Monitor the output for any errors.

## Step 7: Post-Rebalance Verification
After rebalance completes:
1. `get_cluster_balance` — verify CV decreased
2. `get_cluster_metrics` — confirm message rates are stable
3. `health_check` — ensure no new issues

## Safety Notes
- **Graceful**: topic moves use seal → drain → unload, no message loss or downtime
- **Gradual**: use max_moves to limit scope (e.g., max_moves=5 for a cautious first run)
- **Cooldown**: topics that were just moved cannot be moved again for 60 seconds
- **New topics**: rebalancing only redistributes existing topics; new topics are automatically assigned to the least-loaded broker
- **Timing**: best during low-traffic periods; monitor for 15-30 minutes post-rebalance"#,
    )
}
