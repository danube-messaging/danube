use anyhow::{anyhow, Result};
use danube_metadata_store::{MetaOptions, MetadataStorage, MetadataStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::danube_service::leader_election::LeaderElection;
use crate::danube_service::load_report::LoadReport;
use crate::resources::{BASE_BROKER_PATH, BASE_UNASSIGNED_PATH};
use crate::utils::join_path;

use super::config;

/// Represents a single topic rebalancing move
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingMove {
    /// Topic to be moved
    pub topic_name: String,
    /// Source broker ID
    pub from_broker: u64,
    /// Destination broker ID
    pub to_broker: u64,
    /// Reason for the move
    pub reason: RebalancingReason,
    /// Estimated load score of the topic
    pub estimated_load: f64,
    /// Timestamp when move was initiated (Unix seconds)
    pub timestamp: u64,
}

impl RebalancingMove {
    /// Creates a new rebalancing move
    pub fn new(
        topic_name: String,
        from_broker: u64,
        to_broker: u64,
        reason: RebalancingReason,
        estimated_load: f64,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            topic_name,
            from_broker,
            to_broker,
            reason,
            estimated_load,
            timestamp,
        }
    }
}

/// Reason for rebalancing a topic
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RebalancingReason {
    /// Cluster-wide load imbalance detected
    LoadImbalance,
    /// Specific broker is overloaded
    BrokerOverload,
    /// Broker is under-utilized, spreading load
    BrokerUnderUtilized,
    /// Admin-triggered manual rebalancing
    ManualRebalance,
}

/// Tracks rebalancing history for rate limiting and audit trail
pub(super) struct RebalancingHistory {
    /// Ring buffer of recent moves
    moves: VecDeque<RebalancingMove>,
    /// Maximum number of moves to keep in history
    max_size: usize,
}

impl RebalancingHistory {
    /// Creates a new rebalancing history tracker
    pub fn new(max_size: usize) -> Self {
        Self {
            moves: VecDeque::with_capacity(max_size),
            max_size,
        }
    }

    /// Records a new rebalancing move
    pub fn record_move(&mut self, mv: RebalancingMove) {
        if self.moves.len() >= self.max_size {
            self.moves.pop_front();
        }
        self.moves.push_back(mv);
    }

    /// Counts moves that happened in the last hour
    pub fn count_moves_in_last_hour(&self) -> usize {
        let one_hour_ago = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(3600);

        self.moves
            .iter()
            .filter(|m| m.timestamp >= one_hour_ago)
            .count()
    }

    /// Get the N most recent rebalancing moves
    /// Reserved for Step 9 (Admin CLI - show rebalancing history)
    #[allow(dead_code)]
    pub fn get_recent_moves(&self, limit: usize) -> Vec<&RebalancingMove> {
        self.moves.iter().rev().take(limit).collect()
    }

    /// Checks if a topic was moved recently (within cooldown period)
    /// This prevents oscillations by filtering out recently moved topics
    pub fn was_topic_recently_moved(&self, topic_name: &str, cooldown_seconds: u64) -> bool {
        let cooldown_threshold = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(cooldown_seconds);

        self.moves
            .iter()
            .any(|m| m.topic_name == topic_name && m.timestamp >= cooldown_threshold)
    }

    /// Returns total number of moves in history
    /// Reserved for Step 9 (Admin CLI - rebalancing stats)
    #[allow(dead_code)]
    pub fn total_moves(&self) -> usize {
        self.moves.len()
    }

    /// Clear all history (useful for testing or manual resets)
    /// Reserved for Step 9 (Admin CLI - clear rebalancing history)
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.moves.clear();
    }
}

/// Statistical metrics about cluster load imbalance
#[derive(Debug, Clone)]
pub struct ImbalanceMetrics {
    /// Coefficient of Variation (std_deviation / mean)
    /// Lower = more balanced, Higher = more imbalanced
    pub coefficient_of_variation: f64,
    /// Mean load score across all brokers
    pub mean_load: f64,
    /// Maximum load score in the cluster
    pub max_load: f64,
    /// Minimum load in the cluster
    pub min_load: f64,
    /// Standard deviation of load scores
    /// Used in Prometheus metrics and Admin CLI
    pub std_deviation: f64,
    /// Broker IDs with load > mean + 1 std_dev (overloaded)
    pub overloaded_brokers: Vec<u64>,
    /// Broker IDs with load < mean - 1 std_dev (underloaded)
    pub underloaded_brokers: Vec<u64>,
}

impl ImbalanceMetrics {
    /// Checks if cluster is balanced based on threshold
    /// Returns true if CV is below the threshold (cluster is balanced)
    pub fn is_balanced(&self, threshold: f64) -> bool {
        self.coefficient_of_variation < threshold
    }

    /// Returns true if rebalancing is needed (CV >= threshold)
    pub fn needs_rebalancing(&self, threshold: f64) -> bool {
        !self.is_balanced(threshold)
    }

    /// Returns true if there are any overloaded brokers
    /// Reserved for Step 8 (Prometheus metrics) and Step 9 (CLI status)
    #[allow(dead_code)]
    pub fn has_overloaded_brokers(&self) -> bool {
        !self.overloaded_brokers.is_empty()
    }

    /// Checks if there are any underloaded brokers
    /// Reserved for Step 8 (Prometheus metrics) and Step 9 (CLI status)
    #[allow(dead_code)]
    pub fn has_underloaded_brokers(&self) -> bool {
        !self.underloaded_brokers.is_empty()
    }
}

/// Starts the automated rebalancing background loop
///
/// ## Purpose:
/// Orchestrates the entire rebalancing process in a continuous background task.
/// Combines Steps 3-6 into an automated loop that periodically checks cluster
/// balance and initiates rebalancing when needed.
///
/// ## Leader-Only Execution:
/// Only the elected leader broker runs the rebalancing logic to prevent conflicts.
/// Non-leader brokers skip the rebalancing cycle entirely.
///
/// ## Loop Flow:
/// 1. Wait for configured check interval (default: 300s)
/// 2. Check if current broker is leader → skip if not
/// 3. Check if rebalancing is enabled → skip if not
/// 4. Check minimum broker count → skip if insufficient
/// 5. Calculate cluster imbalance metrics (Step 3)
/// 6. Decide if rebalancing needed (Step 3)
/// 7. Select topics to move (Step 4)
/// 8. Execute moves (Step 5 + Step 6)
/// 9. Loop continues forever
///
/// ## Safety Features:
/// - **Leader-only**: Prevents simultaneous rebalancing by multiple brokers
/// - **Interval pacing**: Allows cluster to stabilize between checks
/// - **History tracking**: Enforces hourly rate limits
/// - **Error handling**: Logs errors but continues loop
/// - **Minimum brokers**: Requires at least 2 active brokers
///
/// ## Parameters:
/// - `config`: Rebalancing configuration (intervals, thresholds, limits)
/// - `leader_election`: Leader election service for state checking
///
/// ## Returns:
/// JoinHandle for the background task (can be used to await or abort)
pub(super) fn start_rebalancing_loop<F>(
    rankings: Arc<Mutex<Vec<(u64, usize)>>>,
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
    meta_store: MetadataStorage,
    config: config::RebalancingConfig,
    leader_election: LeaderElection,
    is_broker_active: F,
) -> JoinHandle<()>
where
    F: Fn(u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
        + Send
        + Sync
        + 'static,
{
    tokio::spawn(async move {
        info!(
            check_interval_seconds = config.check_interval_seconds,
            aggressiveness = ?config.aggressiveness,
            max_moves_per_hour = config.max_moves_per_hour,
            "starting automated rebalancing loop (moves 1 topic per cycle)"
        );

        let mut interval =
            tokio::time::interval(Duration::from_secs(config.check_interval_seconds));

        let mut history = RebalancingHistory::new(1000);

        loop {
            interval.tick().await;

            // Check 1: Only leader performs rebalancing
            let leader_state = leader_election.get_state().await;
            if leader_state != crate::danube_service::leader_election::LeaderElectionState::Leading
            {
                continue;
            }

            // Check 2: Rebalancing must be enabled
            if !config.enabled {
                continue;
            }

            // Check 3: Cluster health - need minimum broker count
            let rankings_clone = rankings.lock().await.clone();
            if rankings_clone.len() < config.min_brokers_for_rebalance {
                continue;
            }

            // Step 3: Calculate imbalance metrics
            let metrics = match calculate_imbalance(rankings.clone(), &is_broker_active).await {
                Ok(m) => m,
                Err(e) => {
                    error!(error = %e, "failed to calculate imbalance metrics");
                    continue;
                }
            };

            // Step 3: Decide if rebalancing is needed
            if !should_rebalance(&metrics, &config) {
                continue;
            }

            info!(
                cv = %metrics.coefficient_of_variation,
                threshold = config.aggressiveness.threshold(),
                overloaded_brokers = ?metrics.overloaded_brokers,
                underloaded_brokers = ?metrics.underloaded_brokers,
                "cluster imbalance detected, initiating rebalancing"
            );

            // Start timing the rebalancing cycle
            let cycle_start = std::time::Instant::now();

            // Step 4: Select one topic to move
            let rebalancing_move = match select_rebalancing_candidate(
                rankings.clone(),
                brokers_usage.clone(),
                &metrics,
                &config,
                &is_broker_active,
                &history,
            )
            .await
            {
                Ok(m) => m,
                Err(e) => {
                    error!(error = %e, "failed to select rebalancing candidate");
                    continue;
                }
            };

            // If no suitable topic found, skip this cycle
            let rebalancing_move = match rebalancing_move {
                Some(mv) => mv,
                None => {
                    warn!(
                        overloaded_count = metrics.overloaded_brokers.len(),
                        overloaded_brokers = ?metrics.overloaded_brokers,
                        "no suitable topic found for rebalancing despite cluster imbalance"
                    );
                    continue;
                }
            };

            info!(
                topic = %rebalancing_move.topic_name,
                from_broker = %rebalancing_move.from_broker,
                to_broker = %rebalancing_move.to_broker,
                "selected topic for rebalancing"
            );

            // Step 5 + Step 6: Execute the single rebalancing move
            // execute_rebalancing expects Vec, so wrap in a vec with 1 element
            match execute_rebalancing(&meta_store, vec![rebalancing_move], &config, &mut history)
                .await
            {
                Ok(executed) => {
                    // Record cycle duration
                    let cycle_duration = cycle_start.elapsed().as_secs_f64();
                    metrics::histogram!(
                        crate::broker_metrics::REBALANCING_CYCLE_DURATION_SECONDS.name
                    )
                    .record(cycle_duration);

                    info!(
                        executed = executed,
                        total_moves_in_last_hour = history.count_moves_in_last_hour(),
                        cycle_duration_secs = %cycle_duration,
                        "rebalancing cycle completed successfully"
                    );
                }
                Err(e) => {
                    error!(error = %e, "rebalancing execution failed");
                }
            }
        }
    })
}

// ============================================================================
// Rebalancing Logic Functions
// ============================================================================

/// Calculates cluster imbalance metrics for rebalancing decisions
///
/// ## Purpose:
/// Computes statistical measures of cluster load distribution to determine
/// if automated rebalancing is needed. Uses coefficient of variation (CV)
/// as the primary metric for cluster balance.
///
/// ## Returns:
/// `ImbalanceMetrics` containing:
/// - **Coefficient of Variation (CV)**: std_dev / mean (key metric)
/// - **Statistical measures**: mean, std_dev, min, max loads
/// - **Problem brokers**: Overloaded (> mean + 1σ) and underloaded (< mean - 1σ)
///
/// ## CV Interpretation:
/// - CV < 0.20: Well balanced (aggressive threshold)
/// - CV < 0.30: Reasonably balanced (balanced threshold)
/// - CV < 0.40: Acceptable (conservative threshold)
/// - CV ≥ threshold: Needs rebalancing
pub(super) async fn calculate_imbalance<F, Fut>(
    rankings: Arc<Mutex<Vec<(u64, usize)>>>,
    is_broker_active: F,
) -> Result<ImbalanceMetrics>
where
    F: Fn(u64) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let rankings = rankings.lock().await;

    // Filter to only active brokers for rebalancing calculations
    // Non-active brokers (draining/drained) should not affect metrics
    let mut active_rankings = Vec::new();
    let mut inactive_brokers = Vec::new();
    for (broker_id, load) in rankings.iter() {
        if is_broker_active(*broker_id).await {
            active_rankings.push((*broker_id, *load));
        } else {
            inactive_brokers.push(*broker_id);
        }
    }

    if !inactive_brokers.is_empty() {
        debug!(
            inactive_count = inactive_brokers.len(),
            inactive_brokers = ?inactive_brokers,
            "filtered out non-active brokers from imbalance calculation"
        );
    }

    let rankings = &active_rankings;

    // Need at least 2 brokers for meaningful imbalance calculation
    if rankings.len() < 2 {
        return Ok(ImbalanceMetrics {
            coefficient_of_variation: 0.0,
            max_load: 0.0,
            min_load: 0.0,
            mean_load: 0.0,
            std_deviation: 0.0,
            overloaded_brokers: vec![],
            underloaded_brokers: vec![],
        });
    }

    // Extract load values and broker IDs
    let loads: Vec<f64> = rankings.iter().map(|(_, load)| *load as f64).collect();

    // Calculate mean
    let mean = loads.iter().sum::<f64>() / loads.len() as f64;

    // Calculate variance and standard deviation
    let variance =
        loads.iter().map(|load| (*load - mean).powi(2)).sum::<f64>() / loads.len() as f64;
    let std_dev = variance.sqrt();

    // Find max and min loads
    let max_load = loads
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .copied()
        .unwrap_or(0.0);

    let min_load = loads
        .iter()
        .min_by(|a, b| a.partial_cmp(b).unwrap())
        .copied()
        .unwrap_or(0.0);

    // Coefficient of variation (CV = std_dev / mean)
    // This is the key metric - it's scale-independent unlike std_dev
    let cv = if mean > 0.0 { std_dev / mean } else { 0.0 };

    // Export metric for monitoring
    metrics::gauge!(crate::broker_metrics::CLUSTER_IMBALANCE_CV.name).set(cv);

    // Identify overloaded and underloaded brokers
    // Use a more permissive threshold when CV is high to ensure rebalancing happens
    let mut overloaded = vec![];
    let mut underloaded = vec![];

    // For high imbalance (CV > 0.3), use 0.5 std_dev threshold for faster convergence
    // For moderate imbalance, use 1.0 std_dev threshold (original behavior)
    let threshold_multiplier = if cv > 0.3 { 0.5 } else { 1.0 };

    // Minimum std_dev floor to prevent over-sensitive labeling on idle/uniform clusters.
    // Without this, small CPU fluctuations (2-3 points) would cause status flapping
    // between Normal/Overloaded/Underloaded on idle clusters.
    // With floor of 5.0, brokers need to differ by at least 5 points from mean to be labeled.
    const MIN_STD_DEV_FLOOR: f64 = 5.0;
    let effective_std_dev = std_dev.max(MIN_STD_DEV_FLOOR);

    for (broker_id, load) in rankings.iter() {
        let load_f64 = *load as f64;

        // Overloaded: more than threshold above mean
        if load_f64 > mean + (effective_std_dev * threshold_multiplier) {
            overloaded.push(*broker_id);
        }
        // Underloaded: more than threshold below mean
        else if load_f64 < mean - (effective_std_dev * threshold_multiplier) {
            underloaded.push(*broker_id);
        }
    }

    Ok(ImbalanceMetrics {
        coefficient_of_variation: cv,
        max_load,
        min_load,
        mean_load: mean,
        std_deviation: std_dev,
        overloaded_brokers: overloaded,
        underloaded_brokers: underloaded,
    })
}

/// Determines if cluster needs rebalancing based on configuration and metrics
///
/// ## Purpose:
/// Decision logic for automated rebalancing. Checks multiple conditions:
/// 1. Rebalancing is enabled in config
/// 2. Minimum broker count is met
/// 3. CV exceeds the threshold for current aggressiveness level
///
/// ## Parameters:
/// - `metrics`: Current cluster imbalance metrics
/// - `config`: Rebalancing configuration (threshold, min brokers, etc.)
///
/// ## Returns:
/// `true` if automated rebalancing should proceed, `false` otherwise
pub(super) fn should_rebalance(
    metrics: &ImbalanceMetrics,
    config: &config::RebalancingConfig,
) -> bool {
    // Check if rebalancing is enabled
    if !config.enabled {
        return false;
    }

    // Check minimum broker requirement
    let threshold = config.aggressiveness.threshold();

    // Use coefficient of variation to determine if rebalancing is needed
    metrics.needs_rebalancing(threshold)
}

/// Selects one topic to move for rebalancing
///
/// ## Purpose:
/// Core candidate selection logic that chooses exactly one topic to move from
/// an overloaded broker to a less loaded broker. Uses intelligent selection
/// strategies to minimize disruption while maximizing balance improvement.
///
/// ## Algorithm:
/// 1. Identify source brokers from `metrics.overloaded_brokers`
/// 2. For each overloaded broker:
///    - Get all its topics
///    - Filter blacklisted and recently moved topics
///    - Sort by load (lightest first - safer to move)
/// 3. Select target broker (least loaded, excluding source)
/// 4. Move exactly 1 topic (recalculate imbalance on next cycle)
///
/// ## Strategy:
/// - **One at a time**: Move 1 topic per cycle for maximum safety
/// - **Lightest first**: Small topics are easier and safer to move
/// - **Blacklist respect**: Never move protected topics
/// - **Cooldown respect**: Don't move recently moved topics
///
/// ## Parameters:
/// - `metrics`: Cluster imbalance metrics (identifies overloaded brokers)
/// - `config`: Rebalancing configuration (limits, filters, blacklist)
///
/// ## Returns:
/// `Option<RebalancingMove>` - Exactly one move, or None if no suitable candidate found
pub(super) async fn select_rebalancing_candidate<F, Fut>(
    rankings: Arc<Mutex<Vec<(u64, usize)>>>,
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
    metrics: &ImbalanceMetrics,
    config: &config::RebalancingConfig,
    is_broker_active: F,
    history: &RebalancingHistory,
) -> Result<Option<RebalancingMove>>
where
    F: Fn(u64) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let rankings = rankings.lock().await;
    let brokers = brokers_usage.lock().await;

    // Need brokers and overloaded brokers to proceed
    if rankings.is_empty() || metrics.overloaded_brokers.is_empty() {
        return Ok(None);
    }

    // For each overloaded broker, select ONE topic to move
    // We only move 1 topic per cycle to avoid overshooting
    for overloaded_id in &metrics.overloaded_brokers {
        // Get broker's load report
        let overloaded_report = match brokers.get(overloaded_id) {
            Some(r) => r,
            None => continue,
        };

        // Get topics with their estimated load scores
        let mut candidates: Vec<_> = overloaded_report
            .topics
            .iter()
            .map(|t| (t.clone(), t.estimated_load_score()))
            .collect();

        // Filter blacklisted topics
        if !config.blacklist_topics.is_empty() {
            candidates.retain(|(topic, _)| {
                !is_topic_blacklisted(&topic.topic_name, &config.blacklist_topics)
            });
        }

        // Filter recently moved topics to prevent oscillations
        // Use min_topic_age_seconds as cooldown duration
        candidates.retain(|(topic, _)| {
            !history.was_topic_recently_moved(&topic.topic_name, config.min_topic_age_seconds)
        });

        // Sort by load (ascending - lightest first)
        // This is safer: small topics are easier to move with less disruption
        candidates.sort_by(|(_, load_a), (_, load_b)| {
            load_a
                .partial_cmp(load_b)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Select target broker (least loaded, excluding source)
        let target_broker =
            match select_target_broker(*overloaded_id, &rankings, &is_broker_active).await {
                Ok(broker) => broker,
                Err(e) => {
                    warn!(
                        source_broker = %overloaded_id,
                        error = %e,
                        "no suitable target broker found for rebalancing"
                    );
                    continue;
                }
            };

        // Move only the first (lightest) topic
        // This ensures we recalculate imbalance before moving another topic
        if let Some((topic, estimated_load)) = candidates.first() {
            let rebalancing_move = RebalancingMove::new(
                topic.topic_name.clone(),
                *overloaded_id,
                target_broker,
                RebalancingReason::LoadImbalance,
                *estimated_load,
            );

            // Return exactly 1 topic move, then recalculate imbalance on next cycle
            return Ok(Some(rebalancing_move));
        }
    }

    // No suitable topics found across all overloaded brokers
    Ok(None)
}

/// Selects target broker for topic move
///
/// ## Purpose:
/// Finds the least loaded broker to receive a topic being moved.
/// Excludes the source broker and inactive/draining brokers.
///
/// ## Algorithm:
/// - Rankings are already sorted (least loaded first)
/// - Skip source broker (can't move to self)
/// - Skip inactive/draining brokers (only active brokers can receive topics)
/// - Return first suitable broker
///
/// ## Parameters:
/// - `exclude_broker`: Source broker ID (can't be target)
/// - `rankings`: Broker rankings (sorted least loaded first)
/// - `is_broker_active`: Function to check if broker is in active state
///
/// ## Returns:
/// Target broker ID or error if no suitable broker found
async fn select_target_broker<F, Fut>(
    exclude_broker: u64,
    rankings: &[(u64, usize)],
    is_broker_active: F,
) -> Result<u64>
where
    F: Fn(u64) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    // Find least loaded broker that is not the excluded one and is active
    for (broker_id, _) in rankings.iter() {
        if *broker_id != exclude_broker && is_broker_active(*broker_id).await {
            return Ok(*broker_id);
        }
    }

    Err(anyhow!(
        "No suitable target broker found for rebalancing (all brokers either inactive or excluded)"
    ))
}

/// Checks if topic matches any blacklist pattern
///
/// ## Purpose:
/// Determines if a topic should never be rebalanced based on blacklist patterns.
/// Supports exact matches and namespace wildcards.
///
/// ## Pattern Matching:
/// - **Exact**: `/default/critical-topic` matches only that topic
/// - **Namespace wildcard**: `/default/*` matches all topics in the `/default` namespace
///
/// ## Topic Format:
/// Topics are structured as: `/{namespace}/{topic_name}`
/// Examples: `/default/my-topic`, `/system/metrics`, `/production/orders`
///
/// ## Parameters:
/// - `topic_name`: Topic to check (must start with `/`)
/// - `blacklist`: List of blacklist patterns
///
/// ## Returns:
/// `true` if topic is blacklisted, `false` otherwise
pub(super) fn is_topic_blacklisted(topic_name: &str, blacklist: &[String]) -> bool {
    for pattern in blacklist {
        // Exact match
        if pattern == topic_name {
            return true;
        }

        // Namespace wildcard: /namespace/*
        if pattern.ends_with("/*") {
            // Extract namespace prefix (e.g., "/default/" from "/default/*")
            let namespace_prefix = &pattern[..pattern.len() - 1]; // Remove the '*', keep the '/'

            // Topic must start with the namespace and have something after it
            // e.g., "/default/*" matches "/default/anything" but not "/default" or "/defaultx/topic"
            if topic_name.starts_with(namespace_prefix) && topic_name.len() > namespace_prefix.len()
            {
                return true;
            }
        }
    }

    false
}

/// Executes a list of rebalancing moves
///
/// ## Purpose:
/// Main rebalancing execution loop that orchestrates topic moves from overloaded
/// to underloaded brokers. Includes safety controls like rate limiting, cooldown
/// delays, history tracking, and audit logging.
///
/// ## Safety Features:
/// - **Rate limiting**: Stops when `max_moves_per_hour` is reached
/// - **Cooldown**: Waits between moves to allow cluster stabilization
/// - **Error handling**: Logs failures but continues with remaining moves
/// - **Audit trail**: Records every move to ETCD for compliance/debugging
///
/// ## Algorithm:
/// For each move:
/// 1. Check hourly rate limit → stop if exceeded
/// 2. Execute the single move (create marker + delete assignment)
/// 3. Record in history (for rate limiting)
/// 4. Log to ETCD (for audit)
/// 5. Sleep for cooldown period
///
/// ## Parameters:
/// - `moves`: List of moves selected by `select_rebalancing_candidates()`
/// - `config`: Rebalancing configuration (rate limits, cooldown)
/// - `history`: Rebalancing history tracker (for rate limiting)
///
/// ## Returns:
/// Number of successfully executed moves
pub(super) async fn execute_rebalancing(
    meta_store: &MetadataStorage,
    moves: Vec<RebalancingMove>,
    config: &config::RebalancingConfig,
    history: &mut RebalancingHistory,
) -> Result<usize> {
    let mut executed = 0;
    let total_moves = moves.len();

    for mv in moves {
        // Check rate limit
        if history.count_moves_in_last_hour() >= config.max_moves_per_hour {
            warn!(
                current_moves = history.count_moves_in_last_hour(),
                limit = config.max_moves_per_hour,
                "rebalancing rate limit reached, stopping cycle"
            );
            break;
        }

        info!(
            topic = %mv.topic_name,
            from = %mv.from_broker,
            to = %mv.to_broker,
            reason = ?mv.reason,
            estimated_load = %mv.estimated_load,
            "executing rebalancing move"
        );

        // Execute the move
        match execute_single_move(meta_store, &mv).await {
            Ok(()) => {
                executed += 1;
                history.record_move(mv.clone());

                // Record success metric
                metrics::counter!(
                    crate::broker_metrics::REBALANCING_MOVES_TOTAL.name,
                    "reason" => format!("{:?}", mv.reason)
                )
                .increment(1);

                // Log to ETCD for auditing
                log_rebalancing_event(meta_store, &mv).await;

                // Cooldown between moves to allow cluster stabilization
                if config.cooldown_seconds > 0 {
                    tokio::time::sleep(Duration::from_secs(config.cooldown_seconds)).await;
                }
            }
            Err(e) => {
                // Record failure metric
                metrics::counter!(crate::broker_metrics::REBALANCING_FAILURES_TOTAL.name)
                    .increment(1);

                error!(
                    topic = %mv.topic_name,
                    error = %e,
                    "failed to execute rebalancing move, continuing with remaining moves"
                );
            }
        }
    }

    info!(
        executed = executed,
        total_moves = total_moves,
        "rebalancing cycle complete"
    );

    Ok(executed)
}

/// Executes a single topic move for rebalancing
///
/// ## Purpose:
/// Performs the actual topic move by creating an unassigned marker with a target
/// broker hint and deleting the source assignment. This triggers the existing
/// broker watch mechanism to gracefully unload the topic from the source broker.
///
/// ## How It Works:
/// 1. **Create unassigned marker** at `/cluster/unassigned/{topic_name}`:
///    - Includes `to_broker` hint for LoadManager
///    - Includes `reason: "rebalance"` for tracking
///    - Includes timestamp for audit trail
///
/// 2. **Delete source assignment** at `/cluster/brokers/{from_broker}/{topic_name}`:
///    - Source broker's `broker_watcher` detects deletion
///    - Triggers existing `topic.unload()` from Phase D
///    - Topic gracefully closes producers/consumers
///
/// 3. **Automatic reassignment** (handled by existing watchers):
///    - LoadManager sees unassigned marker
///    - Step 6 logic reads `to_broker` hint
///    - Assigns topic to target broker (not rankings)
///    - Target broker loads topic from cloud storage
///
/// ## Reuses Existing Infrastructure:
/// - ✅ Phase D unload mechanism (graceful topic migration)
/// - ✅ broker_watcher (detects assignment changes)
/// - ✅ assign_topic_to_broker (will be enhanced in Step 6)
///
/// ## Parameters:
/// - `mv`: The move to execute (source, target, topic, metadata)
///
/// ## Returns:
/// `Ok(())` if marker created and assignment deleted successfully
pub(super) async fn execute_single_move(
    meta_store: &MetadataStorage,
    mv: &RebalancingMove,
) -> Result<()> {
    // Create unassigned marker with rebalance hint
    let unassigned_path = join_path(&[BASE_UNASSIGNED_PATH, &mv.topic_name]);

    let marker = serde_json::json!({
        "reason": "rebalance",
        "from_broker": mv.from_broker,
        "to_broker": mv.to_broker,  // ← Target broker hint for Step 6
        "timestamp": mv.timestamp,
    });

    // Post unassigned marker
    meta_store
        .put(&unassigned_path, marker, MetaOptions::None)
        .await?;

    // Delete assignment from source broker (triggers unload via watch)
    let assignment_path = join_path(&[
        BASE_BROKER_PATH,
        &mv.from_broker.to_string(),
        &mv.topic_name,
    ]);

    meta_store.delete(&assignment_path).await?;

    Ok(())
}

/// Logs rebalancing event to ETCD for audit trail
///
/// ## Purpose:
/// Creates a persistent audit log of all rebalancing moves in the metadata store.
/// This provides:
/// - **Compliance**: Track all automated topic moves
/// - **Debugging**: Understand why/when topics were moved
/// - **Metrics**: Analyze rebalancing patterns over time
/// - **CLI visibility**: Show rebalancing history to admins
///
/// ## Storage:
/// Events stored at: `/cluster/rebalancing_history/{timestamp}`
///
/// ## Event Structure:
/// ```json
/// {
///   "topic": "/default/my-topic",
///   "from_broker": 1,
///   "to_broker": 2,
///   "reason": "LoadImbalance",
///   "estimated_load": 5.2,
///   "timestamp": 1234567890
/// }
/// ```
///
/// ## Parameters:
/// - `mv`: The move that was executed
pub(super) async fn log_rebalancing_event(meta_store: &MetadataStorage, mv: &RebalancingMove) {
    let key = format!("/cluster/rebalancing_history/{}", mv.timestamp);

    let event = serde_json::json!({
        "topic": mv.topic_name,
        "from_broker": mv.from_broker,
        "to_broker": mv.to_broker,
        "reason": format!("{:?}", mv.reason),
        "estimated_load": mv.estimated_load,
        "timestamp": mv.timestamp,
    });

    if let Err(e) = meta_store.put(&key, event, MetaOptions::None).await {
        warn!(
            error = %e,
            topic = %mv.topic_name,
            "failed to log rebalancing event to ETCD (non-critical)"
        );
    }
}
