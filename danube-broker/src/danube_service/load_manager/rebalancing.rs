use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::SystemTime;

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
pub struct RebalancingHistory {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebalancing_move_creation() {
        let mv = RebalancingMove::new(
            "/default/test-topic".to_string(),
            1,
            2,
            RebalancingReason::LoadImbalance,
            42.5,
        );

        assert_eq!(mv.topic_name, "/default/test-topic");
        assert_eq!(mv.from_broker, 1);
        assert_eq!(mv.to_broker, 2);
        assert_eq!(mv.reason, RebalancingReason::LoadImbalance);
        assert_eq!(mv.estimated_load, 42.5);
        assert!(mv.timestamp > 0);
    }

    #[test]
    fn test_rebalancing_history_record() {
        let mut history = RebalancingHistory::new(3);

        // Add moves
        for i in 0..5 {
            let mv = RebalancingMove::new(
                format!("/default/topic-{}", i),
                1,
                2,
                RebalancingReason::LoadImbalance,
                10.0,
            );
            history.record_move(mv);
        }

        // Should only keep last 3 (ring buffer)
        assert_eq!(history.total_moves(), 3);

        let recent = history.get_recent_moves(10);
        assert_eq!(recent.len(), 3);
        // Most recent first
        assert_eq!(recent[0].topic_name, "/default/topic-4");
        assert_eq!(recent[1].topic_name, "/default/topic-3");
        assert_eq!(recent[2].topic_name, "/default/topic-2");
    }

    #[test]
    fn test_rebalancing_history_hourly_count() {
        let mut history = RebalancingHistory::new(100);

        // Add a move with current timestamp (should be counted)
        let mv1 = RebalancingMove::new(
            "/default/topic-1".to_string(),
            1,
            2,
            RebalancingReason::LoadImbalance,
            10.0,
        );
        history.record_move(mv1);

        // Add a move with old timestamp (should not be counted)
        let old_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(7200); // 2 hours ago

        let mut mv2 = RebalancingMove::new(
            "/default/topic-2".to_string(),
            1,
            2,
            RebalancingReason::LoadImbalance,
            10.0,
        );
        mv2.timestamp = old_timestamp;
        history.record_move(mv2);

        // Only the recent move should be counted
        assert_eq!(history.count_moves_in_last_hour(), 1);
        assert_eq!(history.total_moves(), 2);
    }

    #[test]
    fn test_rebalancing_history_clear() {
        let mut history = RebalancingHistory::new(10);

        for i in 0..5 {
            let mv = RebalancingMove::new(
                format!("/default/topic-{}", i),
                1,
                2,
                RebalancingReason::LoadImbalance,
                10.0,
            );
            history.record_move(mv);
        }

        assert_eq!(history.total_moves(), 5);
        history.clear();
        assert_eq!(history.total_moves(), 0);
    }

    #[test]
    fn test_imbalance_metrics_is_balanced() {
        let metrics = ImbalanceMetrics {
            coefficient_of_variation: 0.25,
            max_load: 100.0,
            min_load: 50.0,
            mean_load: 75.0,
            std_deviation: 18.75,
            overloaded_brokers: vec![],
            underloaded_brokers: vec![],
        };

        // CV 0.25 < 0.30 threshold (balanced)
        assert!(metrics.is_balanced(0.30));
        assert!(!metrics.needs_rebalancing(0.30));

        // CV 0.25 > 0.20 threshold (needs rebalancing)
        assert!(!metrics.is_balanced(0.20));
        assert!(metrics.needs_rebalancing(0.20));
    }

    #[test]
    fn test_imbalance_metrics_broker_identification() {
        let metrics = ImbalanceMetrics {
            coefficient_of_variation: 0.35,
            max_load: 100.0,
            min_load: 50.0,
            mean_load: 75.0,
            std_deviation: 26.25,
            overloaded_brokers: vec![1, 2],
            underloaded_brokers: vec![3, 4],
        };

        assert!(metrics.has_overloaded_brokers());
        assert!(metrics.has_underloaded_brokers());
        assert_eq!(metrics.overloaded_brokers.len(), 2);
        assert_eq!(metrics.underloaded_brokers.len(), 2);
    }

    #[test]
    fn test_rebalancing_reason_equality() {
        assert_eq!(
            RebalancingReason::LoadImbalance,
            RebalancingReason::LoadImbalance
        );
        assert_ne!(
            RebalancingReason::LoadImbalance,
            RebalancingReason::BrokerOverload
        );
    }
}
