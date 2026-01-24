use serde::{Deserialize, Serialize};

/// Assignment strategy for new topics
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentStrategy {
    /// Fair distribution - simple topic count only (simplest, most predictable)
    /// Best for: Development, testing, predictable placement
    Fair,
    
    /// Balanced multi-factor scoring (RECOMMENDED, default)
    /// Formula: (weighted_topic_load × 0.3) + (CPU × 0.35) + (Memory × 0.35)
    /// Best for: General purpose, mixed workloads, production clusters
    Balanced,
    
    /// Adaptive weighted load - smart bottleneck detection
    /// Automatically prioritizes the resource under most pressure
    /// Best for: Variable workloads, auto-optimization, advanced users
    WeightedLoad,
}

impl Default for AssignmentStrategy {
    fn default() -> Self {
        Self::Balanced
    }
}

/// Rebalancing aggressiveness level
/// Controls how aggressively to optimize cluster balance
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RebalancingAggressiveness {
    /// Fewer moves, only when severely imbalanced (CV > 40%), more stable
    Conservative,
    /// Moderate optimization (CV > 30%), good for most production clusters
    Balanced,
    /// Optimize harder (CV > 20%), more moves, responds faster to changes
    Aggressive,
}

impl Default for RebalancingAggressiveness {
    fn default() -> Self {
        Self::Balanced
    }
}

impl RebalancingAggressiveness {
    /// Returns the coefficient of variation threshold for triggering rebalancingation)
    pub fn threshold(&self) -> f64 {
        match self {
            Self::Conservative => 0.40,
            Self::Balanced => 0.30,
            Self::Aggressive => 0.20,
        }
    }

    /// Get default check interval based on aggressiveness (seconds)
    /// Reserved for future CLI/API use (Step 9)
    #[allow(dead_code)]
    pub fn default_check_interval(&self) -> u64 {
        match self {
            Self::Conservative => 600, // 10 minutes
            Self::Balanced => 240,     // 4 minutes
            Self::Aggressive => 120,   // 2 minutes
        }
    }

    /// Get default max moves per cycle based on aggressiveness
    /// Reserved for future CLI/API use (Step 9)
    #[allow(dead_code)]
    pub fn default_max_moves_per_cycle(&self) -> usize {
        match self {
            Self::Conservative => 2,
            Self::Balanced => 4,
            Self::Aggressive => 6,
        }
    }

    /// Get default max moves per hour based on aggressiveness
    /// Reserved for future CLI/API use (Step 9)
    #[allow(dead_code)]
    pub fn default_max_moves_per_hour(&self) -> usize {
        match self {
            Self::Conservative => 5,
            Self::Balanced => 10,
            Self::Aggressive => 20,
        }
    }

    /// Get default cooldown seconds based on aggressiveness
    /// Reserved for future CLI/API use (Step 9)
    #[allow(dead_code)]
    pub fn default_cooldown_seconds(&self) -> u64 {
        match self {
            Self::Conservative => 120, // 2 minutes
            Self::Balanced => 60,      // 1 minute
            Self::Aggressive => 30,    // 30 seconds
        }
    }
}

/// Rebalancing configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RebalancingConfig {
    /// Enable/disable automated rebalancing
    pub enabled: bool,
    /// Aggressiveness level (conservative, balanced, aggressive)
    pub aggressiveness: RebalancingAggressiveness,
    /// How often to check cluster balance (seconds)
    pub check_interval_seconds: u64,
    /// Max concurrent topic moves per rebalancing cycle
    pub max_moves_per_cycle: usize,
    /// Max topic moves per hour (rate limiting)
    pub max_moves_per_hour: usize,
    /// Wait time between individual topic moves (seconds)
    pub cooldown_seconds: u64,
    /// Minimum brokers needed to enable rebalancing
    pub min_brokers_for_rebalance: usize,
    /// Don't move topics younger than this (seconds)
    pub min_topic_age_seconds: u64,
    /// Topics matching these patterns will never be rebalanced
    pub blacklist_topics: Vec<String>,
}

impl Default for RebalancingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            aggressiveness: RebalancingAggressiveness::Balanced,
            check_interval_seconds: 300,
            max_moves_per_cycle: 3,
            max_moves_per_hour: 10,
            cooldown_seconds: 60,
            min_brokers_for_rebalance: 2,
            min_topic_age_seconds: 300,
            blacklist_topics: vec![],
        }
    }
}

/// Load Manager configuration (Phase 3)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoadManagerConfig {
    /// Topic assignment strategy for new topics
    #[serde(default)]
    pub assignment_strategy: AssignmentStrategy,

    /// Automated rebalancing configuration
    #[serde(default)]
    pub rebalancing: RebalancingConfig,
}

impl Default for LoadManagerConfig {
    fn default() -> Self {
        Self {
            assignment_strategy: AssignmentStrategy::default(),
            rebalancing: RebalancingConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_assignment_strategy() {
        let strategy = AssignmentStrategy::default();
        assert_eq!(strategy, AssignmentStrategy::Balanced);
    }

    #[test]
    fn test_default_rebalancing_aggressiveness() {
        let agg = RebalancingAggressiveness::default();
        assert_eq!(agg, RebalancingAggressiveness::Balanced);
    }

    #[test]
    fn test_aggressiveness_thresholds() {
        assert_eq!(RebalancingAggressiveness::Conservative.threshold(), 0.40);
        assert_eq!(RebalancingAggressiveness::Balanced.threshold(), 0.30);
        assert_eq!(RebalancingAggressiveness::Aggressive.threshold(), 0.20);
    }

    #[test]
    fn test_aggressiveness_defaults() {
        let conservative = RebalancingAggressiveness::Conservative;
        assert_eq!(conservative.default_check_interval(), 600);
        assert_eq!(conservative.default_max_moves_per_cycle(), 2);
        assert_eq!(conservative.default_max_moves_per_hour(), 5);
        assert_eq!(conservative.default_cooldown_seconds(), 120);

        let balanced = RebalancingAggressiveness::Balanced;
        assert_eq!(balanced.default_check_interval(), 240);
        assert_eq!(balanced.default_max_moves_per_cycle(), 4);
        assert_eq!(balanced.default_max_moves_per_hour(), 10);
        assert_eq!(balanced.default_cooldown_seconds(), 60);

        let aggressive = RebalancingAggressiveness::Aggressive;
        assert_eq!(aggressive.default_check_interval(), 120);
        assert_eq!(aggressive.default_max_moves_per_cycle(), 6);
        assert_eq!(aggressive.default_max_moves_per_hour(), 20);
        assert_eq!(aggressive.default_cooldown_seconds(), 30);
    }

    #[test]
    fn test_rebalancing_config_defaults() {
        let config = RebalancingConfig::default();
        assert!(!config.enabled); // Should start disabled
        assert_eq!(config.aggressiveness, RebalancingAggressiveness::Balanced);
        assert_eq!(config.check_interval_seconds, 300);
        assert_eq!(config.max_moves_per_cycle, 3);
        assert_eq!(config.max_moves_per_hour, 10);
        assert_eq!(config.cooldown_seconds, 60);
        assert_eq!(config.min_brokers_for_rebalance, 2);
        assert_eq!(config.min_topic_age_seconds, 300);
        assert_eq!(config.blacklist_topics, Vec::<String>::new());
    }

    #[test]
    fn test_load_manager_config_defaults() {
        let config = LoadManagerConfig::default();
        assert_eq!(config.assignment_strategy, AssignmentStrategy::Balanced);
        assert!(!config.rebalancing.enabled);
    }
}
