# LoadManager Phase 3: Automated Proactive Rebalancing

**Priority:** HIGH  
**Timeline:** 4-5 weeks  
**Status:** Not Started  
**Depends On:** Phase 1 (Resource Monitoring), Phase 2 (Intelligent Ranking)

---

## 🎯 Goals

- **Automated topic moves** when clusters become imbalanced
- Minimal disruption using graceful topic move workflow
- Configurable rebalancing policies (conservative, balanced, aggressive)
- Safety mechanisms to prevent rebalancing storms
- Integration with existing sealed state mechanism

---

## 📋 Implementation Steps

### Step 1: Define Rebalancing Policy Configuration
**Status:** ☐ Not Started

**Task:** Add rebalancing config to `config/danube_broker.yml`

```yaml
# Load Manager Configuration
load_manager:
  ranking_algorithm: "weighted_composite"
  
  ranking_weights:
    cpu: 0.25
    memory: 0.20
    disk_io: 0.15
    network_io: 0.10
    throughput: 0.15
    topic_count: 0.10
    disk_usage: 0.05
  
  # Automated Rebalancing (NEW)
  rebalancing:
    enabled: true
    strategy: "balanced"              # conservative | balanced | aggressive
    check_interval_seconds: 300       # Check every 5 minutes
    max_moves_per_cycle: 3            # Limit concurrent moves
    cooldown_seconds: 60              # Wait between moves
    min_brokers_for_rebalance: 2      # Require at least 2 brokers
    
    # Thresholds for each strategy
    thresholds:
      conservative: 0.40   # 40% imbalance
      balanced: 0.30       # 30% imbalance
      aggressive: 0.20     # 20% imbalance
    
    # Safety limits
    max_moves_per_hour: 10
    min_topic_age_seconds: 300        # Don't move topics younger than 5 min
    blacklist_topics: []              # Topics to never rebalance
```

**Add to ServiceConfiguration:**
```rust
#[derive(Debug, Clone, Deserialize)]
pub struct LoadManagerConfig {
    pub ranking_algorithm: String,
    pub ranking_weights: RankingWeights,
    
    #[serde(default)]
    pub rebalancing: RebalancingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RebalancingConfig {
    pub enabled: bool,
    pub strategy: RebalancingStrategy,
    pub check_interval_seconds: u64,
    pub max_moves_per_cycle: usize,
    pub cooldown_seconds: u64,
    pub min_brokers_for_rebalance: usize,
    pub thresholds: RebalancingThresholds,
    pub max_moves_per_hour: usize,
    pub min_topic_age_seconds: u64,
    pub blacklist_topics: Vec<String>,
}

impl Default for RebalancingConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Start disabled for safety
            strategy: RebalancingStrategy::Balanced,
            check_interval_seconds: 300,
            max_moves_per_cycle: 3,
            cooldown_seconds: 60,
            min_brokers_for_rebalance: 2,
            thresholds: RebalancingThresholds::default(),
            max_moves_per_hour: 10,
            min_topic_age_seconds: 300,
            blacklist_topics: vec![],
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RebalancingStrategy {
    Conservative,
    Balanced,
    Aggressive,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RebalancingThresholds {
    pub conservative: f64,
    pub balanced: f64,
    pub aggressive: f64,
}

impl Default for RebalancingThresholds {
    fn default() -> Self {
        Self {
            conservative: 0.40,
            balanced: 0.30,
            aggressive: 0.20,
        }
    }
}
```

**Verification:**
- [ ] Configuration loads correctly
- [ ] Defaults are safe (enabled=false)
- [ ] Validation prevents invalid values

---

### Step 2: Create Rebalancing Types and State
**Status:** ☐ Not Started

**Task:** Create `danube-broker/src/danube_service/load_manager/rebalancing.rs`

```rust
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use std::collections::VecDeque;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingMove {
    pub topic_name: String,
    pub from_broker: u64,
    pub to_broker: u64,
    pub reason: RebalancingReason,
    pub estimated_load: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RebalancingReason {
    LoadImbalance,
    BrokerOverload,
    BrokerUnderUtilized,
    ManualRebalance,
}

/// Tracks rebalancing history for rate limiting
pub struct RebalancingHistory {
    moves: VecDeque<RebalancingMove>,
    max_size: usize,
}

impl RebalancingHistory {
    pub fn new(max_size: usize) -> Self {
        Self {
            moves: VecDeque::with_capacity(max_size),
            max_size,
        }
    }
    
    pub fn record_move(&mut self, mv: RebalancingMove) {
        if self.moves.len() >= self.max_size {
            self.moves.pop_front();
        }
        self.moves.push_back(mv);
    }
    
    pub fn count_moves_in_last_hour(&self) -> usize {
        let one_hour_ago = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() - 3600;
        
        self.moves.iter()
            .filter(|m| m.timestamp >= one_hour_ago)
            .count()
    }
    
    pub fn get_recent_moves(&self, limit: usize) -> Vec<&RebalancingMove> {
        self.moves.iter()
            .rev()
            .take(limit)
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct ImbalanceMetrics {
    pub coefficient_of_variation: f64,
    pub max_load: f64,
    pub min_load: f64,
    pub mean_load: f64,
    pub std_deviation: f64,
    pub overloaded_brokers: Vec<u64>,
    pub underloaded_brokers: Vec<u64>,
}

impl ImbalanceMetrics {
    pub fn is_balanced(&self, threshold: f64) -> bool {
        self.coefficient_of_variation < threshold
    }
}
```

**Verification:**
- [ ] Types compile
- [ ] Serialization works
- [ ] History tracking works correctly

---

### Step 3: Implement Imbalance Detection
**Status:** ☐ Not Started

**Task:** Add imbalance calculation methods to LoadManager

```rust
// In load_manager.rs

impl LoadManager {
    /// Calculates cluster imbalance metrics
    pub async fn calculate_imbalance(&self) -> Result<ImbalanceMetrics> {
        let rankings = self.rankings.lock().await;
        
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
        
        // Extract loads
        let loads: Vec<f64> = rankings.iter().map(|(_, load)| *load).collect();
        
        // Calculate statistics
        let mean = loads.iter().sum::<f64>() / loads.len() as f64;
        let variance = loads.iter()
            .map(|load| (*load - mean).powi(2))
            .sum::<f64>() / loads.len() as f64;
        let std_dev = variance.sqrt();
        
        let max_load = loads.iter()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .copied()
            .unwrap_or(0.0);
        
        let min_load = loads.iter()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .copied()
            .unwrap_or(0.0);
        
        // Coefficient of variation (CV = std_dev / mean)
        let cv = if mean > 0.0 { std_dev / mean } else { 0.0 };
        
        // Identify overloaded and underloaded brokers (beyond 1 std dev)
        let mut overloaded = vec![];
        let mut underloaded = vec![];
        
        for (broker_id, load) in rankings.iter() {
            if *load > mean + std_dev {
                overloaded.push(*broker_id);
            } else if *load < mean - std_dev && *load < mean * 0.5 {
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
    
    /// Determines if rebalancing is needed based on strategy
    pub fn should_rebalance(
        &self,
        metrics: &ImbalanceMetrics,
        config: &RebalancingConfig,
    ) -> bool {
        if !config.enabled {
            return false;
        }
        
        let threshold = match config.strategy {
            RebalancingStrategy::Conservative => config.thresholds.conservative,
            RebalancingStrategy::Balanced => config.thresholds.balanced,
            RebalancingStrategy::Aggressive => config.thresholds.aggressive,
        };
        
        metrics.coefficient_of_variation > threshold
    }
}
```

**Verification:**
- [ ] Statistics are calculated correctly
- [ ] Thresholds work as expected
- [ ] Edge cases handled (1 broker, 0 topics)

---

### Step 4: Implement Candidate Selection
**Status:** ☐ Not Started

**Task:** Add logic to select topics for rebalancing

```rust
impl LoadManager {
    /// Selects topics to move for rebalancing
    pub async fn select_rebalancing_candidates(
        &self,
        metrics: &ImbalanceMetrics,
        config: &RebalancingConfig,
    ) -> Result<Vec<RebalancingMove>> {
        let rankings = self.rankings.lock().await;
        let brokers = self.brokers_usage.lock().await;
        
        if rankings.is_empty() || metrics.overloaded_brokers.is_empty() {
            return Ok(vec![]);
        }
        
        let mut moves = Vec::new();
        
        // For each overloaded broker
        for overloaded_id in &metrics.overloaded_brokers {
            if moves.len() >= config.max_moves_per_cycle {
                break;
            }
            
            // Get broker's topics
            let overloaded_report = match brokers.get(overloaded_id) {
                Some(r) => r,
                None => continue,
            };
            
            // Select lightest topics first (easier to move)
            let mut candidates: Vec<_> = overloaded_report.topics.iter()
                .map(|t| (t, t.estimated_load_score()))
                .collect();
            
            // Filter blacklisted topics
            candidates.retain(|(topic, _)| {
                !config.blacklist_topics.contains(&topic.topic_name)
            });
            
            // Filter topics that are too young
            let min_age = config.min_topic_age_seconds;
            if min_age > 0 {
                candidates.retain(|(topic, _)| {
                    // TODO: Get topic creation time
                    true
                });
            }
            
            // Sort by load (ascending - lightest first)
            candidates.sort_by(|(_, load_a), (_, load_b)| {
                load_a.partial_cmp(load_b).unwrap()
            });
            
            // Select target broker (least loaded, excluding source)
            let target_broker = self.select_target_broker(*overloaded_id, &rankings).await?;
            
            // Create moves
            for (topic, estimated_load) in candidates.iter().take(config.max_moves_per_cycle - moves.len()) {
                moves.push(RebalancingMove {
                    topic_name: topic.topic_name.clone(),
                    from_broker: *overloaded_id,
                    to_broker: target_broker,
                    reason: RebalancingReason::LoadImbalance,
                    estimated_load: *estimated_load,
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                });
            }
        }
        
        Ok(moves)
    }
    
    async fn select_target_broker(
        &self,
        exclude_broker: u64,
        rankings: &[(u64, f64)],
    ) -> Result<u64> {
        // Find least loaded broker that is not the excluded one and is active
        for (broker_id, _) in rankings.iter() {
            if *broker_id != exclude_broker && self.is_broker_active(*broker_id).await {
                return Ok(*broker_id);
            }
        }
        
        Err(anyhow::anyhow!(
            "No suitable target broker found for rebalancing"
        ))
    }
}
```

**Verification:**
- [ ] Lightest topics selected first
- [ ] Blacklist is respected
- [ ] Age filter works
- [ ] Target selection is correct

---

### Step 5: Implement Rebalancing Execution
**Status:** ☐ Not Started

**Task:** Add topic move execution logic

```rust
impl LoadManager {
    /// Executes a list of rebalancing moves
    pub async fn execute_rebalancing(
        &mut self,
        moves: Vec<RebalancingMove>,
        config: &RebalancingConfig,
        history: &mut RebalancingHistory,
    ) -> Result<usize> {
        let mut executed = 0;
        
        for mv in moves {
            // Check rate limit
            if history.count_moves_in_last_hour() >= config.max_moves_per_hour {
                warn!(
                    "Rebalancing rate limit reached ({} moves/hour)",
                    config.max_moves_per_hour
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
            match self.execute_single_move(&mv).await {
                Ok(()) => {
                    executed += 1;
                    history.record_move(mv.clone());
                    
                    // Log to ETCD for auditing
                    self.log_rebalancing_event(&mv).await;
                    
                    // Cooldown between moves
                    tokio::time::sleep(Duration::from_secs(config.cooldown_seconds)).await;
                }
                Err(e) => {
                    error!(
                        topic = %mv.topic_name,
                        error = %e,
                        "failed to execute rebalancing move"
                    );
                }
            }
        }
        
        Ok(executed)
    }
    
    async fn execute_single_move(&mut self, mv: &RebalancingMove) -> Result<()> {
        // Create unassigned marker with rebalance hint
        let unassigned_path = join_path(&[
            BASE_UNASSIGNED_PATH,
            &mv.topic_name,
        ]);
        
        let marker = serde_json::json!({
            "reason": "rebalance",
            "from_broker": mv.from_broker,
            "to_broker": mv.to_broker,
            "timestamp": mv.timestamp,
        });
        
        // Post unassigned marker
        self.meta_store
            .put(&unassigned_path, marker, MetaOptions::None)
            .await?;
        
        // Delete assignment from source broker
        let assignment_path = join_path(&[
            BASE_BROKER_PATH,
            &mv.from_broker.to_string(),
            &mv.topic_name,
        ]);
        
        self.meta_store.delete(&assignment_path).await?;
        
        // The existing broker_watcher will trigger unload on source broker
        // LoadManager will assign to target broker (using hint if available)
        
        info!(
            topic = %mv.topic_name,
            from = %mv.from_broker,
            to = %mv.to_broker,
            "rebalancing move initiated"
        );
        
        Ok(())
    }
    
    async fn log_rebalancing_event(&self, mv: &RebalancingMove) {
        let key = format!(
            "/cluster/rebalancing_history/{}",
            mv.timestamp
        );
        
        let event = serde_json::json!({
            "topic": mv.topic_name,
            "from_broker": mv.from_broker,
            "to_broker": mv.to_broker,
            "reason": format!("{:?}", mv.reason),
            "estimated_load": mv.estimated_load,
            "timestamp": mv.timestamp,
        });
        
        if let Err(e) = self.meta_store
            .put(&key, event, MetaOptions::None)
            .await
        {
            warn!(error = %e, "Failed to log rebalancing event to ETCD");
        }
    }
}
```

**Verification:**
- [ ] Moves are executed correctly
- [ ] Rate limiting works
- [ ] Cooldown is respected
- [ ] History is recorded

---

### Step 6: Enhance Topic Assignment to Use Rebalance Hints
**Status:** ☐ Not Started

**Task:** Update `assign_topic_to_broker()` to respect target broker hints

```rust
// In load_manager.rs

async fn assign_topic_to_broker(&mut self, event: WatchEvent) {
    match event {
        WatchEvent::Put { key, value, .. } => {
            self.calculate_rankings_simple().await;
            
            let key_str = match std::str::from_utf8(&key) {
                Ok(s) => s,
                Err(e) => {
                    error!(error = %e, "invalid UTF-8 in key");
                    return;
                }
            };
            
            let parts: Vec<_> = key_str.split(BASE_UNASSIGNED_PATH).collect();
            let topic_name = parts[1];
            
            // Parse unload/rebalance marker
            let mut exclude_broker: Option<u64> = None;
            let mut target_broker_hint: Option<u64> = None;
            
            if !value.is_empty() {
                if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&value) {
                    if let Some(obj) = val.as_object() {
                        let reason = obj.get("reason").and_then(|v| v.as_str());
                        
                        // Handle unload (exclude source broker)
                        if reason == Some("unload") {
                            if let Some(from_broker) = obj.get("from_broker").and_then(|v| v.as_u64()) {
                                exclude_broker = Some(from_broker);
                            }
                        }
                        
                        // Handle rebalance (prefer target broker)
                        if reason == Some("rebalance") {
                            if let Some(from_broker) = obj.get("from_broker").and_then(|v| v.as_u64()) {
                                exclude_broker = Some(from_broker);
                            }
                            if let Some(to_broker) = obj.get("to_broker").and_then(|v| v.as_u64()) {
                                target_broker_hint = Some(to_broker);
                            }
                        }
                    }
                }
            }
            
            // Select broker
            let broker_id = if let Some(target) = target_broker_hint {
                // Prefer rebalance target if active
                if self.is_broker_active(target).await {
                    target
                } else {
                    warn!(
                        topic = %topic_name,
                        target = %target,
                        "rebalance target broker is not active, selecting alternative"
                    );
                    match self.get_next_broker_excluding(exclude_broker.unwrap_or(0)).await {
                        Ok(id) => id,
                        Err(e) => {
                            error!(topic = %topic_name, error = %e, "cannot assign topic");
                            return;
                        }
                    }
                }
            } else if let Some(ex) = exclude_broker {
                match self.get_next_broker_excluding(ex).await {
                    Ok(id) => id,
                    Err(e) => {
                        error!(topic = %topic_name, error = %e, "cannot reassign topic for unload");
                        return;
                    }
                }
            } else {
                self.get_next_broker().await
            };
            
            // Rest of assignment logic...
            let path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string(), topic_name]);
            
            match self.meta_store.put(&path, serde_json::Value::Null, MetaOptions::None).await {
                Ok(_) => {
                    info!(
                        topic = %topic_name,
                        broker_id = %broker_id,
                        "topic assigned to broker"
                    );
                }
                Err(err) => {
                    warn!(
                        topic = %topic_name,
                        broker_id = %broker_id,
                        error = %err,
                        "unable to assign topic to broker"
                    );
                }
            }
            
            // Delete unassigned entry
            if let Err(err) = self.meta_store.delete(key_str).await {
                warn!(key = %key_str, error = %err, "failed to delete unassigned entry");
            }
            
            // Update internal state
            let mut brokers_usage = self.brokers_usage.lock().await;
            if let Some(load_report) = brokers_usage.get_mut(&broker_id) {
                load_report.topics_len += 1;
                load_report.topic_list.push(topic_name.to_string());
            }
        }
        WatchEvent::Delete { .. } => (),
    }
}
```

**Verification:**
- [ ] Rebalance hints are respected
- [ ] Fallback works if hint is invalid
- [ ] Existing logic still works

---

### Step 7: Create Rebalancing Background Task
**Status:** ☐ Not Started

**Task:** Add automated rebalancing loop

```rust
impl LoadManager {
    /// Starts the automated rebalancing loop (leader-only)
    pub async fn start_rebalancing_loop(
        mut self,
        config: RebalancingConfig,
        leader_election: LeaderElection,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                Duration::from_secs(config.check_interval_seconds)
            );
            
            let mut history = RebalancingHistory::new(1000);
            
            loop {
                interval.tick().await;
                
                // Only leader performs rebalancing
                if leader_election.get_state().await != LeaderElectionState::Leading {
                    continue;
                }
                
                if !config.enabled {
                    continue;
                }
                
                // Check cluster health
                let rankings = self.rankings.lock().await.clone();
                drop(rankings);
                
                if rankings.len() < config.min_brokers_for_rebalance {
                    debug!(
                        broker_count = rankings.len(),
                        min_required = config.min_brokers_for_rebalance,
                        "not enough brokers for rebalancing"
                    );
                    continue;
                }
                
                // Calculate imbalance
                let metrics = match self.calculate_imbalance().await {
                    Ok(m) => m,
                    Err(e) => {
                        error!(error = %e, "failed to calculate imbalance");
                        continue;
                    }
                };
                
                // Log imbalance metrics
                debug!(
                    cv = %metrics.coefficient_of_variation,
                    mean_load = %metrics.mean_load,
                    max_load = %metrics.max_load,
                    min_load = %metrics.min_load,
                    "cluster imbalance metrics"
                );
                
                // Decide if rebalancing is needed
                if !self.should_rebalance(&metrics, &config) {
                    debug!("cluster is balanced, no action needed");
                    continue;
                }
                
                info!(
                    cv = %metrics.coefficient_of_variation,
                    overloaded = ?metrics.overloaded_brokers,
                    underloaded = ?metrics.underloaded_brokers,
                    "cluster imbalance detected, initiating rebalancing"
                );
                
                // Select topics to move
                let moves = match self.select_rebalancing_candidates(&metrics, &config).await {
                    Ok(m) => m,
                    Err(e) => {
                        error!(error = %e, "failed to select rebalancing candidates");
                        continue;
                    }
                };
                
                if moves.is_empty() {
                    warn!("no suitable topics found for rebalancing");
                    continue;
                }
                
                info!(move_count = moves.len(), "executing rebalancing moves");
                
                // Execute rebalancing
                match self.execute_rebalancing(moves, &config, &mut history).await {
                    Ok(executed) => {
                        info!(executed = executed, "rebalancing cycle completed");
                    }
                    Err(e) => {
                        error!(error = %e, "rebalancing execution failed");
                    }
                }
            }
        })
    }
}
```

**Verification:**
- [ ] Loop runs at configured interval
- [ ] Only leader executes rebalancing
- [ ] Metrics are logged
- [ ] Errors are handled gracefully

---

### Step 8: Add Prometheus Metrics
**Status:** ☐ Not Started

**Task:** Export rebalancing metrics

```rust
// In broker_metrics.rs

use metrics::{describe_counter, describe_gauge, describe_histogram};

pub const REBALANCING_MOVES_TOTAL: &str = "danube_rebalancing_moves_total";
pub const REBALANCING_FAILURES_TOTAL: &str = "danube_rebalancing_failures_total";
pub const CLUSTER_IMBALANCE: &str = "danube_cluster_imbalance";
pub const REBALANCING_LATENCY_SECONDS: &str = "danube_rebalancing_latency_seconds";

pub fn register_rebalancing_metrics() {
    describe_counter!(
        REBALANCING_MOVES_TOTAL,
        "Total number of topic rebalancing moves"
    );
    
    describe_counter!(
        REBALANCING_FAILURES_TOTAL,
        "Total number of failed rebalancing attempts"
    );
    
    describe_gauge!(
        CLUSTER_IMBALANCE,
        "Current cluster load imbalance coefficient"
    );
    
    describe_histogram!(
        REBALANCING_LATENCY_SECONDS,
        "Latency of rebalancing operations"
    );
}
```

**Update rebalancing code:**
```rust
use metrics::{counter, gauge, histogram};
use crate::broker_metrics::*;

impl LoadManager {
    async fn execute_rebalancing(&mut self, ...) -> Result<usize> {
        let start = Instant::now();
        
        // ... existing logic
        
        for mv in moves {
            match self.execute_single_move(&mv).await {
                Ok(()) => {
                    executed += 1;
                    counter!(REBALANCING_MOVES_TOTAL, "reason" => format!("{:?}", mv.reason))
                        .increment(1);
                    
                    history.record_move(mv.clone());
                    self.log_rebalancing_event(&mv).await;
                    
                    tokio::time::sleep(Duration::from_secs(config.cooldown_seconds)).await;
                }
                Err(e) => {
                    counter!(REBALANCING_FAILURES_TOTAL).increment(1);
                    error!(topic = %mv.topic_name, error = %e, "failed to execute move");
                }
            }
        }
        
        histogram!(REBALANCING_LATENCY_SECONDS).record(start.elapsed().as_secs_f64());
        
        Ok(executed)
    }
    
    async fn calculate_imbalance(&self) -> Result<ImbalanceMetrics> {
        let metrics = // ... calculation
        
        gauge!(CLUSTER_IMBALANCE).set(metrics.coefficient_of_variation);
        
        Ok(metrics)
    }
}
```

**Verification:**
- [ ] Metrics are exported
- [ ] Prometheus scraping works
- [ ] Grafana dashboards show data

---

### Step 9: Add Admin CLI Commands
**Status:** ☐ Not Started

**Task:** Create manual rebalancing commands

```rust
// In danube-admin-cli/src/cluster.rs

pub async fn handle_cluster_command(matches: &ArgMatches, config: &Config) -> Result<()> {
    match matches.subcommand() {
        Some(("rebalance", rebalance_matches)) => {
            handle_rebalance(rebalance_matches, config).await
        }
        Some(("balance", balance_matches)) => {
            handle_show_balance(balance_matches, config).await
        }
        _ => Err(anyhow::anyhow!("Unknown cluster command")),
    }
}

async fn handle_rebalance(matches: &ArgMatches, config: &Config) -> Result<()> {
    let strategy = matches.get_one::<String>("strategy");
    let dry_run = matches.get_flag("dry-run");
    let max_moves = matches.get_one::<usize>("max-moves").copied().unwrap_or(5);
    
    // Get broker admin client
    let admin_addr = &config.admin_service_url;
    let client = DanubeAdminClient::connect(admin_addr.clone()).await?;
    
    let request = RebalanceRequest {
        strategy: strategy.cloned(),
        dry_run,
        max_moves,
    };
    
    let response = client.trigger_rebalance(request).await?;
    
    if dry_run {
        println!("Dry run - would move {} topics:", response.proposed_moves.len());
        for mv in response.proposed_moves {
            println!(
                "  {} from broker {} to broker {} (load: {:.2})",
                mv.topic_name, mv.from_broker, mv.to_broker, mv.estimated_load
            );
        }
    } else {
        println!("Rebalancing initiated - {} moves", response.executed_moves);
    }
    
    Ok(())
}

async fn handle_show_balance(matches: &ArgMatches, config: &Config) -> Result<()> {
    let admin_addr = &config.admin_service_url;
    let client = DanubeAdminClient::connect(admin_addr.clone()).await?;
    
    let response = client.get_cluster_balance().await?;
    
    println!("Cluster Balance Report");
    println!("======================");
    println!("Brokers: {}", response.broker_count);
    println!("Imbalance CV: {:.2}%", response.imbalance * 100.0);
    println!("Mean Load: {:.2}", response.mean_load);
    println!("Max Load: {:.2}", response.max_load);
    println!("Min Load: {:.2}", response.min_load);
    println!();
    println!("Broker Details:");
    for broker in response.brokers {
        println!(
            "  Broker {}: load={:.2}, topics={}, status={}",
            broker.id, broker.load, broker.topic_count, broker.status
        );
    }
    
    Ok(())
}
```

**Add to CLI:**
```bash
# Usage examples
danube-admin-cli cluster rebalance --strategy aggressive
danube-admin-cli cluster rebalance --dry-run
danube-admin-cli cluster rebalance --max-moves 10
danube-admin-cli cluster balance
```

**Verification:**
- [ ] CLI commands work
- [ ] Dry-run shows proposed moves
- [ ] Balance report is accurate

---

### Step 10: Add Comprehensive Tests
**Status:** ☐ Not Started

**Task:** Create tests for rebalancing logic

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_imbalance_detection() {
        let lm = create_test_load_manager().await;
        
        // Add imbalanced brokers
        add_broker(&lm, 1, 10.0).await;
        add_broker(&lm, 2, 50.0).await;
        add_broker(&lm, 3, 90.0).await;
        
        let metrics = lm.calculate_imbalance().await.unwrap();
        
        assert!(metrics.coefficient_of_variation > 0.3);
        assert_eq!(metrics.overloaded_brokers.len(), 1);
        assert_eq!(metrics.overloaded_brokers[0], 3);
    }
    
    #[tokio::test]
    async fn test_candidate_selection() {
        let lm = create_test_load_manager().await;
        let config = RebalancingConfig::default();
        
        // Setup imbalanced cluster
        setup_imbalanced_cluster(&lm).await;
        
        let metrics = lm.calculate_imbalance().await.unwrap();
        let candidates = lm.select_rebalancing_candidates(&metrics, &config).await.unwrap();
        
        assert!(!candidates.is_empty());
        assert!(candidates.len() <= config.max_moves_per_cycle);
        
        // Verify lightest topics selected first
        for i in 1..candidates.len() {
            assert!(candidates[i-1].estimated_load <= candidates[i].estimated_load);
        }
    }
    
    #[tokio::test]
    async fn test_rate_limiting() {
        let mut history = RebalancingHistory::new(100);
        let config = RebalancingConfig {
            max_moves_per_hour: 5,
            ..Default::default()
        };
        
        // Add 5 moves in the last hour
        for i in 0..5 {
            history.record_move(create_test_move(i));
        }
        
        assert_eq!(history.count_moves_in_last_hour(), 5);
        
        // Should not allow more moves
        let lm = create_test_load_manager().await;
        let moves = vec![create_test_move(6)];
        let result = lm.execute_rebalancing(moves, &config, &mut history).await.unwrap();
        
        assert_eq!(result, 0); // No moves executed due to rate limit
    }
    
    #[tokio::test]
    async fn test_blacklist_topics() {
        let lm = create_test_load_manager().await;
        let config = RebalancingConfig {
            blacklist_topics: vec!["/default/important".to_string()],
            ..Default::default()
        };
        
        setup_cluster_with_blacklisted_topic(&lm).await;
        
        let metrics = lm.calculate_imbalance().await.unwrap();
        let candidates = lm.select_rebalancing_candidates(&metrics, &config).await.unwrap();
        
        // Verify blacklisted topic is not in candidates
        assert!(!candidates.iter().any(|m| m.topic_name == "/default/important"));
    }
}
```

**Verification:**
- [ ] All tests pass
- [ ] Edge cases covered
- [ ] Integration tests added

---

## ✅ Success Criteria

Phase 3 is complete when:

- [ ] All 10 implementation steps marked complete
- [ ] Automated rebalancing works
- [ ] Manual rebalancing via CLI works
- [ ] Rate limiting prevents storms
- [ ] Metrics are exported
- [ ] Safety mechanisms work (blacklist, age filter, cooldown)
- [ ] Integration with existing topic move works
- [ ] Tests pass with >80% coverage
- [ ] Documentation is complete
- [ ] Production testing shows <5% cluster churn

---

## 🧪 Testing Checklist

- [ ] Unit tests for imbalance calculation
- [ ] Unit tests for candidate selection
- [ ] Unit tests for rate limiting
- [ ] Integration tests with real cluster
- [ ] Chaos tests (broker failures during rebalancing)
- [ ] Load tests (1000+ topics)
- [ ] Safety tests (blacklist, age filter)
- [ ] CLI command tests

---

## 📊 Metrics to Track

- Rebalancing frequency (moves/hour)
- Cluster imbalance coefficient
- Rebalancing latency
- Failed rebalancing attempts
- Topic move success rate
- Cluster stability (topic churn rate)

---

## 🔗 Dependencies

- Phase 1 (Resource Monitoring)
- Phase 2 (Intelligent Ranking)
- Existing topic move workflow (sealed state)
- Leader election service
- Admin CLI framework

---

## ⚠️ Safety Considerations

1. **Start with `enabled: false`** - Enable after testing
2. **Conservative first** - Use conservative strategy initially
3. **Monitor closely** - Watch for rebalancing storms
4. **Blacklist critical topics** - Don't move system topics
5. **Test in staging** - Verify behavior before production
6. **Have rollback plan** - Can disable via config update

---

## 📝 Notes

- Rebalancing uses existing topic move mechanism (graceful)
- Only leader performs rebalancing (prevents conflicts)
- Rate limiting prevents cascading failures
- Dry-run mode for testing
- History logged to ETCD for audit trail
- Metrics exported for monitoring
