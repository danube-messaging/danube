# LoadManager: Additional Enhancements

**Priority:** MEDIUM  
**Timeline:** 2-3 weeks (can be done incrementally)  
**Status:** Not Started  
**Depends On:** Phases 1, 2, and 3

---

## 🎯 Goals

Beyond core LoadManager functionality, these enhancements improve:
- **Observability** - Comprehensive monitoring and debugging
- **Operational control** - Admin tools for manual intervention
- **Intelligence** - Smart topic placement strategies
- **Reliability** - Better failure handling and recovery

---

## 📋 Enhancement Categories

### A. Observability & Monitoring
### B. Admin CLI Enhancements
### C. Smart Topic Placement
### D. Historical Tracking & Analytics
### E. Performance Optimizations

---

## A. Observability & Monitoring

### Enhancement A1: Comprehensive Prometheus Metrics
**Status:** ☐ Not Started

**Task:** Export detailed LoadManager metrics

```rust
// In broker_metrics.rs

// Cluster-level metrics
pub const CLUSTER_IMBALANCE: &str = "danube_cluster_imbalance";
pub const CLUSTER_BROKER_COUNT: &str = "danube_cluster_broker_count";
pub const CLUSTER_TOPIC_COUNT: &str = "danube_cluster_topic_count";
pub const CLUSTER_TOTAL_THROUGHPUT_MBPS: &str = "danube_cluster_total_throughput_mbps";

// LoadManager metrics
pub const LOAD_MANAGER_RANKING_LATENCY_MS: &str = "danube_load_manager_ranking_latency_ms";
pub const LOAD_MANAGER_ASSIGNMENT_LATENCY_MS: &str = "danube_load_manager_assignment_latency_ms";
pub const LOAD_MANAGER_ASSIGNMENTS_TOTAL: &str = "danube_load_manager_assignments_total";

// Rebalancing metrics (from Phase 3)
pub const REBALANCING_MOVES_TOTAL: &str = "danube_rebalancing_moves_total";
pub const REBALANCING_FAILURES_TOTAL: &str = "danube_rebalancing_failures_total";
pub const REBALANCING_LATENCY_SECONDS: &str = "danube_rebalancing_latency_seconds";

// Per-broker metrics
pub const BROKER_LOAD_SCORE: &str = "danube_broker_load_score";
pub const BROKER_TOPIC_COUNT: &str = "danube_broker_topic_count";
pub const BROKER_THROUGHPUT_MBPS: &str = "danube_broker_throughput_mbps";
pub const BROKER_CPU_USAGE: &str = "danube_broker_cpu_usage";
pub const BROKER_MEMORY_USAGE: &str = "danube_broker_memory_usage";

pub fn register_load_manager_metrics() {
    describe_gauge!(CLUSTER_IMBALANCE, "Cluster load imbalance coefficient");
    describe_gauge!(CLUSTER_BROKER_COUNT, "Number of active brokers");
    describe_gauge!(CLUSTER_TOPIC_COUNT, "Total topics in cluster");
    describe_gauge!(CLUSTER_TOTAL_THROUGHPUT_MBPS, "Total cluster throughput");
    
    describe_histogram!(LOAD_MANAGER_RANKING_LATENCY_MS, "Time to calculate rankings");
    describe_histogram!(LOAD_MANAGER_ASSIGNMENT_LATENCY_MS, "Time to assign topic");
    describe_counter!(LOAD_MANAGER_ASSIGNMENTS_TOTAL, "Total topic assignments");
    
    describe_gauge!(BROKER_LOAD_SCORE, "Broker composite load score");
    describe_gauge!(BROKER_TOPIC_COUNT, "Number of topics on broker");
    describe_gauge!(BROKER_THROUGHPUT_MBPS, "Broker throughput in Mbps");
    describe_gauge!(BROKER_CPU_USAGE, "Broker CPU usage percentage");
    describe_gauge!(BROKER_MEMORY_USAGE, "Broker memory usage percentage");
}
```

**Update LoadManager to export metrics:**
```rust
impl LoadManager {
    async fn update_metrics(&self) {
        let brokers = self.brokers_usage.lock().await;
        let rankings = self.rankings.lock().await;
        
        // Cluster metrics
        gauge!(CLUSTER_BROKER_COUNT).set(brokers.len() as f64);
        
        let total_topics: usize = brokers.values().map(|r| r.topics.len()).sum();
        gauge!(CLUSTER_TOPIC_COUNT).set(total_topics as f64);
        
        let total_throughput: f64 = brokers.values().map(|r| r.total_throughput_mbps).sum();
        gauge!(CLUSTER_TOTAL_THROUGHPUT_MBPS).set(total_throughput);
        
        // Per-broker metrics
        for (broker_id, report) in brokers.iter() {
            let labels = [("broker_id", broker_id.to_string())];
            
            // Load score from rankings
            if let Some((_, score)) = rankings.iter().find(|(id, _)| id == broker_id) {
                gauge!(BROKER_LOAD_SCORE, &labels).set(*score);
            }
            
            gauge!(BROKER_TOPIC_COUNT, &labels).set(report.topics.len() as f64);
            gauge!(BROKER_THROUGHPUT_MBPS, &labels).set(report.total_throughput_mbps);
            
            // Resource metrics
            if let Some(cpu) = report.resources_usage.iter()
                .find(|r| matches!(r.resource, ResourceType::CPU))
            {
                gauge!(BROKER_CPU_USAGE, &labels).set(cpu.usage);
            }
            
            if let Some(mem) = report.resources_usage.iter()
                .find(|r| matches!(r.resource, ResourceType::Memory))
            {
                gauge!(BROKER_MEMORY_USAGE, &labels).set(mem.usage);
            }
        }
    }
    
    // Call from main loop
    pub async fn start_metrics_updater(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            
            loop {
                interval.tick().await;
                self.update_metrics().await;
            }
        });
    }
}
```

**Verification:**
- [ ] Metrics are exported
- [ ] Prometheus scrapes successfully
- [ ] Values are accurate
- [ ] No performance impact

---

### Enhancement A2: Grafana Dashboard
**Status:** ☐ Not Started

**Task:** Create Grafana dashboard JSON

**Create `grafana/load_manager_dashboard.json`:**
```json
{
  "title": "Danube LoadManager",
  "panels": [
    {
      "title": "Cluster Imbalance",
      "targets": [
        {
          "expr": "danube_cluster_imbalance"
        }
      ],
      "type": "graph"
    },
    {
      "title": "Broker Load Distribution",
      "targets": [
        {
          "expr": "danube_broker_load_score"
        }
      ],
      "type": "graph"
    },
    {
      "title": "Rebalancing Activity",
      "targets": [
        {
          "expr": "rate(danube_rebalancing_moves_total[5m])"
        }
      ],
      "type": "graph"
    },
    {
      "title": "Topic Assignment Rate",
      "targets": [
        {
          "expr": "rate(danube_load_manager_assignments_total[5m])"
        }
      ],
      "type": "graph"
    }
  ]
}
```

**Verification:**
- [ ] Dashboard imports into Grafana
- [ ] All panels show data
- [ ] Alerts are configured

---

### Enhancement A3: Structured Logging
**Status:** ☐ Not Started

**Task:** Add detailed debug logging

```rust
impl LoadManager {
    pub async fn log_cluster_state(&self) {
        let brokers = self.brokers_usage.lock().await;
        let rankings = self.rankings.lock().await;
        
        info!(
            target = "load_manager",
            broker_count = brokers.len(),
            topic_count = brokers.values().map(|r| r.topics.len()).sum::<usize>(),
            "cluster state snapshot"
        );
        
        for (rank, (broker_id, score)) in rankings.iter().enumerate() {
            if let Some(report) = brokers.get(broker_id) {
                info!(
                    target = "load_manager",
                    rank = rank,
                    broker_id = broker_id,
                    load_score = score,
                    topics = report.topics.len(),
                    throughput_mbps = report.total_throughput_mbps,
                    cpu = report.get_resource_usage(ResourceType::CPU),
                    memory = report.get_resource_usage(ResourceType::Memory),
                    "broker status"
                );
            }
        }
    }
}
```

**Verification:**
- [ ] Logs are structured JSON
- [ ] Useful for debugging
- [ ] Not too verbose

---

## B. Admin CLI Enhancements

### Enhancement B1: Load Report Inspection
**Status:** ☐ Not Started

**Task:** Add CLI command to view load reports

```bash
# Show all broker load reports
danube-admin-cli cluster load-reports

# Show specific broker
danube-admin-cli cluster load-reports --broker 1

# Show as JSON
danube-admin-cli cluster load-reports --format json
```

**Implementation:**
```rust
async fn handle_load_reports(matches: &ArgMatches, config: &Config) -> Result<()> {
    let client = DanubeAdminClient::connect(&config.admin_service_url).await?;
    
    let broker_id = matches.get_one::<u64>("broker");
    let format = matches.get_one::<String>("format").map(|s| s.as_str()).unwrap_or("table");
    
    let reports = client.get_load_reports(broker_id.copied()).await?;
    
    match format {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&reports)?);
        }
        "table" => {
            print_load_reports_table(&reports);
        }
        _ => return Err(anyhow::anyhow!("Unknown format: {}", format)),
    }
    
    Ok(())
}

fn print_load_reports_table(reports: &HashMap<u64, LoadReport>) {
    println!("{:<10} {:<8} {:<10} {:<10} {:<10} {:<15}", 
        "Broker", "Topics", "CPU%", "Memory%", "Throughput", "Disk GB");
    println!("{}", "-".repeat(70));
    
    for (broker_id, report) in reports {
        println!(
            "{:<10} {:<8} {:<10.1} {:<10.1} {:<10.2} {:<15.2}",
            broker_id,
            report.topics.len(),
            report.get_resource_usage(ResourceType::CPU),
            report.get_resource_usage(ResourceType::Memory),
            report.total_throughput_mbps,
            report.total_disk_usage_gb,
        );
    }
}
```

**Verification:**
- [ ] Command works
- [ ] Output is readable
- [ ] JSON format is valid

---

### Enhancement B2: Topic Move Management
**Status:** ☐ Not Started

**Task:** Manual topic move commands

```bash
# Move specific topic
danube-admin-cli topics move /default/my-topic --to-broker 2

# Move topic with dry-run
danube-admin-cli topics move /default/my-topic --to-broker 2 --dry-run

# View pending moves
danube-admin-cli topics moves --status pending
```

**Implementation:**
```rust
async fn handle_topic_move(matches: &ArgMatches, config: &Config) -> Result<()> {
    let topic = matches.get_one::<String>("topic").unwrap();
    let to_broker = matches.get_one::<u64>("to-broker").unwrap();
    let dry_run = matches.get_flag("dry-run");
    
    let client = DanubeAdminClient::connect(&config.admin_service_url).await?;
    
    if dry_run {
        let current_broker = client.get_topic_assignment(topic).await?;
        println!("Would move {} from broker {} to broker {}", 
            topic, current_broker, to_broker);
        return Ok(());
    }
    
    client.move_topic(topic, *to_broker).await?;
    println!("Topic move initiated for {}", topic);
    
    Ok(())
}
```

**Verification:**
- [ ] Move works correctly
- [ ] Dry-run doesn't execute
- [ ] Error handling works

---

### Enhancement B3: Rebalancing History
**Status:** ☐ Not Started

**Task:** View rebalancing history

```bash
# Show recent rebalancing events
danube-admin-cli cluster rebalancing-history --limit 20

# Show rebalancing stats
danube-admin-cli cluster rebalancing-stats
```

**Implementation:**
```rust
async fn handle_rebalancing_history(matches: &ArgMatches, config: &Config) -> Result<()> {
    let limit = matches.get_one::<usize>("limit").copied().unwrap_or(10);
    let client = DanubeAdminClient::connect(&config.admin_service_url).await?;
    
    let history = client.get_rebalancing_history(limit).await?;
    
    println!("{:<20} {:<30} {:<10} {:<10} {:<15}", 
        "Timestamp", "Topic", "From", "To", "Reason");
    println!("{}", "-".repeat(90));
    
    for event in history {
        let timestamp = format_timestamp(event.timestamp);
        println!(
            "{:<20} {:<30} {:<10} {:<10} {:<15}",
            timestamp, event.topic_name, event.from_broker, 
            event.to_broker, format!("{:?}", event.reason)
        );
    }
    
    Ok(())
}
```

**Verification:**
- [ ] History is displayed correctly
- [ ] Timestamps are readable
- [ ] Pagination works

---

## C. Smart Topic Placement

### Enhancement C1: Affinity/Anti-Affinity Rules
**Status:** ☐ Not Started

**Task:** Implement topic placement preferences

**Configuration:**
```yaml
load_manager:
  placement_rules:
    # Co-locate topics from same namespace
    namespace_affinity: true
    
    # Anti-affinity for replicas (future)
    replica_anti_affinity: true
    
    # Custom affinity groups
    affinity_groups:
      - name: "analytics"
        topics: ["/analytics/*"]
        prefer_brokers: [1, 2]
      
      - name: "realtime"
        topics: ["/realtime/*"]
        avoid_brokers: [3]
```

**Implementation:**
```rust
#[derive(Debug, Clone, Deserialize)]
pub struct PlacementRules {
    pub namespace_affinity: bool,
    pub replica_anti_affinity: bool,
    pub affinity_groups: Vec<AffinityGroup>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AffinityGroup {
    pub name: String,
    pub topics: Vec<String>,  // Glob patterns
    pub prefer_brokers: Option<Vec<u64>>,
    pub avoid_brokers: Option<Vec<u64>>,
}

impl LoadManager {
    async fn select_broker_with_affinity(
        &self,
        topic_name: &str,
        rules: &PlacementRules,
    ) -> u64 {
        let rankings = self.rankings.lock().await;
        
        // Check affinity groups
        for group in &rules.affinity_groups {
            if self.topic_matches_group(topic_name, &group.topics) {
                // Prefer specific brokers
                if let Some(preferred) = &group.prefer_brokers {
                    for (broker_id, _) in rankings.iter() {
                        if preferred.contains(broker_id) && self.is_broker_active(*broker_id).await {
                            return *broker_id;
                        }
                    }
                }
                
                // Avoid specific brokers
                if let Some(avoided) = &group.avoid_brokers {
                    for (broker_id, _) in rankings.iter() {
                        if !avoided.contains(broker_id) && self.is_broker_active(*broker_id).await {
                            return *broker_id;
                        }
                    }
                }
            }
        }
        
        // Namespace affinity
        if rules.namespace_affinity {
            if let Some(broker_id) = self.find_broker_with_namespace(topic_name).await {
                return broker_id;
            }
        }
        
        // Fallback to standard ranking
        self.get_next_broker().await
    }
    
    async fn find_broker_with_namespace(&self, topic_name: &str) -> Option<u64> {
        let namespace = extract_namespace(topic_name);
        let brokers = self.brokers_usage.lock().await;
        
        // Find broker with most topics in this namespace
        let mut best_broker = None;
        let mut best_count = 0;
        
        for (broker_id, report) in brokers.iter() {
            let count = report.topics.iter()
                .filter(|t| extract_namespace(&t.topic_name) == namespace)
                .count();
            
            if count > best_count {
                best_count = count;
                best_broker = Some(*broker_id);
            }
        }
        
        best_broker
    }
    
    fn topic_matches_group(&self, topic: &str, patterns: &[String]) -> bool {
        patterns.iter().any(|pattern| {
            glob::Pattern::new(pattern)
                .ok()
                .map(|p| p.matches(topic))
                .unwrap_or(false)
        })
    }
}

fn extract_namespace(topic_path: &str) -> &str {
    topic_path.split('/').nth(1).unwrap_or("default")
}
```

**Verification:**
- [ ] Affinity rules work
- [ ] Anti-affinity rules work
- [ ] Fallback to standard ranking works

---

### Enhancement C2: Resource-Based Hints
**Status:** ☐ Not Started

**Task:** Prefer brokers based on topic requirements

**Topic metadata:**
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicRequirements {
    pub estimated_throughput_mbps: Option<f64>,
    pub estimated_size_gb: Option<f64>,
    pub requires_low_latency: bool,
    pub cpu_intensive: bool,
}
```

**Placement logic:**
```rust
impl LoadManager {
    async fn select_broker_for_requirements(
        &self,
        requirements: &TopicRequirements,
    ) -> u64 {
        let rankings = self.rankings.lock().await;
        let brokers = self.brokers_usage.lock().await;
        
        // Filter brokers with sufficient resources
        let mut suitable: Vec<_> = rankings.iter()
            .filter(|(broker_id, _)| {
                if let Some(report) = brokers.get(broker_id) {
                    self.broker_meets_requirements(report, requirements)
                } else {
                    false
                }
            })
            .collect();
        
        // Sort by load
        suitable.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        // Return least loaded suitable broker
        suitable.first()
            .map(|(id, _)| *id)
            .unwrap_or_else(|| self.get_next_broker().await)
    }
    
    fn broker_meets_requirements(
        &self,
        report: &LoadReport,
        requirements: &TopicRequirements,
    ) -> bool {
        // Check CPU headroom
        if requirements.cpu_intensive {
            let cpu = report.get_resource_usage(ResourceType::CPU);
            if cpu > 70.0 {
                return false;
            }
        }
        
        // Check disk space
        if let Some(required_size) = requirements.estimated_size_gb {
            if report.total_disk_usage_gb + required_size > 1000.0 {
                return false;
            }
        }
        
        // Check throughput capacity
        if let Some(required_throughput) = requirements.estimated_throughput_mbps {
            if report.total_throughput_mbps + required_throughput > 1000.0 {
                return false;
            }
        }
        
        true
    }
}
```

**Verification:**
- [ ] Resource requirements are respected
- [ ] Fallback works when no suitable broker
- [ ] High-throughput topics go to capable brokers

---

## D. Historical Tracking & Analytics

### Enhancement D1: Load Trends Tracking
**Status:** ☐ Not Started

**Task:** Track historical load data

```rust
pub struct LoadTrends {
    broker_id: u64,
    samples: VecDeque<LoadSample>,
    max_samples: usize,
}

#[derive(Debug, Clone)]
pub struct LoadSample {
    timestamp: u64,
    cpu: f64,
    memory: f64,
    throughput: f64,
    topic_count: usize,
}

impl LoadTrends {
    pub fn new(broker_id: u64, max_samples: usize) -> Self {
        Self {
            broker_id,
            samples: VecDeque::with_capacity(max_samples),
            max_samples,
        }
    }
    
    pub fn record(&mut self, report: &LoadReport) {
        if self.samples.len() >= self.max_samples {
            self.samples.pop_front();
        }
        
        self.samples.push_back(LoadSample {
            timestamp: report.timestamp,
            cpu: report.get_resource_usage(ResourceType::CPU),
            memory: report.get_resource_usage(ResourceType::Memory),
            throughput: report.total_throughput_mbps,
            topic_count: report.topics.len(),
        });
    }
    
    pub fn get_trend(&self) -> Trend {
        if self.samples.len() < 2 {
            return Trend::Stable;
        }
        
        let recent_avg = self.average_load(10);
        let older_avg = self.average_load_offset(20, 10);
        
        if recent_avg > older_avg * 1.2 {
            Trend::Increasing
        } else if recent_avg < older_avg * 0.8 {
            Trend::Decreasing
        } else {
            Trend::Stable
        }
    }
    
    fn average_load(&self, count: usize) -> f64 {
        self.samples.iter()
            .rev()
            .take(count)
            .map(|s| s.cpu + s.memory / 2.0)
            .sum::<f64>() / count as f64
    }
}

pub enum Trend {
    Increasing,
    Decreasing,
    Stable,
}
```

**Verification:**
- [ ] Trends are calculated correctly
- [ ] Memory usage is bounded
- [ ] Trend detection is accurate

---

### Enhancement D2: Predictive Rebalancing
**Status:** ☐ Not Started

**Task:** Predict when rebalancing will be needed

```rust
impl LoadManager {
    pub async fn predict_imbalance(&self, trends: &HashMap<u64, LoadTrends>) -> Option<Duration> {
        let current_metrics = self.calculate_imbalance().await.ok()?;
        
        if current_metrics.coefficient_of_variation > 0.25 {
            return Some(Duration::from_secs(0)); // Already imbalanced
        }
        
        // Predict based on trends
        let mut diverging_count = 0;
        
        for (broker_id, trend_data) in trends {
            match trend_data.get_trend() {
                Trend::Increasing if current_metrics.overloaded_brokers.contains(broker_id) => {
                    diverging_count += 1;
                }
                Trend::Decreasing if current_metrics.underloaded_brokers.contains(broker_id) => {
                    diverging_count += 1;
                }
                _ => {}
            }
        }
        
        if diverging_count >= trends.len() / 3 {
            Some(Duration::from_secs(300)) // Predict imbalance in 5 minutes
        } else {
            None
        }
    }
}
```

**Verification:**
- [ ] Predictions are reasonably accurate
- [ ] Early warning helps prevent issues
- [ ] No false positives

---

## E. Performance Optimizations

### Enhancement E1: Ranking Cache
**Status:** ☐ Not Started

**Task:** Cache rankings to reduce computation

```rust
pub struct RankingCache {
    rankings: Arc<RwLock<Vec<(u64, f64)>>>,
    last_updated: Arc<RwLock<Instant>>,
    ttl: Duration,
}

impl RankingCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            rankings: Arc::new(RwLock::new(Vec::new())),
            last_updated: Arc::new(RwLock::new(Instant::now())),
            ttl,
        }
    }
    
    pub async fn get_or_calculate<F>(
        &self,
        calculate: F,
    ) -> Vec<(u64, f64)>
    where
        F: Future<Output = Vec<(u64, f64)>>,
    {
        let last = *self.last_updated.read().await;
        
        if last.elapsed() < self.ttl {
            // Return cached value
            return self.rankings.read().await.clone();
        }
        
        // Recalculate
        let new_rankings = calculate.await;
        
        *self.rankings.write().await = new_rankings.clone();
        *self.last_updated.write().await = Instant::now();
        
        new_rankings
    }
}
```

**Verification:**
- [ ] Cache reduces CPU usage
- [ ] TTL is respected
- [ ] No stale data issues

---

### Enhancement E2: Batch Load Report Processing
**Status:** ☐ Not Started

**Task:** Process multiple load reports efficiently

```rust
impl LoadManager {
    pub async fn process_batch_updates(
        &self,
        updates: Vec<(u64, LoadReport)>,
    ) -> Result<()> {
        let mut brokers = self.brokers_usage.lock().await;
        
        // Batch update
        for (broker_id, report) in updates {
            brokers.insert(broker_id, report);
        }
        
        drop(brokers);
        
        // Single ranking calculation for all updates
        self.calculate_rankings().await;
        
        Ok(())
    }
}
```

**Verification:**
- [ ] Batching reduces lock contention
- [ ] Performance improves with multiple updates
- [ ] No race conditions

---

## ✅ Success Criteria

Additional enhancements are complete when:

- [ ] Comprehensive metrics exported
- [ ] Grafana dashboard works
- [ ] Admin CLI commands functional
- [ ] Smart placement rules work
- [ ] Historical tracking operational
- [ ] Performance optimizations effective
- [ ] Documentation complete

---

## 📊 Priority Order

1. **High Priority** (Do first):
   - A1: Prometheus metrics
   - A2: Grafana dashboard
   - B1: Load report inspection
   
2. **Medium Priority** (Do second):
   - A3: Structured logging
   - B2: Topic move management
   - C1: Affinity rules
   
3. **Low Priority** (Do later):
   - D1: Load trends tracking
   - D2: Predictive rebalancing
   - E1: Ranking cache
   - E2: Batch processing

---

## 🧪 Testing

- [ ] Metrics accuracy tests
- [ ] CLI command tests
- [ ] Affinity rule tests
- [ ] Performance benchmarks
- [ ] Load tests

---

## 📝 Notes

- These enhancements can be implemented incrementally
- Start with observability (crucial for production)
- Smart placement is nice-to-have but valuable
- Historical tracking enables future ML-based optimization
- Performance optimizations should be data-driven
