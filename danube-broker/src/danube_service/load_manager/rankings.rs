use super::load_report::{LoadReport, ResourceType};
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

// Simple Load Calculation: the load is just based on the number of topics.
pub(crate) async fn rankings_simple(
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
) -> Vec<(u64, usize)> {
    let brokers_usage = brokers_usage.lock().await;

    let mut broker_loads: Vec<(u64, usize)> = brokers_usage
        .iter()
        .map(|(&broker_id, load_report)| (broker_id, load_report.topics.len()))
        .collect();

    broker_loads.sort_by_key(|&(_, load)| load);

    broker_loads
}

// Composite Load Calculation: the load is based on the number of topics, CPU usage, and memory usage.
#[allow(dead_code)]
pub(crate) async fn rankings_composite(
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
) -> Vec<(u64, usize)> {
    let brokers_usage = brokers_usage.lock().await;

    let weights = (1.0, 0.5, 0.5); // (topics_weight, cpu_weight, memory_weight)

    let mut broker_loads: Vec<(u64, usize)> = brokers_usage
        .iter()
        .map(|(&broker_id, load_report)| {
            let topics_load = load_report.topics.len() as f64 * weights.0;
            let mut cpu_load = 0.0;
            let mut memory_load = 0.0;

            for system_load in &load_report.resources_usage {
                match system_load.resource {
                    ResourceType::CPU => cpu_load += system_load.usage * weights.1,
                    ResourceType::Memory => memory_load += system_load.usage * weights.2,
                    ResourceType::DiskIO => {
                        // DiskIO can contribute to load score in future phases
                    }
                    ResourceType::NetworkIO => {
                        // NetworkIO can contribute to load score in future phases
                    }
                }
            }

            let total_load = (topics_load + cpu_load + memory_load) as usize;
            (broker_id, total_load)
        })
        .collect();

    broker_loads.sort_by_key(|&(_, load)| load);

    broker_loads
}

/// Configurable weights for weighted composite ranking
#[derive(Debug, Clone, Deserialize, Serialize)]
/// Reserved for future advanced load balancing
#[allow(dead_code)]
pub(crate) struct RankingWeights {
    pub cpu: f64,
    pub memory: f64,
    pub disk_io: f64,
    pub network_io: f64,
    pub throughput: f64,
    pub topic_count: f64,
}

impl Default for RankingWeights {
    fn default() -> Self {
        Self {
            cpu: 0.30,
            memory: 0.25,
            disk_io: 0.15,
            network_io: 0.10,
            throughput: 0.15,
            topic_count: 0.05,
        }
    }
}

/// Weighted composite ranking algorithm (Phase 3 - Future enhancement)
/// Calculates broker rankings using multiple weighted factors:
/// - Topic count (30%)
/// - Total throughput in MB/s (40%)
/// - Total connections (20%)
/// - Backlog messages (10% penalty)
///
/// Reserved for future advanced load balancing (Phase 3.1)
///
/// Returns list of (broker_id, score) sorted by score (ascending = less loaded)
#[allow(dead_code)]
pub(crate) async fn rankings_weighted_composite(
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
    weights: RankingWeights,
) -> Vec<(u64, f64)> {
    let brokers = brokers_usage.lock().await;

    if brokers.is_empty() {
        return vec![];
    }

    // Find max values for normalization
    let max_values = calculate_max_values(&brokers);

    let mut broker_scores: Vec<(u64, f64)> = brokers
        .iter()
        .map(|(&broker_id, report)| {
            let score = calculate_broker_score(report, &weights, &max_values);
            (broker_id, score)
        })
        .collect();

    // Sort by score (ascending - lower is better for assignment)
    broker_scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    broker_scores
}

/// Reserved for future advanced load balancing
#[allow(dead_code)]
struct MaxValues {
    cpu: f64,
    memory: f64,
    disk_io: f64,
    network_io: f64,
    throughput: f64,
    topic_count: f64,
}

/// Reserved for future advanced load balancing
#[allow(dead_code)]
fn calculate_max_values(brokers: &HashMap<u64, LoadReport>) -> MaxValues {
    let mut max = MaxValues {
        cpu: 1.0,
        memory: 1.0,
        disk_io: 1.0,
        network_io: 1.0,
        throughput: 1.0,
        topic_count: 1.0,
    };

    for report in brokers.values() {
        // Extract system metrics
        for sys_load in &report.resources_usage {
            match sys_load.resource {
                ResourceType::CPU => max.cpu = max.cpu.max(sys_load.usage),
                ResourceType::Memory => max.memory = max.memory.max(sys_load.usage),
                ResourceType::DiskIO => max.disk_io = max.disk_io.max(sys_load.usage),
                ResourceType::NetworkIO => max.network_io = max.network_io.max(sys_load.usage),
            }
        }

        // Aggregate metrics
        max.throughput = max.throughput.max(report.total_throughput_mbps);
        max.topic_count = max.topic_count.max(report.topics.len() as f64);
    }

    max
}

/// Reserved for future advanced load balancing
#[allow(dead_code)]
fn calculate_broker_score(
    report: &LoadReport,
    weights: &RankingWeights,
    max_values: &MaxValues,
) -> f64 {
    let mut score = 0.0;

    // Normalize and weight each metric
    for sys_load in &report.resources_usage {
        match sys_load.resource {
            ResourceType::CPU => {
                score += normalize(sys_load.usage, max_values.cpu) * weights.cpu;
            }
            ResourceType::Memory => {
                score += normalize(sys_load.usage, max_values.memory) * weights.memory;
            }
            ResourceType::DiskIO => {
                score += normalize(sys_load.usage, max_values.disk_io) * weights.disk_io;
            }
            ResourceType::NetworkIO => {
                score += normalize(sys_load.usage, max_values.network_io) * weights.network_io;
            }
        }
    }

    // Aggregates
    score += normalize(report.total_throughput_mbps, max_values.throughput) * weights.throughput;
    score += normalize(report.topics.len() as f64, max_values.topic_count) * weights.topic_count;

    score
}

/// Reserved for future advanced load balancing
#[allow(dead_code)]
fn normalize(value: f64, max: f64) -> f64 {
    if max == 0.0 {
        0.0
    } else {
        (value / max).min(1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::danube_service::load_manager::{
        load_report::{LoadReport, ResourceType, SystemLoad, TopicLoad},
        LoadManager,
    };
    use danube_metadata_store::{MemoryStore, MetadataStorage};
    use std::sync::atomic::AtomicU64;

    fn create_load_report(cpu_usage: f64, memory_usage: f64, topics_len: usize) -> LoadReport {
        LoadReport {
            resources_usage: vec![
                SystemLoad {
                    resource: ResourceType::CPU,
                    usage: cpu_usage,
                },
                SystemLoad {
                    resource: ResourceType::Memory,
                    usage: memory_usage,
                },
            ],
            topics: (0..topics_len)
                .map(|i| TopicLoad {
                    topic_name: format!("/default/topic_{}", i),
                    message_rate: 0,
                    byte_rate: 0,
                    byte_rate_mbps: 0.0,
                    producer_count: 0,
                    consumer_count: 0,
                    subscription_count: 0,
                    backlog_messages: 0,
                })
                .collect(),
            total_throughput_mbps: 0.0,
            total_message_rate: 0,
            total_lag_messages: 0,
            timestamp: 0,
            broker_id: 0,
        }
    }

    #[tokio::test]
    async fn test_single_broker() {
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::InMemory(
                MemoryStore::new().await.expect("use for testing"),
            ),
        };

        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(50.0, 30.0, 10));

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 1);
        assert_eq!(rankings[0], (1, 50)); // 10 * 1.0 + 50 * 0.5 + 30 * 0.5 = 75
    }

    #[tokio::test]
    async fn test_multiple_brokers() {
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::InMemory(
                MemoryStore::new().await.expect("use for testing"),
            ),
        };

        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(50.0, 30.0, 10));
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(2, create_load_report(20.0, 20.0, 5));
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(3, create_load_report(10.0, 10.0, 2));

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 3);
        assert_eq!(rankings[0], (3, 12)); // 2 * 1.0 + 10 * 0.5 + 10 * 0.5 = 12
        assert_eq!(rankings[1], (2, 25)); // 5 * 1.0 + 20 * 0.5 + 20 * 0.5 = 25
        assert_eq!(rankings[2], (1, 50)); // 10 * 1.0 + 50 * 0.5 + 30 * 0.5 = 75
    }

    #[tokio::test]
    async fn test_empty_brokers_usage() {
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::InMemory(
                MemoryStore::new().await.expect("use for testing"),
            ),
        };

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 0);
    }

    #[tokio::test]
    async fn test_same_load_brokers() {
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::InMemory(
                MemoryStore::new().await.expect("use for testing"),
            ),
        };

        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(10.0, 10.0, 5)); // Load: 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(2, create_load_report(10.0, 10.0, 5)); // Load: 15
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(3, create_load_report(10.0, 10.0, 5)); // Load: 15

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 3);
        assert!(rankings.contains(&(1, 15))); // 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
        assert!(rankings.contains(&(2, 15))); // 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
        assert!(rankings.contains(&(3, 15))); // 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
    }
}
