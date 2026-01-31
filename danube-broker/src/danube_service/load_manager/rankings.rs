use crate::danube_service::load_report::{LoadReport, ResourceType, TopicLoad};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

// Simple Load Calculation: the load is just based on the number of topics.
pub(super) async fn rankings_simple(
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

/// Calculate weighted topic load score
/// Combines topic count, throughput, connections, and backlog
fn calculate_weighted_topic_load(topics: &[TopicLoad]) -> f64 {
    if topics.is_empty() {
        return 0.0;
    }

    // Topic count baseline
    let count_score = topics.len() as f64 * 0.2;

    // Throughput score (MB/s)
    let throughput_score: f64 = topics.iter().map(|t| t.byte_rate_mbps).sum::<f64>() * 0.3;

    // Connection score (producers + consumers)
    let connection_score: f64 = topics
        .iter()
        .map(|t| (t.producer_count + t.consumer_count) as f64)
        .sum::<f64>()
        * 0.3;

    // Backlog penalty
    let backlog_score: f64 = topics
        .iter()
        .map(|t| t.backlog_messages as f64)
        .sum::<f64>()
        / 10_000.0
        * 0.2;

    count_score + throughput_score + connection_score + backlog_score
}

/// Balanced ranking algorithm (default)
/// Combines weighted topic load (30%) with system resources (70%)
/// - Topic load: count + throughput + connections + backlog
/// - System: CPU (35%) + Memory (35%)
///
/// When no system resource data is available (early cluster startup or no load reports),
/// falls back to topic-count-based scoring to ensure fair distribution.
pub(super) async fn rankings_composite(
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
) -> Vec<(u64, usize)> {
    let brokers_usage = brokers_usage.lock().await;

    // First pass: check if ANY broker has CPU/memory data
    // If not, we fall back to topic-count-based scoring
    let has_resource_data = brokers_usage.values().any(|report| {
        report.resources_usage.iter().any(|sys| {
            matches!(sys.resource, ResourceType::CPU | ResourceType::Memory) && sys.usage > 0.0
        })
    });

    let mut broker_loads: Vec<(u64, usize)> = brokers_usage
        .iter()
        .map(|(&broker_id, load_report)| {
            if !has_resource_data {
                // No system resource data available (early startup, no load reports yet)
                // Fall back to pure topic count for fair distribution
                // This ensures topics are evenly distributed before load reports arrive
                (broker_id, load_report.topics.len())
            } else {
                // Normal balanced scoring with system resources
                // Weighted topic load (not just count!)
                let topic_load = calculate_weighted_topic_load(&load_report.topics) * 0.3;

                let mut cpu_load = 0.0;
                let mut memory_load = 0.0;

                for system_load in &load_report.resources_usage {
                    match system_load.resource {
                        ResourceType::CPU => cpu_load = system_load.usage * 0.35,
                        ResourceType::Memory => memory_load = system_load.usage * 0.35,
                        ResourceType::DiskIO | ResourceType::NetworkIO => {
                            // Reserved for weighted_load strategy
                        }
                    }
                }

                let total_load = (topic_load + cpu_load + memory_load) as usize;
                (broker_id, total_load)
            }
        })
        .collect();

    broker_loads.sort_by_key(|&(_, load)| load);

    broker_loads
}

/// Adaptive weighted load ranking (smart strategy)
/// Automatically detects which resources are under pressure and prioritizes them
///
/// Algorithm:
/// 1. Normalize all metrics against cluster max values
/// 2. For each broker, find the highest utilization metric (bottleneck)
/// 3. Prioritize that bottleneck in scoring
///
/// This adapts to workload patterns:
/// - CPU-bound workloads → prioritize CPU in scoring
/// - Throughput-heavy → prioritize bandwidth
/// - Connection-heavy → prioritize connection count
///
/// Returns list of (broker_id, score) sorted by score (ascending = less loaded)
pub(super) async fn rankings_weighted_load(
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
) -> Vec<(u64, usize)> {
    let brokers = brokers_usage.lock().await;

    if brokers.is_empty() {
        return vec![];
    }

    // Find max values for normalization
    let max_values = calculate_max_values(&brokers);

    let mut broker_scores: Vec<(u64, usize)> = brokers
        .iter()
        .map(|(&broker_id, report)| {
            // Calculate utilization for each metric (0.0 to 1.0)
            let mut cpu_util = 0.0;
            let mut mem_util = 0.0;
            let mut disk_util = 0.0;
            let mut net_util = 0.0;

            for sys_load in &report.resources_usage {
                match sys_load.resource {
                    ResourceType::CPU => {
                        cpu_util = if max_values.cpu > 0.0 {
                            sys_load.usage / max_values.cpu
                        } else {
                            0.0
                        };
                    }
                    ResourceType::Memory => {
                        mem_util = if max_values.memory > 0.0 {
                            sys_load.usage / max_values.memory
                        } else {
                            0.0
                        };
                    }
                    ResourceType::DiskIO => {
                        disk_util = if max_values.disk_io > 0.0 {
                            sys_load.usage / max_values.disk_io
                        } else {
                            0.0
                        };
                    }
                    ResourceType::NetworkIO => {
                        net_util = if max_values.network_io > 0.0 {
                            sys_load.usage / max_values.network_io
                        } else {
                            0.0
                        };
                    }
                }
            }

            // Throughput utilization
            let throughput_util = if max_values.throughput > 0.0 {
                report.total_throughput_mbps / max_values.throughput
            } else {
                0.0
            };

            // Weighted topic load utilization
            let topic_load = calculate_weighted_topic_load(&report.topics);
            let topic_util = if max_values.topic_load > 0.0 {
                topic_load / max_values.topic_load
            } else {
                0.0
            };

            // Find the bottleneck (highest utilization)
            let max_util = cpu_util
                .max(mem_util)
                .max(disk_util)
                .max(net_util)
                .max(throughput_util)
                .max(topic_util);

            // Adaptive scoring: Prioritize bottleneck resource
            // If a resource is >70% utilized, give it extra weight
            let score = if max_util > 0.7 {
                match max_util {
                    util if util == cpu_util => {
                        cpu_util * 0.5 + mem_util * 0.2 + throughput_util * 0.15 + topic_util * 0.15
                    }
                    util if util == mem_util => {
                        mem_util * 0.5 + cpu_util * 0.2 + throughput_util * 0.15 + topic_util * 0.15
                    }
                    util if util == throughput_util => {
                        throughput_util * 0.5 + net_util * 0.2 + cpu_util * 0.15 + mem_util * 0.15
                    }
                    util if util == net_util => {
                        net_util * 0.5 + throughput_util * 0.2 + cpu_util * 0.15 + mem_util * 0.15
                    }
                    _ => topic_util * 0.5 + cpu_util * 0.2 + mem_util * 0.2 + throughput_util * 0.1,
                }
            } else {
                topic_util * 0.25
                    + cpu_util * 0.25
                    + mem_util * 0.25
                    + throughput_util * 0.15
                    + net_util * 0.1
            };

            (broker_id, (score * 100.0) as usize)
        })
        .collect();

    // Sort by score (ascending - lower is better)
    broker_scores.sort_by_key(|&(_, score)| score);

    broker_scores
}

/// Maximum values across all brokers for normalization
struct MaxValues {
    cpu: f64,
    memory: f64,
    disk_io: f64,
    network_io: f64,
    throughput: f64,
    topic_load: f64, // Weighted topic load (not just count)
}

/// Calculate maximum values across all brokers for normalization
fn calculate_max_values(brokers: &HashMap<u64, LoadReport>) -> MaxValues {
    let mut max = MaxValues {
        cpu: 1.0,
        memory: 1.0,
        disk_io: 1.0,
        network_io: 1.0,
        throughput: 1.0,
        topic_load: 1.0,
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

        // Weighted topic load
        let topic_load = calculate_weighted_topic_load(&report.topics);
        max.topic_load = max.topic_load.max(topic_load);
    }

    max
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::danube_service::{
        load_manager::LoadManager,
        load_report::{LoadReport, ResourceType, SystemLoad, TopicLoad},
    };
    use danube_metadata_store::{MemoryStore, MetadataStorage};

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
        let meta_store = MetadataStorage::InMemory(
            MemoryStore::new()
                .await
                .expect("Failed to create memory store"),
        );
        let load_manager = LoadManager::new(1, meta_store);

        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(50.0, 30.0, 10));

        // Call ranking function directly to test it
        let broker_loads = rankings_composite(load_manager.brokers_usage.clone()).await;
        *load_manager.rankings.lock().await = broker_loads;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 1);
        // New formula: (topic_load×0.3) + (CPU×0.35) + (Memory×0.35)
        // topic_load = 10×0.2 (count only, no throughput/connections) = 2
        // score = (2×0.3) + (50×0.35) + (30×0.35) = 0.6 + 17.5 + 10.5 = 28.6 ≈ 28
        assert_eq!(rankings[0], (1, 28));
    }

    #[tokio::test]
    async fn test_multiple_brokers() {
        let meta_store = MetadataStorage::InMemory(
            MemoryStore::new()
                .await
                .expect("Failed to create memory store"),
        );
        let load_manager = LoadManager::new(1, meta_store);

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

        // Call ranking function directly to test it
        let broker_loads = rankings_composite(load_manager.brokers_usage.clone()).await;
        *load_manager.rankings.lock().await = broker_loads;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 3);
        // New formula: (topic_load×0.3) + (CPU×0.35) + (Memory×0.35)
        // Broker 3: (2×0.2×0.3) + (10×0.35) + (10×0.35) = 0.12 + 3.5 + 3.5 = 7.12 ≈ 7
        // Broker 2: (5×0.2×0.3) + (20×0.35) + (20×0.35) = 0.3 + 7.0 + 7.0 = 14.3 ≈ 14
        // Broker 1: (10×0.2×0.3) + (50×0.35) + (30×0.35) = 0.6 + 17.5 + 10.5 = 28.6 ≈ 28
        assert_eq!(rankings[0], (3, 7));
        assert_eq!(rankings[1], (2, 14));
        assert_eq!(rankings[2], (1, 28));
    }

    #[tokio::test]
    async fn test_empty_brokers() {
        let meta_store = MetadataStorage::InMemory(
            MemoryStore::new()
                .await
                .expect("Failed to create memory store"),
        );
        let load_manager = LoadManager::new(1, meta_store);

        // Call ranking function directly to test it
        let broker_loads = rankings_composite(load_manager.brokers_usage.clone()).await;
        *load_manager.rankings.lock().await = broker_loads;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 0);
    }

    #[tokio::test]
    async fn test_same_load_brokers() {
        let meta_store = MetadataStorage::InMemory(
            MemoryStore::new()
                .await
                .expect("Failed to create memory store"),
        );
        let load_manager = LoadManager::new(1, meta_store);

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

        // Call ranking function directly to test it
        let broker_loads = rankings_composite(load_manager.brokers_usage.clone()).await;
        *load_manager.rankings.lock().await = broker_loads;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 3);
        // All brokers have the same load (new formula)
        // (5×0.2×0.3) + (10×0.35) + (10×0.35) = 0.3 + 3.5 + 3.5 = 7.3 ≈ 7
        for ranking in rankings.iter() {
            assert_eq!(ranking.1, 7);
        }
    }
}
