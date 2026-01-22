use crate::load_report::{LoadReport, ResourceType};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Simple Load Calculation: the load is just based on the number of topics.
///
/// ## Algorithm
/// Ranks brokers purely by topic count (ascending order).
/// Brokers with fewer topics are ranked higher (lower score).
///
/// ## Returns
/// Vector of (broker_id, load_score) tuples sorted by load (ascending)
pub async fn rankings_simple(
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
) -> Vec<(u64, usize)> {
    let brokers_usage = brokers_usage.lock().await;

    let mut broker_loads: Vec<(u64, usize)> = brokers_usage
        .iter()
        .map(|(&broker_id, load_report)| (broker_id, load_report.topics_len))
        .collect();

    broker_loads.sort_by_key(|&(_, load)| load);

    broker_loads
}

/// Composite Load Calculation: the load is based on the number of topics, CPU usage, and memory usage.
///
/// ## Algorithm
/// Calculates a weighted composite score considering:
/// - Topic count (weight: 1.0)
/// - CPU usage (weight: 0.5)
/// - Memory usage (weight: 0.5)
///
/// ## Returns
/// Vector of (broker_id, composite_load_score) tuples sorted by load (ascending)
pub async fn rankings_composite(
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
) -> Vec<(u64, usize)> {
    let brokers_usage = brokers_usage.lock().await;

    let weights = (1.0, 0.5, 0.5); // (topics_weight, cpu_weight, memory_weight)

    let mut broker_loads: Vec<(u64, usize)> = brokers_usage
        .iter()
        .map(|(&broker_id, load_report)| {
            let topics_load = load_report.topics_len as f64 * weights.0;
            let mut cpu_load = 0.0;
            let mut memory_load = 0.0;

            for system_load in &load_report.resources_usage {
                match system_load.resource {
                    ResourceType::CPU => cpu_load += system_load.usage as f64 * weights.1,
                    ResourceType::Memory => memory_load += system_load.usage as f64 * weights.2,
                }
            }

            let total_load = (topics_load + cpu_load + memory_load) as usize;
            (broker_id, total_load)
        })
        .collect();

    broker_loads.sort_by_key(|&(_, load)| load);

    broker_loads
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load_report::SystemLoad;

    fn create_load_report(cpu_usage: usize, memory_usage: usize, topics_len: usize) -> LoadReport {
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
            topics_len,
            topic_list: vec!["/default/topic_name".to_string()],
        }
    }

    #[tokio::test]
    async fn test_single_broker_composite() {
        let brokers_usage = Arc::new(Mutex::new(HashMap::new()));
        brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(50, 30, 10));

        let rankings = rankings_composite(brokers_usage).await;

        assert_eq!(rankings.len(), 1);
        assert_eq!(rankings[0], (1, 50)); // 10 * 1.0 + 50 * 0.5 + 30 * 0.5 = 50
    }

    #[tokio::test]
    async fn test_multiple_brokers_composite() {
        let brokers_usage = Arc::new(Mutex::new(HashMap::new()));
        brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(50, 30, 10));
        brokers_usage
            .lock()
            .await
            .insert(2, create_load_report(20, 20, 5));
        brokers_usage
            .lock()
            .await
            .insert(3, create_load_report(10, 10, 2));

        let rankings = rankings_composite(brokers_usage).await;

        assert_eq!(rankings.len(), 3);
        assert_eq!(rankings[0], (3, 12)); // 2 * 1.0 + 10 * 0.5 + 10 * 0.5 = 12
        assert_eq!(rankings[1], (2, 25)); // 5 * 1.0 + 20 * 0.5 + 20 * 0.5 = 25
        assert_eq!(rankings[2], (1, 50)); // 10 * 1.0 + 50 * 0.5 + 30 * 0.5 = 50
    }

    #[tokio::test]
    async fn test_empty_brokers_usage() {
        let brokers_usage = Arc::new(Mutex::new(HashMap::new()));
        let rankings = rankings_composite(brokers_usage).await;
        assert_eq!(rankings.len(), 0);
    }

    #[tokio::test]
    async fn test_same_load_brokers() {
        let brokers_usage = Arc::new(Mutex::new(HashMap::new()));
        brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(10, 10, 5)); // Load: 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
        brokers_usage
            .lock()
            .await
            .insert(2, create_load_report(10, 10, 5));
        brokers_usage
            .lock()
            .await
            .insert(3, create_load_report(10, 10, 5));

        let rankings = rankings_composite(brokers_usage).await;

        assert_eq!(rankings.len(), 3);
        assert!(rankings.contains(&(1, 15)));
        assert!(rankings.contains(&(2, 15)));
        assert!(rankings.contains(&(3, 15)));
    }

    #[tokio::test]
    async fn test_rankings_simple() {
        let brokers_usage = Arc::new(Mutex::new(HashMap::new()));
        brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(0, 0, 10));
        brokers_usage
            .lock()
            .await
            .insert(2, create_load_report(0, 0, 5));
        brokers_usage
            .lock()
            .await
            .insert(3, create_load_report(0, 0, 2));

        let rankings = rankings_simple(brokers_usage).await;

        assert_eq!(rankings.len(), 3);
        assert_eq!(rankings[0], (3, 2)); // Fewest topics
        assert_eq!(rankings[1], (2, 5));
        assert_eq!(rankings[2], (1, 10)); // Most topics
    }
}
