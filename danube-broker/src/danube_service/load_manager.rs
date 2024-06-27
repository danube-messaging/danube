pub(crate) mod load_report;
use load_report::{LoadReport, ResourceType, SystemLoad};

use anyhow::{anyhow, Result};
use etcd_client::{Client, EventType, GetOptions};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task;
use tokio::time::{self, Duration};
use tracing::{error, info, trace, warn};

use crate::metadata_store::MetaOptions;
use crate::resources::BASE_REGISTER_PATH;
use crate::{
    metadata_store::{
        etcd_watch_prefixes, ETCDWatchEvent, EtcdMetadataStore, MetadataStorage, MetadataStore,
        MetadataStoreConfig,
    },
    resources::{
        BASE_BROKER_LOAD_PATH, BASE_BROKER_PATH, BASE_UNASSIGNED_PATH, LOADBALANCE_DECISION_PATH,
    },
    utils::join_path,
};

use super::{leader_election, LeaderElection, LeaderElectionState};

// The Load Manager monitors and distributes load across brokers by managing topic and partition assignments.
// It implements rebalancing logic to redistribute topics/partitions when brokers join or leave the cluster
// and is responsible for failover mechanisms to handle broker failures.
#[derive(Debug, Clone)]
pub(crate) struct LoadManager {
    // broker_id to LoadReport
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
    // rankings based on the load calculation
    rankings: Arc<Mutex<Vec<(u64, usize)>>>,
    // the broker_id to be served to the caller on function get_next_broker
    next_broker: Arc<AtomicU64>,
    meta_store: MetadataStorage,
}

impl LoadManager {
    pub fn new(broker_id: u64, meta_store: MetadataStorage) -> Self {
        LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(broker_id)),
            meta_store,
        }
    }
    pub async fn bootstrap(&mut self, broker_id: u64) -> Result<mpsc::Receiver<ETCDWatchEvent>> {
        let client = if let Some(client) = self.meta_store.get_client() {
            client
        } else {
            return Err(anyhow!(
                "The Load Manager was unable to fetch the Metadata Store client"
            ));
        };

        // fetch the initial broker Load information
        self.fetch_initial_load(client.clone()).await;

        //calculate rankings after the initial load fetch
        self.calculate_rankings_simple().await;

        let (tx_event, mut rx_event) = mpsc::channel(32);

        // Watch for Metadata Store events (ETCD), interested of :
        //
        // "/cluster/load" to retrieve the load data for each broker from the cluster
        // with the data it calculates the rankings and post the informations on "/cluster/load_balance"
        //
        //
        // "/cluster/unassigned" for new created topics that due to be allocated to brokers
        // it using the calculated rankings to decide which broker will host the new topic
        // and inform the broker by posting the topic on it's path "/cluster/brokers/{broker-id}/{namespace}/{topic}"
        tokio::spawn(async move {
            let mut prefixes = Vec::new();
            prefixes.push(BASE_BROKER_LOAD_PATH.to_string());
            prefixes.push(BASE_UNASSIGNED_PATH.to_string());
            prefixes.push(BASE_REGISTER_PATH.to_string());
            etcd_watch_prefixes(client, prefixes, tx_event).await;
        });

        Ok(rx_event)
    }

    pub(crate) async fn fetch_initial_load(&self, mut client: Client) -> Result<()> {
        // Prepare the etcd request to get all keys under /cluster/brokers/load/
        let options = GetOptions::new().with_prefix();
        let response = client
            .get(BASE_BROKER_LOAD_PATH, Some(options))
            .await
            .expect("Failed to fetch keys from etcd");

        let mut brokers_usage = self.brokers_usage.lock().await;

        // Iterate through the key-value pairs in the response
        for kv in response.kvs() {
            let key = kv.key_str().expect("Failed to parse key");
            let value = kv.value();

            // Extract broker-id from key
            if let Some(broker_id_str) = key.strip_prefix(BASE_BROKER_LOAD_PATH) {
                if let Ok(broker_id) = broker_id_str.parse::<u64>() {
                    // Deserialize the value to LoadReport
                    if let Ok(load_report) = serde_json::from_slice::<LoadReport>(value) {
                        brokers_usage.insert(broker_id, load_report);
                    } else {
                        return Err(anyhow!(
                            "Failed to deserialize LoadReport for broker_id: {}",
                            broker_id
                        ));
                    }
                } else {
                    return Err(anyhow!("Invalid broker_id format in key: {}", key));
                }
            } else {
                return Err(anyhow!("Key does not match expected pattern: {}", key));
            }
        }

        Ok(())
    }

    pub(crate) async fn start(
        &mut self,
        mut rx_event: mpsc::Receiver<ETCDWatchEvent>,
        broker_id: u64,
        leader_election: LeaderElection,
    ) {
        while let Some(event) = rx_event.recv().await {
            let state = leader_election.get_state().await;

            match event.key.as_str() {
                key if key.starts_with(BASE_UNASSIGNED_PATH) => {
                    // only the Leader Broker should assign the topic
                    if state == LeaderElectionState::Following {
                        continue;
                    }
                    info!(
                        "Attempting to assign the new topic {} to a broker",
                        &event.key
                    );
                    self.assign_topic_to_broker(event).await;
                }
                key if key.starts_with(BASE_REGISTER_PATH) => {
                    // only the Leader Broker should realocate the cluster resources
                    if state == LeaderElectionState::Following {
                        continue;
                    }

                    if event.event_type == EventType::Delete {
                        let remove_broker = match key.split('/').last().unwrap().parse::<u64>() {
                            Ok(id) => id,
                            Err(err) => {
                                error!("Unable to parse the broker id: {}", err);
                                continue;
                            }
                        };
                        info!("Broker {} is no longer alive", remove_broker);
                        {
                            let mut brokers_usage_lock = self.brokers_usage.lock().await;
                            brokers_usage_lock.remove(&remove_broker);
                        }

                        {
                            let mut rankings_lock = self.rankings.lock().await;
                            rankings_lock.retain(|&(entry_id, _)| entry_id != remove_broker);
                        }
                        // BIG TODO! - reallocate the resources to another broker
                    }
                }

                key if key.starts_with(BASE_BROKER_LOAD_PATH) => {
                    // the event is processed and added localy,
                    // but only the Leader Broker does the calculations on the loads
                    trace!("A new load report has been received from: {}", &event.key);
                    self.process_event(event).await;

                    if state == LeaderElectionState::Following {
                        continue;
                    }
                    self.calculate_rankings_simple().await;
                    let next_broker = self
                        .rankings
                        .lock()
                        .await
                        .get(0)
                        .get_or_insert(&(broker_id, 0))
                        .0;
                    let _ = self
                        .next_broker
                        .swap(next_broker, std::sync::atomic::Ordering::SeqCst);

                    // need to post it's decision on the Metadata Store
                }
                _ => {
                    // Handle unexpected events if needed
                    error!("Received an unexpected event: {}", &event.key);
                }
            }
        }
    }

    // Post the topic on the broker address /cluster/brokers/{broker-id}/{namespace}/{topic}
    // to be further read and processed by the selected broker
    async fn assign_topic_to_broker(&mut self, event: ETCDWatchEvent) {
        if event.event_type != EventType::Put {
            return;
        }
        let parts: Vec<_> = event.key.split(BASE_UNASSIGNED_PATH).collect();
        let topic_name = parts[1];

        let broker_id = self.get_next_broker().await;
        let path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string(), topic_name]);

        match self
            .meta_store
            .put(&path, serde_json::Value::Null, MetaOptions::None)
            .await
        {
            Ok(_) => info!(
                "The topic {} was successfully assign to broker {}",
                topic_name, broker_id
            ),
            Err(err) => warn!(
                "Unable to assign topic {} to the broker {}, due to error: {}",
                topic_name, broker_id, err
            ),
        }
    }

    async fn process_event(&self, event: ETCDWatchEvent) {
        if event.event_type != EventType::Put {
            return;
        }

        let broker_id = match extract_broker_id(&event.key) {
            Some(id) => id,
            None => return,
        };

        let load_report = match event.value.as_deref().and_then(parse_load_report) {
            Some(report) => report,
            None => return,
        };

        let mut brokers_usage = self.brokers_usage.lock().await;
        brokers_usage.insert(broker_id, load_report);
    }

    pub async fn get_next_broker(&mut self) -> u64 {
        let rankings = self.rankings.lock().await;
        let first_in_list = rankings.get(0).unwrap().0;

        let next_broker = self
            .next_broker
            .swap(first_in_list, std::sync::atomic::Ordering::SeqCst);

        next_broker
    }

    pub(crate) async fn check_ownership(&self, broker_id: u64, topic_name: &str) -> bool {
        let brokers_usage = self.brokers_usage.lock().await;
        if let Some(load_report) = brokers_usage.get(&broker_id) {
            if load_report.topic_list.contains(&topic_name.to_owned()) {
                return true;
            }
        }
        false
    }

    // Simple Load Calculation: the load is just based on the number of topics.
    async fn calculate_rankings_simple(&self) {
        let brokers_usage = self.brokers_usage.lock().await;

        let mut broker_loads: Vec<(u64, usize)> = brokers_usage
            .iter()
            .map(|(&broker_id, load_report)| (broker_id, load_report.topics_len))
            .collect();

        broker_loads.sort_by_key(|&(_, load)| load);

        *self.rankings.lock().await = broker_loads;
    }

    // Composite Load Calculation: the load is based on the number of topics, CPU usage, and memory usage.
    async fn calculate_rankings_composite(&self) {
        let brokers_usage = self.brokers_usage.lock().await;

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

        *self.rankings.lock().await = broker_loads;
    }
}

fn extract_broker_id(key: &str) -> Option<u64> {
    key.strip_prefix(format!("{}/", BASE_BROKER_LOAD_PATH).as_str())?
        .parse()
        .ok()
}

fn parse_load_report(value: &[u8]) -> Option<LoadReport> {
    let value_str = std::str::from_utf8(value).ok()?;
    serde_json::from_str(value_str).ok()
}

#[cfg(test)]
mod tests {
    use crate::metadata_store::MemoryMetadataStore;

    use super::*;

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
    async fn test_single_broker() {
        let store_config = MetadataStoreConfig::new();
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::MemoryStore(
                MemoryMetadataStore::new(store_config)
                    .await
                    .expect("use for testing"),
            ),
        };

        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(50, 30, 10));

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 1);
        assert_eq!(rankings[0], (1, 50)); // 10 * 1.0 + 50 * 0.5 + 30 * 0.5 = 75
    }

    #[tokio::test]
    async fn test_multiple_brokers() {
        let store_config = MetadataStoreConfig::new();
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::MemoryStore(
                MemoryMetadataStore::new(store_config)
                    .await
                    .expect("use for testing"),
            ),
        };

        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(50, 30, 10));
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(2, create_load_report(20, 20, 5));
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(3, create_load_report(10, 10, 2));

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 3);
        assert_eq!(rankings[0], (3, 12)); // 2 * 1.0 + 10 * 0.5 + 10 * 0.5 = 12
        assert_eq!(rankings[1], (2, 25)); // 5 * 1.0 + 20 * 0.5 + 20 * 0.5 = 25
        assert_eq!(rankings[2], (1, 50)); // 10 * 1.0 + 50 * 0.5 + 30 * 0.5 = 75
    }

    #[tokio::test]
    async fn test_empty_brokers_usage() {
        let store_config = MetadataStoreConfig::new();
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::MemoryStore(
                MemoryMetadataStore::new(store_config)
                    .await
                    .expect("use for testing"),
            ),
        };

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 0);
    }

    #[tokio::test]
    async fn test_same_load_brokers() {
        let store_config = MetadataStoreConfig::new();
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::MemoryStore(
                MemoryMetadataStore::new(store_config)
                    .await
                    .expect("use for testing"),
            ),
        };

        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(10, 10, 5)); // Load: 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(2, create_load_report(10, 10, 5)); // Load: 15
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(3, create_load_report(10, 10, 5)); // Load: 15

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 3);
        assert!(rankings.contains(&(1, 15))); // 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
        assert!(rankings.contains(&(2, 15))); // 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
        assert!(rankings.contains(&(3, 15))); // 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
    }
}
