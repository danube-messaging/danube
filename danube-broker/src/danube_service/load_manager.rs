pub(crate) mod load_report;
mod rankings;

use anyhow::{anyhow, Result};
use danube_metadata_store::EtcdGetOptions;
use danube_metadata_store::{MetaOptions, MetadataStore, MetadataStorage, WatchEvent, WatchStream};
use futures::stream::StreamExt;
use load_report::{LoadReport, ResourceType};
use rankings::{rankings_composite, rankings_simple};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, trace, warn};

use crate::{
    resources::{
        BASE_BROKER_LOAD_PATH, BASE_BROKER_PATH, BASE_NAMESPACES_PATH, BASE_REGISTER_PATH,
        BASE_UNASSIGNED_PATH,
    },
    utils::join_path,
};

use super::{LeaderElection, LeaderElectionState};

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
    pub async fn bootstrap(&mut self, _broker_id: u64) -> Result<WatchStream> {
        // fetch the initial broker Load information
        self.fetch_initial_load().await?;

        //calculate rankings after the initial load fetch
        self.calculate_rankings_simple().await;

        // Watch for Metadata Store events (ETCD), interested of :
        //
        // "/cluster/load" to retrieve the load data for each broker from the cluster
        // with the data it calculates the rankings and post the informations on "/cluster/load_balance"
        //
        //
        // "/cluster/unassigned" for new created topics that due to be allocated to brokers
        // it using the calculated rankings to decide which broker will host the new topic
        // and inform the broker by posting the topic on it's path "/cluster/brokers/{broker-id}/{namespace}/{topic}"
        // Watch for multiple prefixes by creating a combined stream
        let mut streams = Vec::new();
        let prefixes = vec![
            BASE_BROKER_LOAD_PATH.to_string(),
            BASE_UNASSIGNED_PATH.to_string(),
            BASE_REGISTER_PATH.to_string(),
        ];

        for prefix in prefixes {
            let watch_stream = self.meta_store.watch(&prefix).await?;
            streams.push(watch_stream);
        }

        // Combine multiple watch streams into one
        let combined_stream = futures::stream::select_all(streams);
        Ok(WatchStream::new(combined_stream))
    }

    pub(crate) async fn fetch_initial_load(&self) -> Result<()> {
        // Get all keys under /cluster/brokers/load/
        let response = self
            .meta_store
            .get(
                BASE_BROKER_LOAD_PATH,
                MetaOptions::EtcdGet(EtcdGetOptions::new().with_prefix()),
            )
            .await?;

        if let Some(Value::Object(map)) = response {
            let mut brokers_usage = self.brokers_usage.lock().await;

            for (key, value) in map {
                // Extract broker-id from key
                if let Some(broker_id_str) = key.strip_prefix(BASE_BROKER_LOAD_PATH) {
                    if let Ok(broker_id) = broker_id_str.parse::<u64>() {
                        // Deserialize the value to LoadReport
                        if let Ok(load_report) = serde_json::from_value::<LoadReport>(value) {
                            brokers_usage.insert(broker_id, load_report);
                        } else {
                            return Err(anyhow!(
                                "Failed to deserialize LoadReport for broker_id: {}",
                                broker_id
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn start(
        &mut self,
        mut watch_stream: WatchStream,
        broker_id: u64,
        leader_election: LeaderElection,
    ) {
        while let Some(result) = watch_stream.next().await {
            match result {
                Ok(event) => {
                    let state = leader_election.get_state().await;

                    match &event {
                        WatchEvent::Put { key, value, .. } => {
                            // Handle different paths
                            if key.starts_with(BASE_UNASSIGNED_PATH.as_bytes()) {
                                if let Err(e) =
                                    self.handle_unassigned_topic(&key, &value, state).await
                                {
                                    error!("Error handling unassigned topic: {}", e);
                                }
                            } else if key.starts_with(BASE_REGISTER_PATH.as_bytes()) {
                                if let Err(e) = self.handle_broker_registration(&event, state).await
                                {
                                    error!("Error handling broker registration: {}", e);
                                }
                            } else if key.starts_with(BASE_BROKER_LOAD_PATH.as_bytes()) {
                                if let Err(e) = self
                                    .handle_load_update(&key, &value, state, broker_id)
                                    .await
                                {
                                    error!("Error handling load update: {}", e);
                                }
                            }
                        }
                        WatchEvent::Delete { key, .. } => {
                            if key.starts_with(BASE_REGISTER_PATH.as_bytes()) {
                                if let Err(e) = self.handle_broker_registration(&event, state).await
                                {
                                    error!("Error handling broker registration: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error receiving watch event: {}", e);
                }
            }
        }
    }

    async fn handle_unassigned_topic(
        &mut self,
        key: &[u8],
        value: &[u8],
        state: LeaderElectionState,
    ) -> Result<()> {
        // Only leader assigns topics
        if state == LeaderElectionState::Following {
            return Ok(());
        }

        let key_str =
            std::str::from_utf8(key).map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?;

        info!("Attempting to assign the new topic {} to a broker", key_str);

        // Create WatchEvent for assign_topic_to_broker
        self.assign_topic_to_broker(WatchEvent::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            mod_revision: None,
            version: None,
        })
        .await;

        Ok(())
    }

    async fn handle_broker_registration(
        &mut self,
        event: &WatchEvent,
        state: LeaderElectionState,
    ) -> Result<()> {
        // Only leader handles broker registration
        if state == LeaderElectionState::Following {
            return Ok(());
        }

        match event {
            WatchEvent::Delete { key, .. } => {
                let key_str = std::str::from_utf8(&key)
                    .map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?;

                let remove_broker = match key_str.split('/').last().unwrap().parse::<u64>() {
                    Ok(id) => id,
                    Err(err) => {
                        error!("Unable to parse the broker id: {}", err);
                        return Ok(());
                    }
                };

                info!("Broker {} is no longer alive", remove_broker);

                // Remove from brokers usage
                {
                    let mut brokers_usage_lock = self.brokers_usage.lock().await;
                    brokers_usage_lock.remove(&remove_broker);
                }

                // Remove from rankings
                {
                    let mut rankings_lock = self.rankings.lock().await;
                    rankings_lock.retain(|&(entry_id, _)| entry_id != remove_broker);
                }

                // TODO! - reallocate the resources to another broker
                // for now I will delete from metadata store all the topics assigned to /cluster/brokers/removed_broker/*
                if let Err(err) = self.delete_topic_allocation(remove_broker).await {
                    error!(
                        "Unable to delete resources of the unregistered broker {}, due to error {}",
                        remove_broker, err
                    );
                }
            }
            WatchEvent::Put { .. } => (), // should not get here
        }
        Ok(())
    }

    async fn handle_load_update(
        &mut self,
        key: &[u8],
        value: &[u8],
        state: LeaderElectionState,
        broker_id: u64,
    ) -> Result<()> {
        let key_str =
            std::str::from_utf8(key).map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?;

        trace!("A new load report has been received from: {}", key_str);

        // Process the event locally
        self.process_event(WatchEvent::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            mod_revision: None,
            version: None,
        })
        .await?;

        // Only leader proceeds with rankings calculation
        if state == LeaderElectionState::Following {
            return Ok(());
        }

        // Leader calculates new rankings
        self.calculate_rankings_simple().await;

        // Update next_broker based on rankings
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

        Ok(())
    }

    // Post the topic on the broker address /cluster/brokers/{broker-id}/{namespace}/{topic}
    // to be further read and processed by the selected broker
    async fn assign_topic_to_broker(&mut self, event: WatchEvent) {
        match event {
            WatchEvent::Put { key, .. } => {
                // recalculate the rankings after the topic was assigned
                self.calculate_rankings_simple().await;

                // Convert key from bytes to string
                let key_str = match std::str::from_utf8(&key) {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Invalid UTF-8 in key: {}", e);
                        return;
                    }
                };

                let parts: Vec<_> = key_str.split(BASE_UNASSIGNED_PATH).collect();
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

                // Update internal state
                let mut brokers_usage = self.brokers_usage.lock().await;
                if let Some(load_report) = brokers_usage.get_mut(&broker_id) {
                    load_report.topics_len += 1;
                    load_report.topic_list.push(topic_name.to_string());
                }
            }
            WatchEvent::Delete { .. } => (), // Ignore delete events
        }
    }

    async fn process_event(&mut self, event: WatchEvent) -> Result<()> {
        match event {
            WatchEvent::Put { key, value, .. } => {
                let load_report: LoadReport = serde_json::from_slice(&value)
                    .map_err(|e| anyhow!("Failed to parse LoadReport: {}", e))?;

                let key_str = std::str::from_utf8(&key)
                    .map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?;

                // Extract broker ID from key path
                if let Some(broker_id) = extract_broker_id(key_str) {
                    let mut brokers_usage = self.brokers_usage.lock().await;
                    brokers_usage.insert(broker_id, load_report);
                }
            }
            WatchEvent::Delete { .. } => (), // should not happen
        }
        Ok(())
    }

    pub async fn get_next_broker(&mut self) -> u64 {
        let rankings = self.rankings.lock().await;
        let next_broker = rankings.get(0).unwrap().0;

        let _ = self
            .next_broker
            .swap(next_broker, std::sync::atomic::Ordering::SeqCst);

        next_broker
    }

    pub async fn delete_topic_allocation(&mut self, broker_id: u64) -> Result<()> {
        let path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string()]);
        let childrens = self.meta_store.get_childrens(&path).await?;

        // should delete the paths like this /cluster/brokers/6719542140305601108/namespace_name/topic_name
        // so the topics are not allocated with unregistered broker
        for children in childrens {
            self.meta_store.delete(&children).await?;

            // delete the topic from the /namespace path as well
            let parts: Vec<&str> = children.split('/').collect();
            let path_ns = join_path(&[
                BASE_NAMESPACES_PATH,
                &parts[4],
                "topics",
                &parts[4],
                &parts[5],
            ]);
            self.meta_store.delete(&path_ns).await?;
        }

        Ok(())
    }

    //#[allow(dead_code)]
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
        let broker_loads = rankings_simple(self.brokers_usage.clone()).await;
        *self.rankings.lock().await = broker_loads;
    }

    // Composite Load Calculation: the load is based on the number of topics, CPU usage, and memory usage.
    #[allow(dead_code)]
    async fn calculate_rankings_composite(&self) {
        let broker_loads = rankings_composite(self.brokers_usage.clone()).await;
        *self.rankings.lock().await = broker_loads;
    }
}

fn extract_broker_id(key: &str) -> Option<u64> {
    key.strip_prefix(format!("{}/", BASE_BROKER_LOAD_PATH).as_str())?
        .parse()
        .ok()
}

#[allow(dead_code)]
fn parse_load_report(value: &[u8]) -> Option<LoadReport> {
    let value_str = std::str::from_utf8(value).ok()?;
    serde_json::from_str(value_str).ok()
}
