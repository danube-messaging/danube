pub(crate) mod load_report;
mod rankings;

use anyhow::{anyhow, Result};
use danube_metadata_store::EtcdGetOptions;
use danube_metadata_store::{MetaOptions, MetadataStorage, MetadataStore, WatchEvent, WatchStream};
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
        BASE_BROKER_LOAD_PATH, BASE_BROKER_PATH, BASE_REGISTER_PATH, BASE_UNASSIGNED_PATH,
    },
    utils::join_path,
};

use super::{LeaderElection, LeaderElectionState};

/// LoadManager - Distributed Broker Load Balancing and Failover Management
///
/// The LoadManager is a critical component of the Danube messaging system that handles:
///
/// ## Core Responsibilities:
/// - **Load Distribution**: Monitors broker resource usage and distributes topic assignments
/// - **Dynamic Rebalancing**: Redistributes topics when brokers join or leave the cluster  
/// - **Failover Management**: Handles broker failures by reallocating topics to healthy brokers
/// - **Resource Optimization**: Uses ranking algorithms to optimize broker utilization
///
/// ## Architecture:
/// The LoadManager operates as a centralized coordinator that:
/// 1. Watches broker load metrics via metadata store events
/// 2. Calculates broker rankings based on resource utilization
/// 3. Assigns new topics to the least loaded brokers
/// 4. Handles broker failures by cleaning up assignments and marking topics for reassignment
///
/// ## Thread Safety:
/// All internal state is protected by Arc<Mutex> or atomic operations for safe concurrent access.
#[derive(Debug, Clone)]
pub(crate) struct LoadManager {
    /// Maps broker IDs to their current load reports (CPU, memory, topic count, etc.)
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,

    /// Broker rankings ordered by load (lowest to highest) - used for topic assignment
    /// Format: Vec<(broker_id, load_score)>
    rankings: Arc<Mutex<Vec<(u64, usize)>>>,

    /// Next broker ID to assign topics to (round-robin within lowest load tier)
    next_broker: Arc<AtomicU64>,

    /// Metadata store interface for persisting broker assignments and watching events
    meta_store: MetadataStorage,
}

impl LoadManager {
    /// Creates a new LoadManager instance
    pub fn new(broker_id: u64, meta_store: MetadataStorage) -> Self {
        LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(broker_id)),
            meta_store,
        }
    }
    /// Initializes the LoadManager and starts watching for cluster events
    ///
    /// ## Purpose:
    /// Bootstraps the LoadManager by loading initial broker state and setting up
    /// event watchers for dynamic load balancing and topic assignment.
    ///
    /// ## Initialization Process:
    /// 1. **Load Initial State**: Fetches current broker load reports from metadata store
    /// 2. **Calculate Rankings**: Computes initial broker rankings based on resource usage
    /// 3. **Setup Watchers**: Creates event streams for real-time cluster monitoring
    ///
    /// ## Watched Paths:
    /// - **`/cluster/load`**: Broker load reports (CPU, memory, topic counts)
    ///   - Used to recalculate rankings and update load balancing decisions
    /// - **`/cluster/unassigned`**: New topics awaiting broker assignment
    ///   - Triggers topic assignment to least loaded brokers
    /// - **`/cluster/register`**: Broker registration/deregistration events
    ///   - Handles broker join/leave events for cluster membership
    ///
    /// ## Returns:
    /// A combined `WatchStream` that emits events from all monitored paths.
    /// The caller should process these events to handle load balancing and assignments.
    pub async fn bootstrap(&mut self, _broker_id: u64) -> Result<WatchStream> {
        // Initialize broker state from metadata store
        self.fetch_initial_load().await?;

        // Calculate initial broker rankings for load balancing
        self.calculate_rankings_simple().await;

        // Setup event watchers for dynamic cluster management
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

    /// Fetches initial broker load data from the metadata store
    ///
    /// ## Purpose:
    /// Loads the current load reports for all active brokers in the cluster.
    /// This provides the baseline data needed for calculating broker rankings
    /// and making informed load balancing decisions.
    ///
    /// ## Process:
    /// 1. **Query Metadata Store**: Retrieves all keys under `/cluster/load/` path
    /// 2. **Parse Broker IDs**: Extracts broker IDs from the key paths
    /// 3. **Deserialize Load Reports**: Converts JSON data to `LoadReport` structs
    /// 4. **Update Internal State**: Populates the `brokers_usage` HashMap
    pub(crate) async fn fetch_initial_load(&self) -> Result<()> {
        // Query all broker load reports from metadata store
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
                // Extract broker ID from the key path
                if let Some(broker_id_str) = key.strip_prefix(BASE_BROKER_LOAD_PATH) {
                    if let Ok(broker_id) = broker_id_str.parse::<u64>() {
                        // Deserialize JSON to LoadReport struct
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

    /// Main event processing loop for LoadManager cluster coordination
    ///
    /// ## Purpose:
    /// Continuously processes metadata store events to handle cluster state changes,
    /// topic assignments, broker registrations, and load updates. This is the core
    /// coordination loop that maintains cluster-wide load balancing.
    ///
    /// ## Event Processing:
    /// The function monitors three main event categories:
    ///
    /// ### **Unassigned Topics** (`/cluster/unassigned/`)
    /// - New topics requiring broker assignment
    /// - Topics from failed brokers needing reassignment
    /// - Triggers load-balanced topic allocation
    ///
    /// ### **Broker Registration** (`/cluster/register/`)
    /// - Broker join/leave events
    /// - Updates cluster membership and rankings
    /// - Handles broker lifecycle management
    ///
    /// ### **Load Updates** (`/cluster/load/`)
    /// - Real-time broker resource utilization reports
    /// - Triggers ranking recalculation
    /// - Maintains current load balancing state
    ///
    /// ## Leader Election Integration:
    /// Only the elected leader processes assignment events to prevent conflicts.
    /// All brokers can process load updates for local state consistency.
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

    /// Handles assignment of unassigned topics to available brokers
    ///
    /// ## Purpose:
    /// Processes unassigned topic events from the metadata store and assigns them
    /// to the least loaded broker. This is triggered when new topics are created
    /// or when topics need reassignment after broker failures.
    ///
    /// ## Leader-Only Operation:
    /// Only the elected cluster leader processes topic assignments to prevent
    /// conflicts and ensure consistent assignment decisions across the cluster.
    ///
    /// ## Process:
    /// 1. **Leader Check**: Verifies current broker is the elected leader
    /// 2. **Key Validation**: Converts byte key to UTF-8 string format
    /// 3. **Assignment Delegation**: Calls `assign_topic_to_broker()` with event data
    ///
    /// ## Event Flow:
    /// - Unassigned topics appear at `/cluster/unassigned/{namespace}/{topic}`
    /// - Leader processes the event and selects optimal broker
    /// - Topic gets assigned to `/cluster/brokers/{broker_id}/{namespace}/{topic}`
    /// - Unassigned entry is removed after successful assignment
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

    /// Handles broker registration and deregistration events
    ///
    /// ## Purpose:
    /// Processes broker lifecycle events to maintain accurate cluster membership
    /// and handle broker failures. Updates internal state and triggers topic
    /// reassignment when brokers leave the cluster.
    ///
    /// ## Leader-Only Operation:
    /// Only the elected cluster leader processes broker registration events to
    /// ensure consistent cluster state management and prevent coordination conflicts.
    ///
    /// ## Event Types:
    ///
    /// ### **Broker Deregistration** (Delete Events)
    /// When a broker leaves or fails:
    /// 1. **Parse Broker ID**: Extracts broker ID from the registration key
    /// 2. **Update Internal State**: Removes broker from usage tracking and rankings
    /// 3. **Trigger Failover**: Calls `delete_topic_allocation()` to reassign topics
    ///
    /// ### **Broker Registration** (Put Events)
    /// New broker joins are handled implicitly through load report updates.
    /// Registration put events are currently no-ops.
    ///
    /// ## Failover Process:
    /// When a broker fails:
    /// - Removes broker from `brokers_usage` HashMap
    /// - Removes broker from `rankings` vector
    /// - Calls `delete_topic_allocation()` to clean up assignments
    /// - Creates unassigned entries for topic reassignment
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

                // Trigger topic reassignment for failed broker
                if let Err(err) = self.delete_topic_allocation(remove_broker).await {
                    error!(
                        "Unable to delete resources of the unregistered broker {}, due to error {}",
                        remove_broker, err
                    );
                }
            }
            WatchEvent::Put { .. } => (), // Broker registration handled via load reports
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
            WatchEvent::Put { key, value, .. } => {
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

                // Try to parse unload marker to exclude originating broker
                let mut exclude_broker: Option<u64> = None;
                if !value.is_empty() {
                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&value) {
                        if let Some(obj) = val.as_object() {
                            if obj.get("reason").and_then(|v| v.as_str()) == Some("unload") {
                                if let Some(from_broker) =
                                    obj.get("from_broker").and_then(|v| v.as_u64())
                                {
                                    exclude_broker = Some(from_broker);
                                }
                            }
                        }
                    }
                }

                let broker_id = if let Some(ex) = exclude_broker {
                    match self.get_next_broker_excluding(ex).await {
                        Ok(id) => id,
                        Err(e) => {
                            error!("Cannot reassign topic {} for unload: {}", topic_name, e);
                            // Keep unassigned marker for future reassignment
                            return;
                        }
                    }
                } else {
                    self.get_next_broker().await
                };
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

                // Delete the unassigned entry after successful assignment to keep the path clean
                let unassigned_key = key_str.to_string();
                if let Err(err) = self.meta_store.delete(&unassigned_key).await {
                    warn!(
                        "Failed to delete unassigned entry {} after assignment: {}",
                        unassigned_key, err
                    );
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

    /// Selects the next broker for topic assignment using load-based selection
    ///
    /// ## Purpose:
    /// Returns the broker ID of the least loaded broker for new topic assignments.
    /// This implements the core load balancing logic for distributing topics across brokers.
    ///
    /// ## Algorithm:
    /// 1. **Access Rankings**: Locks and reads current broker rankings
    /// 2. **Select Least Loaded**: Chooses the broker with the lowest load (index 0)
    /// 3. **Update State**: Atomically updates the `next_broker` field for tracking
    pub async fn get_next_broker(&mut self) -> u64 {
        let rankings = self.rankings.lock().await;
        // pick first active broker from rankings
        let mut chosen: Option<u64> = None;
        for (bid, _) in rankings.iter() {
            if self.is_broker_active(*bid).await {
                chosen = Some(*bid);
                break;
            }
        }
        let next_broker = chosen.unwrap_or_else(|| {
            // fallback to current atomic value if none active found (should be rare)
            self.next_broker
                .load(std::sync::atomic::Ordering::SeqCst)
        });

        let _ = self
            .next_broker
            .swap(next_broker, std::sync::atomic::Ordering::SeqCst);

        next_broker
    }

    /// Select next broker excluding a specific broker id, and skipping non-active brokers.
    async fn get_next_broker_excluding(&self, exclude_id: u64) -> Result<u64> {
        let rankings = self.rankings.lock().await;
        for (broker_id, _) in rankings.iter() {
            if *broker_id != exclude_id && self.is_broker_active(*broker_id).await {
                return Ok(*broker_id);
            }
        }
        Err(anyhow!(
            "Cannot unload topic: no alternative active broker available. Unload requires at least 2 active brokers in the cluster."
        ))
    }

    /// Returns true if broker state is active or missing (default active); false for draining/drained.
    async fn is_broker_active(&self, broker_id: u64) -> bool {
        let state_path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string(), "state"]);
        match self.meta_store.get(&state_path, MetaOptions::None).await {
            Ok(Some(val)) => val
                .get("mode")
                .and_then(|m| m.as_str())
                .map(|m| m == "active")
                .unwrap_or(true),
            _ => true,
        }
    }

    /// Handles broker failover by cleaning up topic assignments and creating reassignment entries
    ///
    /// ## Purpose:
    /// Called during broker failover to safely remove a failed broker's topic assignments
    /// while preserving all topic and namespace metadata. Creates unassigned entries to
    /// trigger topic reassignment to healthy brokers.
    ///
    /// ## Failover Process:
    /// 1. **Query Assignments**: Retrieves all topics assigned to the failed broker
    /// 2. **Delete Assignments**: Removes broker-specific assignment paths only
    /// 3. **Preserve Metadata**: Leaves all `/topics/**` and `/namespaces/**` data intact
    /// 4. **Create Unassigned**: Adds entries to `/cluster/unassigned/` for reassignment
    ///
    /// ## Path Operations:
    /// - **Deletes**: `/cluster/brokers/{broker_id}/{namespace}/{topic}`
    /// - **Creates**: `/cluster/unassigned/{namespace}/{topic}`
    /// - **Preserves**: `/topics/{namespace}/{topic}` and `/namespaces/{namespace}`
    pub async fn delete_topic_allocation(&mut self, broker_id: u64) -> Result<()> {
        let path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string()]);
        let childrens = self.meta_store.get_childrens(&path).await?;

        // Only delete broker assignment keys and create unassigned entries
        // DO NOT delete /topics/** or /namespaces/** metadata
        for full_assignment_path in childrens {
            // get_childrens now returns full paths (matching ETCD behavior)
            info!(
                "Attempting to delete broker assignment: {}",
                full_assignment_path
            );

            // Delete only the broker assignment: /cluster/brokers/{dead-broker}/{ns}/{topic}
            if let Err(e) = self.meta_store.delete(&full_assignment_path).await {
                error!(
                    "Failed to delete broker assignment {}: {}",
                    full_assignment_path, e
                );
            } else {
                info!(
                    "Successfully deleted broker assignment: {}",
                    full_assignment_path
                );
            }

            // Extract namespace and topic from the full path
            let parts: Vec<&str> = full_assignment_path.split('/').collect();
            if parts.len() >= 6 {
                // parts: ["", "cluster", "brokers", "{broker_id}", "{namespace}", "{topic}"]
                // Create unassigned entry: /cluster/unassigned/{ns}/{topic}
                let unassigned_path = join_path(&[
                    BASE_UNASSIGNED_PATH,
                    &parts[4], // namespace
                    &parts[5], // topic
                ]);

                // Create unassigned entry with empty value to signal Load Manager for reassignment
                if let Err(e) = self
                    .meta_store
                    .put(&unassigned_path, serde_json::Value::Null, MetaOptions::None)
                    .await
                {
                    warn!(
                        "Failed to create unassigned entry for {}: {}",
                        unassigned_path, e
                    );
                } else {
                    info!(
                        "Created unassigned entry for topic reassignment: {}",
                        unassigned_path
                    );
                }
            }
        }

        Ok(())
    }

    /// Checks if a specific broker owns/manages a given topic.
    pub(crate) async fn check_ownership(&self, broker_id: u64, topic_name: &str) -> bool {
        let brokers_usage = self.brokers_usage.lock().await;
        if let Some(load_report) = brokers_usage.get(&broker_id) {
            if load_report.topic_list.contains(&topic_name.to_owned()) {
                return true;
            }
        }
        false
    }

    /// Calculates broker rankings using simple topic-count based algorithm
    ///
    /// ## Purpose:
    /// Computes broker load rankings based solely on the number of assigned topics.
    /// This provides a lightweight load balancing strategy suitable for most scenarios.
    async fn calculate_rankings_simple(&self) {
        let broker_loads = rankings_simple(self.brokers_usage.clone()).await;
        *self.rankings.lock().await = broker_loads;
    }

    /// Calculates broker rankings using composite resource utilization metrics
    ///
    /// ## Purpose:
    /// Computes broker load rankings based on multiple resource factors:
    /// topic count, CPU usage, memory usage, and other system metrics.
    ///
    /// ## Algorithm:
    /// - Weights different resource metrics (CPU, memory, topics, I/O)
    /// - Calculates composite load score for each broker
    /// - Ranks brokers from lowest to highest composite load
    #[allow(dead_code)]
    async fn calculate_rankings_composite(&self) {
        let broker_loads = rankings_composite(self.brokers_usage.clone()).await;
        *self.rankings.lock().await = broker_loads;
    }
}

/// Extracts broker ID from a metadata store key path
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

#[cfg(test)]
mod load_manager_test;
