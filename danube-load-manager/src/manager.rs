use crate::load_report::LoadReport;
use crate::rankings::{rankings_composite, rankings_simple};

use anyhow::{anyhow, Result};
use danube_metadata_store::{MetaOptions, MetadataStorage, MetadataStore, WatchEvent, WatchStream};
use futures::stream::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Constants for metadata store paths
/// These should be provided by the broker crate
pub const BASE_BROKER_LOAD_PATH: &str = "/cluster/brokers/load";
pub const BASE_BROKER_PATH: &str = "/cluster/brokers";
pub const BASE_REGISTER_PATH: &str = "/cluster/register";
pub const BASE_UNASSIGNED_PATH: &str = "/cluster/unassigned";

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
#[derive(Clone)]
pub struct LoadManager {
    /// Maps broker IDs to their current load reports (CPU, memory, topic count, etc.)
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,

    /// Broker rankings ordered by load (lowest to highest) - used for topic assignment
    /// Format: Vec<(broker_id, load_score)>
    rankings: Arc<Mutex<Vec<(u64, usize)>>>,

    /// Next broker ID to assign topics to (round-robin within lowest load tier)
    next_broker: Arc<AtomicU64>,

    /// Metadata store interface for persisting broker assignments and watching events
    meta_store: MetadataStorage,

    /// Callback for recording broker assignment metrics
    assignment_callback: Option<Arc<dyn Fn(u64, &str) + Send + Sync>>,
}

impl std::fmt::Debug for LoadManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadManager")
            .field("next_broker", &self.next_broker)
            .field("meta_store", &self.meta_store)
            .field("has_callback", &self.assignment_callback.is_some())
            .finish()
    }
}

impl LoadManager {
    /// Creates a new LoadManager instance
    pub fn new(broker_id: u64, meta_store: MetadataStorage) -> Self {
        LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(broker_id)),
            meta_store,
            assignment_callback: None,
        }
    }

    /// Sets a callback for recording broker assignment metrics
    pub fn with_assignment_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(u64, &str) + Send + Sync + 'static,
    {
        self.assignment_callback = Some(Arc::new(callback));
        self
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
    async fn fetch_initial_load(&self) -> Result<()> {
        use danube_metadata_store::EtcdGetOptions;
        
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
    pub async fn start<S: LeaderStateProvider>(
        &mut self,
        mut watch_stream: WatchStream,
        broker_id: u64,
        leader_state_provider: S,
    ) {
        while let Some(result) = watch_stream.next().await {
            match result {
                Ok(event) => {
                    let state = leader_state_provider.get_state().await;

                    match &event {
                        WatchEvent::Put { key, value, .. } => {
                            // Handle different paths
                            if key.starts_with(BASE_UNASSIGNED_PATH.as_bytes()) {
                                if let Err(e) =
                                    self.handle_unassigned_topic(&key, &value, state).await
                                {
                                    error!(error = %e, "error handling unassigned topic");
                                }
                            } else if key.starts_with(BASE_REGISTER_PATH.as_bytes()) {
                                if let Err(e) = self.handle_broker_registration(&event, state).await
                                {
                                    error!(error = %e, "error handling broker registration");
                                }
                            } else if key.starts_with(BASE_BROKER_LOAD_PATH.as_bytes()) {
                                if let Err(e) = self
                                    .handle_load_update(&key, &value, state, broker_id)
                                    .await
                                {
                                    error!(error = %e, "error handling load update");
                                }
                            }
                        }
                        WatchEvent::Delete { key, .. } => {
                            if key.starts_with(BASE_REGISTER_PATH.as_bytes()) {
                                if let Err(e) = self.handle_broker_registration(&event, state).await
                                {
                                    error!(error = %e, "error handling broker registration");
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "error receiving watch event");
                }
            }
        }
    }

    /// Handles assignment of unassigned topics to available brokers
    async fn handle_unassigned_topic(
        &mut self,
        key: &[u8],
        value: &[u8],
        state: LeaderState,
    ) -> Result<()> {
        // Only leader assigns topics
        if state == LeaderState::Following {
            return Ok(());
        }

        let key_str =
            std::str::from_utf8(key).map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?;

        info!(topic = %key_str, "attempting to assign the new topic to a broker");

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
    async fn handle_broker_registration(
        &mut self,
        event: &WatchEvent,
        state: LeaderState,
    ) -> Result<()> {
        // Only leader handles broker registration
        if state == LeaderState::Following {
            return Ok(());
        }

        match event {
            WatchEvent::Delete { key, .. } => {
                let key_str = std::str::from_utf8(&key)
                    .map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?;

                let remove_broker = match key_str.split('/').last().unwrap().parse::<u64>() {
                    Ok(id) => id,
                    Err(err) => {
                        error!(error = %err, "unable to parse the broker id");
                        return Ok(());
                    }
                };

                info!(broker_id = %remove_broker, "broker is no longer alive");

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
                        broker_id = %remove_broker,
                        error = %err,
                        "unable to delete resources of the unregistered broker"
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
        state: LeaderState,
        broker_id: u64,
    ) -> Result<()> {
        let key_str =
            std::str::from_utf8(key).map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?;

        debug!(broker_key = %key_str, "a new load report has been received");

        // Process the event locally
        self.process_event(WatchEvent::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            mod_revision: None,
            version: None,
        })
        .await?;

        // Only leader proceeds with rankings calculation
        if state == LeaderState::Following {
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
                        error!(error = %e, "invalid UTF-8 in key");
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
                            error!(topic = %topic_name, error = %e, "cannot reassign topic for unload");
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
                    Ok(_) => {
                        info!(
                            topic = %topic_name,
                            broker_id = %broker_id,
                            "the topic was successfully assigned to broker"
                        );
                        
                        // Call assignment callback if provided
                        if let Some(callback) = &self.assignment_callback {
                            callback(broker_id, "assign");
                        }
                    }
                    Err(err) => warn!(
                        topic = %topic_name,
                        broker_id = %broker_id,
                        error = %err,
                        "unable to assign topic to the broker"
                    ),
                }

                // Delete the unassigned entry after successful assignment to keep the path clean
                let unassigned_key = key_str.to_string();
                if let Err(err) = self.meta_store.delete(&unassigned_key).await {
                    warn!(
                        key = %unassigned_key,
                        error = %err,
                        "failed to delete unassigned entry after assignment"
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
            self.next_broker.load(std::sync::atomic::Ordering::SeqCst)
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
    pub async fn delete_topic_allocation(&mut self, broker_id: u64) -> Result<()> {
        let path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string()]);
        let childrens = self.meta_store.get_childrens(&path).await?;

        // Only delete broker assignment keys and create unassigned entries
        // DO NOT delete /topics/** or /namespaces/** metadata
        for full_assignment_path in childrens {
            info!(
                path = %full_assignment_path,
                "attempting to delete broker assignment"
            );

            // Delete only the broker assignment: /cluster/brokers/{dead-broker}/{ns}/{topic}
            if let Err(e) = self.meta_store.delete(&full_assignment_path).await {
                error!(
                    path = %full_assignment_path,
                    error = %e,
                    "failed to delete broker assignment"
                );
            } else {
                info!(
                    path = %full_assignment_path,
                    "successfully deleted broker assignment"
                );
                
                // Call assignment callback if provided
                if let Some(callback) = &self.assignment_callback {
                    callback(broker_id, "unassign");
                }
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
                        path = %unassigned_path,
                        error = %e,
                        "failed to create unassigned entry"
                    );
                } else {
                    info!(
                        path = %unassigned_path,
                        "created unassigned entry for topic reassignment"
                    );
                }
            }
        }

        Ok(())
    }

    /// Checks if a specific broker owns/manages a given topic.
    pub async fn check_ownership(&self, broker_id: u64, topic_name: &str) -> bool {
        let brokers_usage = self.brokers_usage.lock().await;
        if let Some(load_report) = brokers_usage.get(&broker_id) {
            if load_report.topic_list.contains(&topic_name.to_owned()) {
                return true;
            }
        }
        false
    }

    /// Calculates broker rankings using simple topic-count based algorithm
    async fn calculate_rankings_simple(&self) {
        let broker_loads = rankings_simple(self.brokers_usage.clone()).await;
        *self.rankings.lock().await = broker_loads;
    }

    /// Calculates broker rankings using composite resource utilization metrics
    #[allow(dead_code)]
    async fn calculate_rankings_composite(&self) {
        let broker_loads = rankings_composite(self.brokers_usage.clone()).await;
        *self.rankings.lock().await = broker_loads;
    }
}

/// Trait for providing leader election state
/// This allows the load manager to work with different leader election implementations
#[async_trait::async_trait]
pub trait LeaderStateProvider: Send + Sync {
    async fn get_state(&self) -> LeaderState;
}

/// Leader election state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderState {
    Leading,
    Following,
}

/// Extracts broker ID from a metadata store key path
fn extract_broker_id(key: &str) -> Option<u64> {
    key.strip_prefix(format!("{}/", BASE_BROKER_LOAD_PATH).as_str())?
        .parse()
        .ok()
}

/// Helper function to join path components
fn join_path(parts: &[&str]) -> String {
    parts.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_broker_id() {
        assert_eq!(
            extract_broker_id("/cluster/brokers/load/123"),
            Some(123)
        );
        assert_eq!(
            extract_broker_id("/cluster/brokers/load/456"),
            Some(456)
        );
        assert_eq!(extract_broker_id("/cluster/other/path"), None);
    }

    #[test]
    fn test_join_path() {
        assert_eq!(
            join_path(&["/cluster", "brokers", "1"]),
            "/cluster/brokers/1"
        );
        assert_eq!(
            join_path(&["/cluster", "unassigned", "default", "topic"]),
            "/cluster/unassigned/default/topic"
        );
    }
}
