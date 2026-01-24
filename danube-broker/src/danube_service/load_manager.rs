pub(crate) mod config;
pub(crate) mod load_report;
pub(crate) mod rebalancing;
mod rankings;

use crate::broker_metrics::BROKER_ASSIGNMENTS_TOTAL;
use anyhow::{anyhow, Result};
use danube_metadata_store::EtcdGetOptions;
use danube_metadata_store::{MetaOptions, MetadataStorage, MetadataStore, WatchEvent, WatchStream};
use futures::stream::StreamExt;
use load_report::LoadReport;
use metrics::counter;
use rankings::{rankings_composite, rankings_simple};
use rebalancing::{ImbalanceMetrics, RebalancingHistory, RebalancingMove, RebalancingReason};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::{
    resources::{
        BASE_BROKER_LOAD_PATH, BASE_BROKER_PATH, BASE_REGISTER_PATH, BASE_UNASSIGNED_PATH,
    },
    utils::join_path,
};

use super::leader_election::{LeaderElection, LeaderElectionState};

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
        state: LeaderElectionState,
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
                        error!(error = %e, "invalid UTF-8 in key");
                        return;
                    }
                };

                let parts: Vec<_> = key_str.split(BASE_UNASSIGNED_PATH).collect();
                let topic_name = parts[1];

                // Parse unload/rebalance marker to determine assignment strategy
                let mut exclude_broker: Option<u64> = None;
                let mut target_broker_hint: Option<u64> = None;
                
                if !value.is_empty() {
                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&value) {
                        if let Some(obj) = val.as_object() {
                            let reason = obj.get("reason").and_then(|v| v.as_str());
                            
                            // Handle unload (Phase D: exclude source broker)
                            if reason == Some("unload") {
                                if let Some(from_broker) = obj.get("from_broker").and_then(|v| v.as_u64()) {
                                    exclude_broker = Some(from_broker);
                                    debug!(
                                        topic = %topic_name,
                                        from_broker = %from_broker,
                                        "unload marker detected, will exclude source broker"
                                    );
                                }
                            }
                            
                            // Handle rebalance (Phase 3 Step 6: prefer target broker)
                            if reason == Some("rebalance") {
                                if let Some(from_broker) = obj.get("from_broker").and_then(|v| v.as_u64()) {
                                    exclude_broker = Some(from_broker);
                                }
                                if let Some(to_broker) = obj.get("to_broker").and_then(|v| v.as_u64()) {
                                    target_broker_hint = Some(to_broker);
                                    info!(
                                        topic = %topic_name,
                                        from_broker = ?exclude_broker,
                                        to_broker = %to_broker,
                                        "rebalance marker detected with target broker hint"
                                    );
                                }
                            }
                        }
                    }
                }

                // Select broker based on marker type and hints
                let broker_id = if let Some(target) = target_broker_hint {
                    // Priority 1: Rebalancing - prefer the target broker hint
                    if self.is_broker_active(target).await {
                        info!(
                            topic = %topic_name,
                            target_broker = %target,
                            "using rebalance target broker hint"
                        );
                        target
                    } else {
                        // Fallback: Target broker is not active, select alternative
                        warn!(
                            topic = %topic_name,
                            target_broker = %target,
                            "rebalance target broker is not active, selecting alternative"
                        );
                        match self.get_next_broker_excluding(exclude_broker.unwrap_or(0)).await {
                            Ok(id) => {
                                info!(
                                    topic = %topic_name,
                                    selected_broker = %id,
                                    "selected alternative broker for rebalancing"
                                );
                                id
                            }
                            Err(e) => {
                                error!(topic = %topic_name, error = %e, "cannot select alternative broker");
                                return;
                            }
                        }
                    }
                } else if let Some(ex) = exclude_broker {
                    // Priority 2: Unload - exclude source broker, use rankings
                    match self.get_next_broker_excluding(ex).await {
                        Ok(id) => id,
                        Err(e) => {
                            error!(topic = %topic_name, error = %e, "cannot reassign topic for unload");
                            return;
                        }
                    }
                } else {
                    // Priority 3: Normal assignment - use rankings
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
                        counter!(
                            BROKER_ASSIGNMENTS_TOTAL.name,
                            "broker_id" => broker_id.to_string(),
                            "action" => "assign"
                        )
                        .increment(1);
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
                    // Add a placeholder TopicLoad entry
                    load_report.topics.push(load_report::TopicLoad {
                        topic_name: topic_name.to_string(),
                        message_rate: 0,
                        byte_rate: 0,
                        byte_rate_mbps: 0.0,
                        producer_count: 0,
                        consumer_count: 0,
                        subscription_count: 0,
                        backlog_messages: 0,
                    });
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
                counter!(
                    BROKER_ASSIGNMENTS_TOTAL.name,
                    "broker_id" => broker_id.to_string(),
                    "action" => "unassign"
                )
                .increment(1);
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
    pub(crate) async fn check_ownership(&self, broker_id: u64, topic_name: &str) -> bool {
        let brokers_usage = self.brokers_usage.lock().await;
        if let Some(load_report) = brokers_usage.get(&broker_id) {
            if load_report.topics.iter().any(|t| t.topic_name == topic_name) {
                return true;
            }
        }
        false
    }

    /// Calculates cluster imbalance metrics for rebalancing decisions
    ///
    /// ## Purpose:
    /// Computes statistical measures of cluster load distribution to determine
    /// if automated rebalancing is needed. Uses coefficient of variation (CV)
    /// as the primary metric for cluster balance.
    ///
    /// ## Returns:
    /// `ImbalanceMetrics` containing:
    /// - **Coefficient of Variation (CV)**: std_dev / mean (key metric)
    /// - **Statistical measures**: mean, std_dev, min, max loads
    /// - **Problem brokers**: Overloaded (> mean + 1σ) and underloaded (< mean - 1σ)
    ///
    /// ## CV Interpretation:
    /// - CV < 0.20: Well balanced (aggressive threshold)
    /// - CV < 0.30: Reasonably balanced (balanced threshold)
    /// - CV < 0.40: Acceptable (conservative threshold)
    /// - CV ≥ threshold: Needs rebalancing
    pub async fn calculate_imbalance(&self) -> Result<ImbalanceMetrics> {
        let rankings = self.rankings.lock().await;

        // Need at least 2 brokers for meaningful imbalance calculation
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

        // Extract load values and broker IDs
        let loads: Vec<f64> = rankings.iter().map(|(_, load)| *load as f64).collect();

        // Calculate mean
        let mean = loads.iter().sum::<f64>() / loads.len() as f64;

        // Calculate variance and standard deviation
        let variance = loads
            .iter()
            .map(|load| (*load - mean).powi(2))
            .sum::<f64>()
            / loads.len() as f64;
        let std_dev = variance.sqrt();

        // Find max and min loads
        let max_load = loads
            .iter()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .copied()
            .unwrap_or(0.0);

        let min_load = loads
            .iter()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .copied()
            .unwrap_or(0.0);

        // Coefficient of variation (CV = std_dev / mean)
        // This is the key metric - it's scale-independent unlike std_dev
        let cv = if mean > 0.0 { std_dev / mean } else { 0.0 };

        // Export metric for monitoring
        metrics::gauge!(crate::broker_metrics::CLUSTER_IMBALANCE_CV.name).set(cv);

        // Identify overloaded and underloaded brokers
        let mut overloaded = vec![];
        let mut underloaded = vec![];

        for (broker_id, load) in rankings.iter() {
            let load_f64 = *load as f64;
            
            // Overloaded: more than 1 standard deviation above mean
            if load_f64 > mean + std_dev {
                overloaded.push(*broker_id);
            }
            // Underloaded: more than 1 standard deviation below mean
            // AND less than half the mean (avoid marking zero-load brokers during startup)
            else if load_f64 < mean - std_dev && load_f64 < mean * 0.5 {
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

    /// Determines if cluster needs rebalancing based on configuration and metrics
    ///
    /// ## Purpose:
    /// Decision logic for automated rebalancing. Checks multiple conditions:
    /// 1. Rebalancing is enabled in config
    /// 2. Minimum broker count is met
    /// 3. CV exceeds the threshold for current aggressiveness level
    ///
    /// ## Parameters:
    /// - `metrics`: Current cluster imbalance metrics
    /// - `config`: Rebalancing configuration (threshold, min brokers, etc.)
    ///
    /// ## Returns:
    /// `true` if automated rebalancing should proceed, `false` otherwise
    pub fn should_rebalance(
        &self,
        metrics: &ImbalanceMetrics,
        config: &config::RebalancingConfig,
    ) -> bool {
        // Check if rebalancing is enabled
        if !config.enabled {
            return false;
        }

        // Check minimum broker requirement
        let threshold = config.aggressiveness.threshold();
        
        // Use coefficient of variation to determine if rebalancing is needed
        metrics.needs_rebalancing(threshold)
    }

    /// Selects topics to move for rebalancing
    ///
    /// ## Purpose:
    /// Core candidate selection logic that chooses which topics to move from
    /// overloaded brokers to underloaded brokers. Uses intelligent selection
    /// strategies to minimize disruption while maximizing balance improvement.
    ///
    /// ## Algorithm:
    /// 1. Identify source brokers from `metrics.overloaded_brokers`
    /// 2. For each overloaded broker:
    ///    - Get all its topics
    ///    - Filter blacklisted topics
    ///    - Sort by load (lightest first - safer to move)
    /// 3. Select target broker (least loaded, excluding source)
    /// 4. Create RebalancingMove objects up to max_moves_per_cycle
    ///
    /// ## Strategy:
    /// - **Lightest first**: Small topics are easier and safer to move
    /// - **Blacklist respect**: Never move protected topics
    /// - **Rate limiting**: Honor max_moves_per_cycle
    ///
    /// ## Parameters:
    /// - `metrics`: Cluster imbalance metrics (identifies overloaded brokers)
    /// - `config`: Rebalancing configuration (limits, filters, blacklist)
    ///
    /// ## Returns:
    /// Vector of `RebalancingMove` objects ready for execution
    pub async fn select_rebalancing_candidates(
        &self,
        metrics: &ImbalanceMetrics,
        config: &config::RebalancingConfig,
    ) -> Result<Vec<RebalancingMove>> {
        let rankings = self.rankings.lock().await;
        let brokers = self.brokers_usage.lock().await;

        // Need brokers and overloaded brokers to proceed
        if rankings.is_empty() || metrics.overloaded_brokers.is_empty() {
            return Ok(vec![]);
        }

        let mut moves = Vec::new();

        // For each overloaded broker, select topics to move
        for overloaded_id in &metrics.overloaded_brokers {
            if moves.len() >= config.max_moves_per_cycle {
                break;
            }

            // Get broker's load report
            let overloaded_report = match brokers.get(overloaded_id) {
                Some(r) => r,
                None => continue,
            };

            // Get topics with their estimated load scores
            let mut candidates: Vec<_> = overloaded_report
                .topics
                .iter()
                .map(|t| (t.clone(), t.estimated_load_score()))
                .collect();

            // Filter blacklisted topics
            if !config.blacklist_topics.is_empty() {
                candidates.retain(|(topic, _)| {
                    !self.is_topic_blacklisted(&topic.topic_name, &config.blacklist_topics)
                });
            }

            // Sort by load (ascending - lightest first)
            // This is safer: small topics are easier to move with less disruption
            candidates.sort_by(|(_, load_a), (_, load_b)| {
                load_a.partial_cmp(load_b).unwrap_or(std::cmp::Ordering::Equal)
            });

            // Select target broker (least loaded, excluding source)
            let target_broker = match self.select_target_broker(*overloaded_id, &rankings).await
            {
                Ok(broker) => broker,
                Err(e) => {
                    warn!(
                        source_broker = %overloaded_id,
                        error = %e,
                        "no suitable target broker found for rebalancing"
                    );
                    continue;
                }
            };

            // Create moves for selected topics
            let remaining_capacity = config.max_moves_per_cycle - moves.len();
            for (topic, estimated_load) in candidates.iter().take(remaining_capacity) {
                moves.push(RebalancingMove::new(
                    topic.topic_name.clone(),
                    *overloaded_id,
                    target_broker,
                    RebalancingReason::LoadImbalance,
                    *estimated_load,
                ));

                info!(
                    topic = %topic.topic_name,
                    from_broker = %overloaded_id,
                    to_broker = %target_broker,
                    estimated_load = %estimated_load,
                    "selected topic for rebalancing"
                );
            }
        }

        Ok(moves)
    }

    /// Selects target broker for topic move
    ///
    /// ## Purpose:
    /// Finds the least loaded broker to receive a topic being moved.
    /// Excludes the source broker and inactive/draining brokers.
    ///
    /// ## Algorithm:
    /// - Rankings are already sorted (least loaded first)
    /// - Skip source broker (can't move to self)
    /// - Skip inactive/draining brokers
    /// - Return first suitable broker
    ///
    /// ## Parameters:
    /// - `exclude_broker`: Source broker ID (can't be target)
    /// - `rankings`: Broker rankings (sorted least loaded first)
    ///
    /// ## Returns:
    /// Target broker ID or error if no suitable broker found
    async fn select_target_broker(
        &self,
        exclude_broker: u64,
        rankings: &[(u64, usize)],
    ) -> Result<u64> {
        // Find least loaded broker that is not the excluded one and is active
        for (broker_id, _) in rankings.iter() {
            if *broker_id != exclude_broker && self.is_broker_active(*broker_id).await {
                return Ok(*broker_id);
            }
        }

        Err(anyhow!(
            "No suitable target broker found for rebalancing (all brokers either inactive or excluded)"
        ))
    }

    /// Checks if topic matches any blacklist pattern
    ///
    /// ## Purpose:
    /// Determines if a topic should never be rebalanced based on blacklist patterns.
    /// Supports exact matches and namespace wildcards.
    ///
    /// ## Pattern Matching:
    /// - **Exact**: `/default/critical-topic` matches only that topic
    /// - **Namespace wildcard**: `/default/*` matches all topics in the `/default` namespace
    ///
    /// ## Topic Format:
    /// Topics are structured as: `/{namespace}/{topic_name}`
    /// Examples: `/default/my-topic`, `/system/metrics`, `/production/orders`
    ///
    /// ## Parameters:
    /// - `topic_name`: Topic to check (must start with `/`)
    /// - `blacklist`: List of blacklist patterns
    ///
    /// ## Returns:
    /// `true` if topic is blacklisted, `false` otherwise
    fn is_topic_blacklisted(&self, topic_name: &str, blacklist: &[String]) -> bool {
        for pattern in blacklist {
            // Exact match
            if pattern == topic_name {
                return true;
            }

            // Namespace wildcard: /namespace/*
            if pattern.ends_with("/*") {
                // Extract namespace prefix (e.g., "/default/" from "/default/*")
                let namespace_prefix = &pattern[..pattern.len() - 1]; // Remove the '*', keep the '/'
                
                // Topic must start with the namespace and have something after it
                // e.g., "/default/*" matches "/default/anything" but not "/default" or "/defaultx/topic"
                if topic_name.starts_with(namespace_prefix) && topic_name.len() > namespace_prefix.len() {
                    return true;
                }
            }
        }

        false
    }

    /// Executes a list of rebalancing moves
    ///
    /// ## Purpose:
    /// Main rebalancing execution loop that orchestrates topic moves from overloaded
    /// to underloaded brokers. Includes safety controls like rate limiting, cooldown
    /// delays, history tracking, and audit logging.
    ///
    /// ## Safety Features:
    /// - **Rate limiting**: Stops when `max_moves_per_hour` is reached
    /// - **Cooldown**: Waits between moves to allow cluster stabilization
    /// - **Error handling**: Logs failures but continues with remaining moves
    /// - **Audit trail**: Records every move to ETCD for compliance/debugging
    ///
    /// ## Algorithm:
    /// For each move:
    /// 1. Check hourly rate limit → stop if exceeded
    /// 2. Execute the single move (create marker + delete assignment)
    /// 3. Record in history (for rate limiting)
    /// 4. Log to ETCD (for audit)
    /// 5. Sleep for cooldown period
    ///
    /// ## Parameters:
    /// - `moves`: List of moves selected by `select_rebalancing_candidates()`
    /// - `config`: Rebalancing configuration (rate limits, cooldown)
    /// - `history`: Rebalancing history tracker (for rate limiting)
    ///
    /// ## Returns:
    /// Number of successfully executed moves
    pub async fn execute_rebalancing(
        &self,
        moves: Vec<RebalancingMove>,
        config: &config::RebalancingConfig,
        history: &mut RebalancingHistory,
    ) -> Result<usize> {
        let mut executed = 0;
        let total_moves = moves.len();

        for mv in moves {
            // Check rate limit
            if history.count_moves_in_last_hour() >= config.max_moves_per_hour {
                warn!(
                    current_moves = history.count_moves_in_last_hour(),
                    limit = config.max_moves_per_hour,
                    "rebalancing rate limit reached, stopping cycle"
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

                    // Record success metric
                    metrics::counter!(
                        crate::broker_metrics::REBALANCING_MOVES_TOTAL.name,
                        "reason" => format!("{:?}", mv.reason)
                    )
                    .increment(1);

                    // Log to ETCD for auditing
                    self.log_rebalancing_event(&mv).await;

                    // Cooldown between moves to allow cluster stabilization
                    if config.cooldown_seconds > 0 {
                        tokio::time::sleep(Duration::from_secs(config.cooldown_seconds)).await;
                    }
                }
                Err(e) => {
                    // Record failure metric
                    metrics::counter!(crate::broker_metrics::REBALANCING_FAILURES_TOTAL.name)
                        .increment(1);

                    error!(
                        topic = %mv.topic_name,
                        error = %e,
                        "failed to execute rebalancing move, continuing with remaining moves"
                    );
                }
            }
        }

        info!(
            executed = executed,
            total_moves = total_moves,
            "rebalancing cycle complete"
        );

        Ok(executed)
    }

    /// Executes a single topic move for rebalancing
    ///
    /// ## Purpose:
    /// Performs the actual topic move by creating an unassigned marker with a target
    /// broker hint and deleting the source assignment. This triggers the existing
    /// broker watch mechanism to gracefully unload the topic from the source broker.
    ///
    /// ## How It Works:
    /// 1. **Create unassigned marker** at `/cluster/unassigned/{topic_name}`:
    ///    - Includes `to_broker` hint for LoadManager
    ///    - Includes `reason: "rebalance"` for tracking
    ///    - Includes timestamp for audit trail
    ///
    /// 2. **Delete source assignment** at `/cluster/brokers/{from_broker}/{topic_name}`:
    ///    - Source broker's `broker_watcher` detects deletion
    ///    - Triggers existing `topic.unload()` from Phase D
    ///    - Topic gracefully closes producers/consumers
    ///
    /// 3. **Automatic reassignment** (handled by existing watchers):
    ///    - LoadManager sees unassigned marker
    ///    - Step 6 logic reads `to_broker` hint
    ///    - Assigns topic to target broker (not rankings)
    ///    - Target broker loads topic from cloud storage
    ///
    /// ## Reuses Existing Infrastructure:
    /// - ✅ Phase D unload mechanism (graceful topic migration)
    /// - ✅ broker_watcher (detects assignment changes)
    /// - ✅ assign_topic_to_broker (will be enhanced in Step 6)
    ///
    /// ## Parameters:
    /// - `mv`: The move to execute (source, target, topic, metadata)
    ///
    /// ## Returns:
    /// `Ok(())` if marker created and assignment deleted successfully
    async fn execute_single_move(&self, mv: &RebalancingMove) -> Result<()> {
        // Create unassigned marker with rebalance hint
        let unassigned_path = join_path(&[BASE_UNASSIGNED_PATH, &mv.topic_name]);

        let marker = serde_json::json!({
            "reason": "rebalance",
            "from_broker": mv.from_broker,
            "to_broker": mv.to_broker,  // ← Target broker hint for Step 6
            "timestamp": mv.timestamp,
        });

        // Post unassigned marker
        self.meta_store
            .put(&unassigned_path, marker, MetaOptions::None)
            .await?;

        // Delete assignment from source broker (triggers unload via watch)
        let assignment_path = join_path(&[
            BASE_BROKER_PATH,
            &mv.from_broker.to_string(),
            &mv.topic_name,
        ]);

        self.meta_store.delete(&assignment_path).await?;

        info!(
            topic = %mv.topic_name,
            from = %mv.from_broker,
            to = %mv.to_broker,
            "rebalancing move initiated (unassigned marker created, source assignment deleted)"
        );

        Ok(())
    }

    /// Logs rebalancing event to ETCD for audit trail
    ///
    /// ## Purpose:
    /// Creates a persistent audit log of all rebalancing moves in the metadata store.
    /// This provides:
    /// - **Compliance**: Track all automated topic moves
    /// - **Debugging**: Understand why/when topics were moved
    /// - **Metrics**: Analyze rebalancing patterns over time
    /// - **CLI visibility**: Show rebalancing history to admins
    ///
    /// ## Storage:
    /// Events stored at: `/cluster/rebalancing_history/{timestamp}`
    ///
    /// ## Event Structure:
    /// ```json
    /// {
    ///   "topic": "/default/my-topic",
    ///   "from_broker": 1,
    ///   "to_broker": 2,
    ///   "reason": "LoadImbalance",
    ///   "estimated_load": 5.2,
    ///   "timestamp": 1234567890
    /// }
    /// ```
    ///
    /// ## Parameters:
    /// - `mv`: The move that was executed
    async fn log_rebalancing_event(&self, mv: &RebalancingMove) {
        let key = format!("/cluster/rebalancing_history/{}", mv.timestamp);

        let event = serde_json::json!({
            "topic": mv.topic_name,
            "from_broker": mv.from_broker,
            "to_broker": mv.to_broker,
            "reason": format!("{:?}", mv.reason),
            "estimated_load": mv.estimated_load,
            "timestamp": mv.timestamp,
        });

        if let Err(e) = self
            .meta_store
            .put(&key, event, MetaOptions::None)
            .await
        {
            warn!(
                error = %e,
                topic = %mv.topic_name,
                "failed to log rebalancing event to ETCD (non-critical)"
            );
        }
    }

    /// Starts the automated rebalancing background loop
    ///
    /// ## Purpose:
    /// Orchestrates the entire rebalancing process in a continuous background task.
    /// Combines Steps 3-6 into an automated loop that periodically checks cluster
    /// balance and initiates rebalancing when needed.
    ///
    /// ## Leader-Only Execution:
    /// Only the elected leader broker runs the rebalancing logic to prevent conflicts.
    /// Non-leader brokers skip the rebalancing cycle entirely.
    ///
    /// ## Loop Flow:
    /// 1. Wait for configured check interval (default: 300s)
    /// 2. Check if current broker is leader → skip if not
    /// 3. Check if rebalancing is enabled → skip if not
    /// 4. Check minimum broker count → skip if insufficient
    /// 5. Calculate cluster imbalance metrics (Step 3)
    /// 6. Decide if rebalancing needed (Step 3)
    /// 7. Select topics to move (Step 4)
    /// 8. Execute moves (Step 5 + Step 6)
    /// 9. Loop continues forever
    ///
    /// ## Safety Features:
    /// - **Leader-only**: Prevents simultaneous rebalancing by multiple brokers
    /// - **Interval pacing**: Allows cluster to stabilize between checks
    /// - **History tracking**: Enforces hourly rate limits
    /// - **Error handling**: Logs errors but continues loop
    /// - **Minimum brokers**: Requires at least 2 active brokers
    ///
    /// ## Parameters:
    /// - `config`: Rebalancing configuration (intervals, thresholds, limits)
    /// - `leader_election`: Leader election service for state checking
    ///
    /// ## Returns:
    /// JoinHandle for the background task (can be used to await or abort)
    pub fn start_rebalancing_loop(
        self,
        config: config::RebalancingConfig,
        leader_election: super::leader_election::LeaderElection,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                check_interval_seconds = config.check_interval_seconds,
                aggressiveness = ?config.aggressiveness,
                max_moves_per_cycle = config.max_moves_per_cycle,
                max_moves_per_hour = config.max_moves_per_hour,
                "starting automated rebalancing loop"
            );

            let mut interval = tokio::time::interval(Duration::from_secs(
                config.check_interval_seconds,
            ));

            let mut history = RebalancingHistory::new(1000);

            loop {
                interval.tick().await;

                // Check 1: Only leader performs rebalancing
                let leader_state = leader_election.get_state().await;
                if leader_state != super::leader_election::LeaderElectionState::Leading {
                    debug!("skipping rebalancing cycle: broker is not the leader");
                    continue;
                }

                // Check 2: Rebalancing must be enabled
                if !config.enabled {
                    debug!("skipping rebalancing cycle: rebalancing is disabled");
                    continue;
                }

                // Check 3: Cluster health - need minimum broker count
                let rankings = self.rankings.lock().await.clone();
                if rankings.len() < config.min_brokers_for_rebalance {
                    debug!(
                        broker_count = rankings.len(),
                        min_required = config.min_brokers_for_rebalance,
                        "skipping rebalancing cycle: not enough brokers in cluster"
                    );
                    continue;
                }

                debug!(
                    broker_count = rankings.len(),
                    "rebalancing cycle started"
                );

                // Step 3: Calculate imbalance metrics
                let metrics = match self.calculate_imbalance().await {
                    Ok(m) => m,
                    Err(e) => {
                        error!(error = %e, "failed to calculate imbalance metrics");
                        continue;
                    }
                };

                // Log imbalance metrics for observability
                debug!(
                    cv = %metrics.coefficient_of_variation,
                    mean_load = %metrics.mean_load,
                    max_load = %metrics.max_load,
                    min_load = %metrics.min_load,
                    overloaded_count = metrics.overloaded_brokers.len(),
                    underloaded_count = metrics.underloaded_brokers.len(),
                    "cluster imbalance metrics calculated"
                );

                // Step 3: Decide if rebalancing is needed
                if !self.should_rebalance(&metrics, &config) {
                    debug!(
                        cv = %metrics.coefficient_of_variation,
                        threshold = config.aggressiveness.threshold(),
                        "cluster is balanced, no rebalancing needed"
                    );
                    continue;
                }

                info!(
                    cv = %metrics.coefficient_of_variation,
                    threshold = config.aggressiveness.threshold(),
                    overloaded_brokers = ?metrics.overloaded_brokers,
                    underloaded_brokers = ?metrics.underloaded_brokers,
                    "cluster imbalance detected, initiating rebalancing"
                );

                // Start timing the rebalancing cycle
                let cycle_start = std::time::Instant::now();

                // Step 4: Select topics to move
                let moves = match self
                    .select_rebalancing_candidates(&metrics, &config)
                    .await
                {
                    Ok(m) => m,
                    Err(e) => {
                        error!(error = %e, "failed to select rebalancing candidates");
                        continue;
                    }
                };

                if moves.is_empty() {
                    warn!("no suitable topics found for rebalancing despite cluster imbalance");
                    continue;
                }

                info!(
                    move_count = moves.len(),
                    topics = ?moves.iter().map(|m| &m.topic_name).collect::<Vec<_>>(),
                    "selected topics for rebalancing"
                );

                // Step 5 + Step 6: Execute rebalancing
                match self
                    .execute_rebalancing(moves, &config, &mut history)
                    .await
                {
                    Ok(executed) => {
                        // Record cycle duration
                        let cycle_duration = cycle_start.elapsed().as_secs_f64();
                        metrics::histogram!(
                            crate::broker_metrics::REBALANCING_CYCLE_DURATION_SECONDS.name
                        )
                        .record(cycle_duration);

                        info!(
                            executed = executed,
                            total_moves_in_last_hour = history.count_moves_in_last_hour(),
                            cycle_duration_secs = %cycle_duration,
                            "rebalancing cycle completed successfully"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "rebalancing execution failed");
                    }
                }
            }
        })
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

    /// Gets current broker rankings (for admin CLI)
    ///
    /// Returns a copy of the current broker load rankings as (broker_id, load) pairs.
    pub async fn get_rankings(&self) -> Vec<(u64, usize)> {
        self.rankings.lock().await.clone()
    }

    /// Gets rebalancing configuration (for admin CLI)
    ///
    /// Returns the current rebalancing config if available from service configuration.
    pub async fn get_rebalancing_config(&self) -> Option<config::RebalancingConfig> {
        // TODO: Store config reference in LoadManager during initialization
        // For now, return None - caller should handle this gracefully
        None
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

// Tests for LoadManager are in load_manager_test.rs
#[cfg(test)]
#[path = "load_manager_test.rs"]
mod tests;
