pub(crate) mod config;
mod rankings;
pub(crate) mod rebalancing;

#[cfg(test)]
#[path = "load_manager/rebalancing_test.rs"]
mod rebalancing_test;

use crate::broker_metrics::BROKER_ASSIGNMENTS_TOTAL;
use crate::danube_service::load_report::{LoadReport, TopicLoad};
use anyhow::{anyhow, Result};
use danube_metadata_store::EtcdGetOptions;
use danube_metadata_store::{MetaOptions, MetadataStorage, MetadataStore, WatchEvent, WatchStream};
use futures::stream::StreamExt;
use metrics::counter;
use rebalancing::{ImbalanceMetrics, RebalancingHistory, RebalancingMove};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
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
    pub(crate) broker_id: u64,
    pub(crate) meta_store: MetadataStorage,
    pub(crate) brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
    pub(crate) rankings: Arc<Mutex<Vec<(u64, usize)>>>,
    next_broker: Arc<Mutex<Option<u64>>>,
    assignment_strategy: config::AssignmentStrategy,
    rebalancing_config: Option<config::RebalancingConfig>,
}

impl LoadManager {
    /// Creates a new LoadManager instance with default configuration
    /// (Used in tests only - production code should use `with_config`)
    #[cfg(test)]
    pub fn new(broker_id: u64, meta_store: MetadataStorage) -> Self {
        Self::with_config(
            broker_id,
            meta_store,
            config::AssignmentStrategy::default(),
            None,
        )
    }

    /// Creates a new LoadManager instance with full configuration
    pub fn with_config(
        broker_id: u64,
        meta_store: MetadataStorage,
        assignment_strategy: config::AssignmentStrategy,
        rebalancing_config: Option<config::RebalancingConfig>,
    ) -> Self {
        LoadManager {
            broker_id,
            meta_store,
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(Mutex::new(Some(broker_id))),
            assignment_strategy,
            rebalancing_config,
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

        // Calculate initial broker rankings using configured strategy
        self.calculate_rankings().await;

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

        // Leader calculates new rankings using configured strategy
        self.calculate_rankings().await;

        // Update next_broker based on rankings
        let next_broker = self
            .rankings
            .lock()
            .await
            .get(0)
            .get_or_insert(&(broker_id, 0))
            .0;

        *self.next_broker.lock().await = Some(next_broker);

        Ok(())
    }

    // Post the topic on the broker address /cluster/brokers/{broker-id}/{namespace}/{topic}
    // to be further read and processed by the selected broker
    async fn assign_topic_to_broker(&mut self, event: WatchEvent) {
        match event {
            WatchEvent::Put { key, value, .. } => {
                // recalculate the rankings after the topic was assigned
                self.calculate_rankings().await;

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
                                if let Some(from_broker) =
                                    obj.get("from_broker").and_then(|v| v.as_u64())
                                {
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
                                if let Some(from_broker) =
                                    obj.get("from_broker").and_then(|v| v.as_u64())
                                {
                                    exclude_broker = Some(from_broker);
                                }
                                if let Some(to_broker) =
                                    obj.get("to_broker").and_then(|v| v.as_u64())
                                {
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
                        match self
                            .get_next_broker_excluding(exclude_broker.unwrap_or(0))
                            .await
                        {
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
                    load_report.topics.push(TopicLoad {
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

        let next_broker = if let Some(id) = chosen {
            *self.next_broker.lock().await = Some(id);
            id
        } else {
            // fallback to current value if none active found (should be rare)
            self.next_broker.lock().await.unwrap_or(self.broker_id)
        };

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
            if load_report
                .topics
                .iter()
                .any(|t| t.topic_name == topic_name)
            {
                return true;
            }
        }
        false
    }

    /// Calculates cluster imbalance metrics for rebalancing decisions
    /// (Delegates to rebalancing module)
    pub async fn calculate_imbalance(&self) -> Result<ImbalanceMetrics> {
        rebalancing::calculate_imbalance(self.rankings.clone()).await
    }

    /// Test helper: Checks if topic matches any blacklist pattern
    #[cfg(test)]
    pub(crate) fn is_topic_blacklisted(&self, topic_name: &str, blacklist: &[String]) -> bool {
        rebalancing::is_topic_blacklisted(topic_name, blacklist)
    }

    /// Determines if cluster needs rebalancing based on configuration and metrics
    /// (Delegates to rebalancing module)
    pub fn should_rebalance(
        &self,
        metrics: &ImbalanceMetrics,
        config: &config::RebalancingConfig,
    ) -> bool {
        rebalancing::should_rebalance(metrics, config)
    }

    /// Selects topics to move for rebalancing
    /// (Delegates to rebalancing module)
    pub async fn select_rebalancing_candidates(
        &self,
        metrics: &ImbalanceMetrics,
        config: &config::RebalancingConfig,
    ) -> Result<Vec<RebalancingMove>> {
        let is_broker_active = |broker_id| self.is_broker_active(broker_id);
        rebalancing::select_rebalancing_candidates(
            self.rankings.clone(),
            self.brokers_usage.clone(),
            metrics,
            config,
            is_broker_active,
        )
        .await
    }

    /// Executes a list of rebalancing moves
    /// (Delegates to rebalancing module)
    pub async fn execute_rebalancing(
        &self,
        moves: Vec<RebalancingMove>,
        config: &config::RebalancingConfig,
        history: &mut RebalancingHistory,
    ) -> Result<usize> {
        rebalancing::execute_rebalancing(&self.meta_store, moves, config, history).await
    }

    /// Starts the automated rebalancing background loop
    /// (Delegates to rebalancing module)
    pub fn start_rebalancing_loop(
        self,
        config: config::RebalancingConfig,
        leader_election: super::leader_election::LeaderElection,
    ) -> JoinHandle<()> {
        let rankings = self.rankings.clone();
        let brokers_usage = self.brokers_usage.clone();
        let meta_store = self.meta_store.clone();
        let meta_store_for_closure = meta_store.clone();

        // Create a boxed closure for is_broker_active
        let is_broker_active = move |broker_id: u64| -> std::pin::Pin<
            Box<dyn std::future::Future<Output = bool> + Send>,
        > {
            let meta_store = meta_store_for_closure.clone();
            Box::pin(async move {
                let state_path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string(), "state"]);
                match meta_store.get(&state_path, MetaOptions::None).await {
                    Ok(Some(val)) => val
                        .get("mode")
                        .and_then(|m| m.as_str())
                        .map(|m| m == "active")
                        .unwrap_or(true),
                    _ => true,
                }
            })
        };

        rebalancing::start_rebalancing_loop(
            rankings,
            brokers_usage,
            meta_store,
            config,
            leader_election,
            is_broker_active,
        )
    }

    /// Calculates broker rankings using the configured assignment strategy
    ///
    /// ## Purpose:
    /// Computes broker load rankings based on the assignment strategy:
    /// - Fair: Simple topic count only (predictable, testing-friendly)
    /// - Balanced: Multi-factor scoring with weighted topic load + CPU + Memory
    /// - WeightedLoad: Adaptive algorithm that detects and prioritizes bottlenecks
    async fn calculate_rankings(&self) {
        let broker_loads = match &self.assignment_strategy {
            config::AssignmentStrategy::Fair => {
                // Simple topic count for fair round-robin distribution
                rankings::rankings_simple(self.brokers_usage.clone()).await
            }
            config::AssignmentStrategy::Balanced => {
                // Balanced multi-factor: weighted topics + CPU + memory
                rankings::rankings_composite(self.brokers_usage.clone()).await
            }
            config::AssignmentStrategy::WeightedLoad => {
                // Smart adaptive: detects bottlenecks and prioritizes them
                rankings::rankings_weighted_load(self.brokers_usage.clone()).await
            }
        };
        *self.rankings.lock().await = broker_loads;
    }

    /// Gets current broker rankings (for admin CLI)
    ///
    /// Returns a copy of the current broker load rankings as (broker_id, load) pairs.
    pub async fn get_rankings(&self) -> Vec<(u64, usize)> {
        self.rankings.lock().await.clone()
    }

    /// Gets current broker usage reports (for admin CLI)
    ///
    /// Returns a copy of the broker usage HashMap containing LoadReport for each broker.
    pub async fn get_brokers_usage(&self) -> HashMap<u64, LoadReport> {
        self.brokers_usage.lock().await.clone()
    }

    /// Gets rebalancing configuration (for admin CLI)
    ///
    /// Returns the current rebalancing config if available from service configuration.
    pub async fn get_rebalancing_config(&self) -> Option<config::RebalancingConfig> {
        self.rebalancing_config.clone()
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
