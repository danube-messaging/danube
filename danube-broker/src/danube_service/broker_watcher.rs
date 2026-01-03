use std::sync::Arc;

use danube_metadata_store::{MetaOptions, MetadataStorage, MetadataStore, WatchEvent};
use futures::StreamExt;
use tokio::time::{sleep, Duration};
use tracing::{error, info, trace, warn};

use crate::{
    broker_service::BrokerService,
    resources::{BASE_BROKER_PATH, BASE_UNASSIGNED_PATH},
    utils::join_path,
};

/// Monitors broker-specific topic assignment events from the metadata store
///
/// ## Purpose:
/// Watches for topic assignments directed to this broker and handles local
/// topic creation/deletion based on LoadManager decisions.
///
/// ## Event Processing:
/// - **Put Events**: Creates topics locally when assigned by LoadManager
/// - **Delete Events**: Removes topics when reassigned to other brokers
///
/// ## Process Flow:
/// 1. **Setup Watch**: Monitors `/cluster/brokers/{broker_id}/` path
/// 2. **Parse Events**: Extracts namespace/topic from assignment paths
/// 3. **Cache Verification**: Ensures metadata readiness before topic creation
/// 4. **Local Operations**: Creates/deletes topics in broker's local state
pub(crate) async fn watch_events_for_broker(
    meta_store: MetadataStorage,
    broker_service: Arc<BrokerService>,
    broker_id: u64,
) {
    // Create watch stream for broker-specific events
    let topic_assignment_path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string()]);

    // Watch for broker's assigned topics
    match meta_store.watch(&topic_assignment_path).await {
        Ok(mut watch_stream) => {
            // Process events in background task
            tokio::spawn(async move {
                while let Some(result) = watch_stream.next().await {
                    match result {
                        Ok(event) => {
                            info!("A new Watch event has been received {}", event);

                            match event {
                                WatchEvent::Put { key, .. } => {
                                    handle_put_event(&meta_store, &broker_service, broker_id, &key)
                                        .await;
                                }
                                WatchEvent::Delete { key, .. } => {
                                    handle_delete_event(
                                        &meta_store,
                                        &broker_service,
                                        broker_id,
                                        &key,
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Error receiving watch event: {}", e);
                        }
                    }
                }
            });
        }
        Err(e) => {
            error!("Failed to create watch stream: {}", e);
        }
    }
}

/// Handles a Put watch event under `/cluster/brokers/{broker_id}/...`
///
/// - Processes a topic assignment directed to this broker.
/// - Bounces the assignment if the broker is not active (draining/drained) by deleting the
///   assignment and posting an `unassigned` marker with reason `drain_redirect`.
/// - Verifies LocalCache readiness (dispatch/schema/policies) before ensuring the topic locally.
async fn handle_put_event(
    meta_store: &MetadataStorage,
    broker_service: &Arc<BrokerService>,
    broker_id: u64,
    key: &[u8],
) {
    let key_str = match std::str::from_utf8(key) {
        Ok(s) => s,
        Err(e) => {
            error!("Invalid UTF-8 in key: {}", e);
            return;
        }
    };

    let parts: Vec<_> = key_str.split('/').collect();
    if parts.len() < 6 {
        warn!("Invalid topic path format: {}", key_str);
        return;
    }
    let topic_name = format!("/{}/{}", parts[4], parts[5]);

    if bounce_if_not_active(meta_store, broker_id, key_str, &topic_name).await {
        return;
    }

    // Cache readiness verification with fallback
    // Verify required metadata is available before creating topic
    if let Err(err) = verify_cache_readiness_with_retry(
        broker_service,
        &topic_name,
        3,
        Duration::from_millis(500),
    )
    .await
    {
        error!(
            "Cache readiness verification failed for {}: {}",
            topic_name, err
        );
        return;
    }

    let manager = broker_service.topic_manager.clone();
    match manager.ensure_local(&topic_name).await {
        Ok((disp_strategy, _schema_subject)) => {
            // Get full schema info for logging (fast LocalCache read)
            let schema_info = match manager.get_schema_info(&topic_name).await {
                Some((subject, schema_id, schema_type)) => {
                    format!(
                        "subject={}, id={}, type={}",
                        subject, schema_id, schema_type
                    )
                }
                None => "none".to_string(),
            };
            info!(
                "Topic '{}' created on broker {} - Strategy: {}, Schema: {}",
                topic_name, broker_id, disp_strategy, schema_info
            );
        }
        Err(err) => {
            error!("Unable to create the topic due to error: {}", err)
        }
    }
}

/// Handles a Delete watch event under `/cluster/brokers/{broker_id}/...`
///
/// - Cleans up local topic state when the assignment is removed.
/// - If there is an `unassigned` marker with reason `unload`, performs a graceful unload
///   (e.g., flush/seal without deleting durable metadata). Otherwise, deletes the local topic.
/// - When broker is in `draining` mode, auto-transitions to `drained` if no local topics remain.
async fn handle_delete_event(
    meta_store: &MetadataStorage,
    broker_service: &Arc<BrokerService>,
    broker_id: u64,
    key: &[u8],
) {
    let key_str = match std::str::from_utf8(key) {
        Ok(s) => s,
        Err(e) => {
            error!("Invalid UTF-8 in key: {}", e);
            return;
        }
    };

    let parts: Vec<_> = key_str.split('/').collect();
    if parts.len() < 6 {
        warn!("Invalid topic path format: {}", key_str);
        return;
    }
    let topic_name = format!("/{}/{}", parts[4], parts[5]);
    let manager = broker_service.topic_manager.clone();

    // Determine if this Delete is part of an unload by inspecting unassigned marker
    let unassigned_key = join_path(&[BASE_UNASSIGNED_PATH, &topic_name]);
    let is_unload = match meta_store.get(&unassigned_key, MetaOptions::None).await {
        Ok(Some(value)) => value.get("reason").and_then(|r| r.as_str()) == Some("unload"),
        _ => false,
    };

    if is_unload {
        match manager.unload_topic(&topic_name).await {
            Ok(()) => info!(
                "Topic '{}' unloaded locally on broker {}",
                topic_name, broker_id
            ),
            Err(err) => {
                error!("Unable to unload the topic due to error: {}", err)
            }
        }
    } else {
        match manager.delete_local(&topic_name).await {
            Ok(_) => info!(
                "The topic {} , was successfully deleted from the broker {}",
                topic_name, broker_id
            ),
            Err(err) => {
                error!("Unable to delete the topic due to error: {}", err)
            }
        }
    }

    // Auto-transition to drained when broker is draining and has no topics left
    let state_path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string(), "state"]);
    let is_draining = match meta_store
        .get(&state_path, danube_metadata_store::MetaOptions::None)
        .await
    {
        Ok(Some(val)) => val.get("mode").and_then(|m| m.as_str()) == Some("draining"),
        _ => false,
    };
    if is_draining {
        let topics_left = broker_service.get_topics();
        if topics_left.is_empty() {
            let drained = serde_json::json!({
                "mode": "drained",
                "reason": "drain_complete"
            });
            if let Err(e) = meta_store
                .put(
                    &state_path,
                    drained,
                    danube_metadata_store::MetaOptions::None,
                )
                .await
            {
                warn!("Failed to set broker {} state to drained: {}", broker_id, e);
            } else {
                info!(
                    "Broker {} transitioned to drained (no topics remaining)",
                    broker_id
                );
            }
        }
    }
}

async fn is_broker_active(meta_store: &MetadataStorage, broker_id: u64) -> bool {
    let state_path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string(), "state"]);
    match meta_store
        .get(&state_path, danube_metadata_store::MetaOptions::None)
        .await
    {
        Ok(Some(val)) => val
            .get("mode")
            .and_then(|m| m.as_str())
            .map(|m| m == "active")
            .unwrap_or(true),
        _ => true,
    }
}

async fn bounce_if_not_active(
    meta_store: &MetadataStorage,
    broker_id: u64,
    key_str: &str,
    topic_name: &str,
) -> bool {
    if is_broker_active(meta_store, broker_id).await {
        return false;
    }
    if let Err(e) = meta_store.delete(key_str).await {
        warn!("Failed to delete assignment during drain redirect: {}", e);
        return true; // treated as handled to avoid loops
    }
    let unassigned_path = join_path(&[BASE_UNASSIGNED_PATH, topic_name]);
    let marker = serde_json::json!({
        "reason": "drain_redirect",
        "from_broker": broker_id
    });
    if let Err(e) = meta_store
        .put(
            &unassigned_path,
            marker,
            danube_metadata_store::MetaOptions::None,
        )
        .await
    {
        warn!("Failed to post unassigned drain_redirect marker: {}", e);
    }
    true
}

// Cache readiness verification with retry and fallback
// Verifies that required metadata (policy/schema/dispatch config) is available in LocalCache
// before calling create_topic_locally() to avoid watcher ordering races
async fn verify_cache_readiness_with_retry(
    broker_service: &Arc<BrokerService>,
    topic_name: &str,
    max_retries: u32,
    retry_delay: Duration,
) -> anyhow::Result<()> {
    for attempt in 0..max_retries {
        // Check if required metadata is available in LocalCache
        let (has_dispatch, has_policies) = {
            let resources = broker_service.resources.lock().await;
            let dispatch_strategy = resources.topic.get_dispatch_strategy(topic_name);
            let policies = resources.topic.get_policies(topic_name);

            (dispatch_strategy.is_some(), policies.is_some())
        };

        if has_dispatch {
            trace!(
                "Cache readiness verified for topic {} on attempt {}",
                topic_name,
                attempt + 1
            );
            return Ok(());
        }

        if attempt < max_retries - 1 {
            trace!(
                "Cache not ready for topic {} (dispatch: {}, policies: {}), retrying in {:?}",
                topic_name,
                has_dispatch,
                has_policies,
                retry_delay
            );
            sleep(retry_delay).await;
        }
    }

    warn!(
        "Cache readiness verification failed for topic {} after {} attempts",
        topic_name, max_retries
    );
    Err(anyhow::anyhow!(
        "Required metadata not available in LocalCache after {} retries",
        max_retries
    ))
}
