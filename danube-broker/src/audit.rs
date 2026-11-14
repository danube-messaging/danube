use crate::metadata_storage::MetadataStorage;
use crate::resources::TopicResources;
use anyhow::Result;
use danube_core::metadata::{MetaOptions, MetadataStore};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

const AUDIT_PREFIX: &str = "/cluster/audit";
const MAX_AUDIT_ENTRIES: usize = 1000;
static AUDIT_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AuditEvent {
    pub(crate) timestamp_ms: u64,
    pub(crate) actor: String,
    pub(crate) action: String,
    pub(crate) target: String,
    pub(crate) details: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CursorSnapshot {
    pub(crate) timestamp_ms: u64,
    pub(crate) topic: String,
    pub(crate) reason: String,
    pub(crate) cursors: BTreeMap<String, Option<u64>>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Record a control-plane audit event under `/cluster/audit/{timestamp_ms}_{seq}`
/// and trim oldest entries once the ring exceeds 1,000 entries.
pub(crate) async fn record_audit_event(
    store: &MetadataStorage,
    actor: &str,
    action: &str,
    target: &str,
    details: serde_json::Value,
) -> Result<()> {
    let ts = now_ms();
    let seq = AUDIT_SEQ.fetch_add(1, Ordering::Relaxed);
    let key = format!("{}/{:016}_{:06}", AUDIT_PREFIX, ts, seq % 1_000_000);
    let event = AuditEvent {
        timestamp_ms: ts,
        actor: actor.to_string(),
        action: action.to_string(),
        target: target.to_string(),
        details,
    };
    store
        .put(&key, serde_json::to_value(event)?, MetaOptions::None)
        .await?;

    // Best-effort bounded ring cleanup
    if let Ok(mut entries) = store.get_bulk(AUDIT_PREFIX).await {
        if entries.len() > MAX_AUDIT_ENTRIES {
            entries.sort_by(|a, b| a.key.cmp(&b.key));
            let to_remove = entries.len() - MAX_AUDIT_ENTRIES;
            for old_entry in entries.into_iter().take(to_remove) {
                let _ = store.delete(&old_entry.key).await;
            }
        }
    }
    Ok(())
}

/// Snapshot all subscription cursors for `topic_name` before destructive operations
/// (`unload_topic`, `delete_topic`, etc.) into `/topics/{ns}/{topic}/snapshots/{ts}`.
pub(crate) async fn snapshot_topic_cursors(
    topic_resources: &TopicResources,
    store: &MetadataStorage,
    topic_name: &str,
    reason: &str,
) -> Result<()> {
    let subscriptions = topic_resources.get_subscription_for_topic(topic_name).await;
    let mut cursors = BTreeMap::new();
    for sub in subscriptions {
        let cursor = topic_resources
            .get_subscription_cursor(&sub, topic_name)
            .await
            .unwrap_or(None);
        cursors.insert(sub, cursor);
    }

    let ts = now_ms();
    let clean_topic = topic_name.trim_start_matches('/');
    let path = format!("/topics/{}/snapshots/{}", clean_topic, ts);
    let snap = CursorSnapshot {
        timestamp_ms: ts,
        topic: topic_name.to_string(),
        reason: reason.to_string(),
        cursors,
    };
    store
        .put(&path, serde_json::to_value(snap)?, MetaOptions::None)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use danube_core::metadata::MemoryStore;

    #[tokio::test]
    async fn test_audit_and_cursor_snapshot() {
        let mem = MemoryStore::new().await.unwrap();
        let store = MetadataStorage::InMemory(mem);
        let topic_res = TopicResources::new(store.clone());

        record_audit_event(
            &store,
            "alice",
            "create_topic",
            "/default/t1",
            serde_json::json!({"partitions": 1}),
        )
        .await
        .unwrap();

        let keys = store.get_bulk(AUDIT_PREFIX).await.unwrap();
        assert_eq!(keys.len(), 1);

        snapshot_topic_cursors(&topic_res, &store, "/default/t1", "before_unload")
            .await
            .unwrap();
        let snap_keys = store
            .get_bulk("/topics/default/t1/snapshots")
            .await
            .unwrap();
        assert_eq!(snap_keys.len(), 1);
    }
}

