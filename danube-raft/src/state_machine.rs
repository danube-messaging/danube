//! Raft state machine — the replicated KV store.
//!
//! This is the core of the Danube metadata layer. Every committed Raft log
//! entry is applied here, mutating the in-memory BTreeMap and emitting
//! watch events via broadcast channels.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError,
    StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::debug;

use danube_core::metadata::WatchEvent;

use crate::commands::{RaftCommand, RaftResponse, VersionedValue};
use crate::typ::TypeConfig;

// Type aliases for readability — openraft 0.9 generics use NodeId + Node, not TypeConfig.
type NodeId = u64;
type Node = BasicNode;

/// Snapshot of the full state machine, serialized via bincode.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct StateMachineSnapshot {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, Node>,
    pub kv: BTreeMap<String, VersionedValue>,
    pub ttl_entries: BTreeMap<u64, Vec<String>>,
    pub counters: BTreeMap<String, u64>,
    pub global_revision: i64,
}

/// Shared inner state accessible by both the state machine (via Raft) and
/// external consumers (RaftMetadataStore, ttl_worker) through `SharedStateMachineData`.
pub struct StateMachineData {
    pub kv: BTreeMap<String, VersionedValue>,
    pub watchers: Arc<DashMap<String, broadcast::Sender<WatchEvent>>>,
    pub ttl_entries: BTreeMap<u64, Vec<String>>,
    pub counters: BTreeMap<String, u64>,
    pub global_revision: i64,
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, Node>,
}

/// Thread-safe handle to the state machine data. Clone this and pass it to
/// `RaftMetadataStore` and `ttl_worker` before handing the SM to `Raft::new`.
pub type SharedStateMachineData = Arc<tokio::sync::RwLock<StateMachineData>>;

/// The replicated state machine. Holds shared data behind `Arc<RwLock<>>`
/// so external consumers can read it while Raft owns the SM.
pub struct DanubeStateMachine {
    data: SharedStateMachineData,
}

impl DanubeStateMachine {
    pub fn new() -> Self {
        Self {
            data: Arc::new(tokio::sync::RwLock::new(StateMachineData {
                kv: BTreeMap::new(),
                watchers: Arc::new(DashMap::new()),
                ttl_entries: BTreeMap::new(),
                counters: BTreeMap::new(),
                global_revision: 0,
                last_applied_log: None,
                last_membership: StoredMembership::default(),
            })),
        }
    }

    /// Get a cloneable handle to the shared data. Call this **before** passing
    /// the state machine to `Raft::new`.
    pub fn shared_data(&self) -> SharedStateMachineData {
        self.data.clone()
    }
}

/// Apply a single command to the shared data (called under write lock).
fn apply_command(d: &mut StateMachineData, cmd: RaftCommand) -> RaftResponse {
    match cmd {
        RaftCommand::Put { key, value } => {
            d.global_revision += 1;
            let ver = d.kv.get(&key).map(|v| v.version + 1).unwrap_or(1);
            let now = now_ms();
            d.kv.insert(
                key.clone(),
                VersionedValue {
                    value: value.clone(),
                    version: ver,
                    mod_revision: d.global_revision,
                    ttl_ms: None,
                    created_at_ms: now,
                },
            );
            notify_watchers(
                &d.watchers,
                WatchEvent::Put {
                    key: key.into_bytes(),
                    value: serde_json::to_vec(&value).unwrap_or_default(),
                    mod_revision: Some(d.global_revision),
                    version: Some(ver),
                },
            );
            RaftResponse::Ok
        }

        RaftCommand::PutWithTTL { key, value, ttl_ms } => {
            d.global_revision += 1;
            let ver = d.kv.get(&key).map(|v| v.version + 1).unwrap_or(1);
            let now = now_ms();
            let expires_at = now + ttl_ms;

            // Remove any previous TTL entry for this key so renewals work correctly.
            d.ttl_entries.retain(|_ts, keys| {
                keys.retain(|k| k != &key);
                !keys.is_empty()
            });

            d.kv.insert(
                key.clone(),
                VersionedValue {
                    value: value.clone(),
                    version: ver,
                    mod_revision: d.global_revision,
                    ttl_ms: Some(ttl_ms),
                    created_at_ms: now,
                },
            );
            d.ttl_entries
                .entry(expires_at)
                .or_default()
                .push(key.clone());
            notify_watchers(
                &d.watchers,
                WatchEvent::Put {
                    key: key.into_bytes(),
                    value: serde_json::to_vec(&value).unwrap_or_default(),
                    mod_revision: Some(d.global_revision),
                    version: Some(ver),
                },
            );
            RaftResponse::Ok
        }

        RaftCommand::Delete { key } => {
            d.global_revision += 1;
            d.kv.remove(&key);
            notify_watchers(
                &d.watchers,
                WatchEvent::Delete {
                    key: key.into_bytes(),
                    mod_revision: Some(d.global_revision),
                    version: None,
                },
            );
            RaftResponse::Ok
        }

        RaftCommand::DeletePrefix { prefix } => {
            d.global_revision += 1;
            let keys_to_delete: Vec<String> =
                d.kv.range(prefix.clone()..)
                    .take_while(|(k, _)| k.starts_with(&prefix))
                    .map(|(k, _)| k.clone())
                    .collect();
            for key in keys_to_delete {
                d.kv.remove(&key);
                notify_watchers(
                    &d.watchers,
                    WatchEvent::Delete {
                        key: key.into_bytes(),
                        mod_revision: Some(d.global_revision),
                        version: None,
                    },
                );
            }
            RaftResponse::Ok
        }

        RaftCommand::ExpireTTLKeys { keys } => {
            d.global_revision += 1;
            let now = now_ms();
            let expired_timestamps: Vec<u64> =
                d.ttl_entries.range(..=now).map(|(ts, _)| *ts).collect();
            for ts in expired_timestamps {
                d.ttl_entries.remove(&ts);
            }
            for key in keys {
                if d.kv.remove(&key).is_some() {
                    notify_watchers(
                        &d.watchers,
                        WatchEvent::Delete {
                            key: key.into_bytes(),
                            mod_revision: Some(d.global_revision),
                            version: None,
                        },
                    );
                }
            }
            RaftResponse::Ok
        }

        RaftCommand::CompareAndSwap {
            key,
            expected,
            new_value,
            ttl_ms,
        } => {
            let current = d.kv.get(&key).cloned();
            let current_value = current.as_ref().map(|v| &v.value);
            let matches = match (&expected, current_value) {
                (None, None) => true,
                (Some(exp), Some(cur)) => exp == cur,
                _ => false,
            };

            if matches {
                d.global_revision += 1;
                let ver = current.as_ref().map(|v| v.version + 1).unwrap_or(1);
                let now = now_ms();
                d.kv.insert(
                    key.clone(),
                    VersionedValue {
                        value: new_value.clone(),
                        version: ver,
                        mod_revision: d.global_revision,
                        ttl_ms,
                        created_at_ms: now,
                    },
                );
                if let Some(ttl) = ttl_ms {
                    d.ttl_entries
                        .entry(now + ttl)
                        .or_default()
                        .push(key.clone());
                }
                notify_watchers(
                    &d.watchers,
                    WatchEvent::Put {
                        key: key.into_bytes(),
                        value: serde_json::to_vec(&new_value).unwrap_or_default(),
                        mod_revision: Some(d.global_revision),
                        version: Some(ver),
                    },
                );
                RaftResponse::Ok
            } else {
                RaftResponse::CasFailed { current }
            }
        }

        RaftCommand::AllocateMonotonicId { counter_key } => {
            let counter = d.counters.entry(counter_key).or_insert(0);
            *counter += 1;
            RaftResponse::AllocatedId(*counter)
        }
    }
}

fn notify_watchers(watchers: &DashMap<String, broadcast::Sender<WatchEvent>>, event: WatchEvent) {
    let key_str = match &event {
        WatchEvent::Put { key, .. } | WatchEvent::Delete { key, .. } => {
            String::from_utf8_lossy(key).to_string()
        }
    };
    for entry in watchers.iter() {
        if key_str.starts_with(entry.key()) {
            let _ = entry.value().send(event.clone());
        }
    }
}

fn to_snapshot(d: &StateMachineData) -> StateMachineSnapshot {
    StateMachineSnapshot {
        last_applied_log: d.last_applied_log,
        last_membership: d.last_membership.clone(),
        kv: d.kv.clone(),
        ttl_entries: d.ttl_entries.clone(),
        counters: d.counters.clone(),
        global_revision: d.global_revision,
    }
}

fn from_snapshot(d: &mut StateMachineData, snap: StateMachineSnapshot) {
    d.last_applied_log = snap.last_applied_log;
    d.last_membership = snap.last_membership;
    d.kv = snap.kv;
    d.ttl_entries = snap.ttl_entries;
    d.counters = snap.counters;
    d.global_revision = snap.global_revision;
}

impl RaftStateMachine<TypeConfig> for DanubeStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        let d = self.data.read().await;
        Ok((d.last_applied_log, d.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut d = self.data.write().await;
        let mut responses = Vec::new();

        for entry in entries {
            d.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(RaftResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    let resp = apply_command(&mut d, cmd);
                    debug!(log_id = ?entry.log_id, "applied command");
                    responses.push(resp);
                }
                EntryPayload::Membership(mem) => {
                    d.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    responses.push(RaftResponse::Ok);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Return a new state machine that shares the same data handle.
        // openraft will call build_snapshot() on the returned builder.
        DanubeStateMachine {
            data: self.data.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let raw = snapshot.into_inner();
        let snap: StateMachineSnapshot = serde_json::from_slice(&raw).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;
        let mut d = self.data.write().await;
        from_snapshot(&mut d, snap);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let d = self.data.read().await;
        let snap = to_snapshot(&d);
        let data = serde_json::to_vec(&snap).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::Other, e),
            )
        })?;

        if let Some(last) = snap.last_applied_log {
            Ok(Some(Snapshot {
                meta: SnapshotMeta {
                    last_log_id: Some(last),
                    last_membership: snap.last_membership,
                    snapshot_id: format!("snap-{}-{}", last.leader_id, last.index),
                },
                snapshot: Box::new(Cursor::new(data)),
            }))
        } else {
            Ok(None)
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for DanubeStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let d = self.data.read().await;
        let snap = to_snapshot(&d);
        let data = serde_json::to_vec(&snap).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::Other, e),
            )
        })?;

        let last = snap.last_applied_log.unwrap_or_default();
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: snap.last_applied_log,
                last_membership: snap.last_membership,
                snapshot_id: format!("snap-{}-{}", last.leader_id, last.index),
            },
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
