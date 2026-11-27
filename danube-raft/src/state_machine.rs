//! Raft state machine — the replicated KV store.
//!
//! This is the core of the Danube metadata layer. Every committed Raft log
//! entry is applied here, mutating the in-memory BTreeMap and emitting
//! watch events via broadcast channels.
//!
//! The state itself lives in memory, but every snapshot (built locally or
//! installed from the leader) is persisted in redb before openraft is told
//! about it. openraft purges log entries covered by a snapshot, so on restart
//! the state machine must be rebuilt from the persisted snapshot; the log then
//! replays only the entries after it.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use redb::{Database, TableDefinition};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, SnapshotMeta,
    StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info};

use danube_core::metadata::WatchEvent;

use crate::commands::{RaftCommand, RaftResponse, VersionedValue};
use crate::typ::TypeConfig;

// Type aliases for readability — openraft 0.9 generics use NodeId + Node, not TypeConfig.
type NodeId = u64;
type Node = BasicNode;

/// redb table holding the latest snapshot. Same database file as the Raft log.
const SNAPSHOT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("sm_snapshot");
const SNAPSHOT_META_KEY: &str = "meta";
const SNAPSHOT_DATA_KEY: &str = "data";

/// Snapshot of the full state machine, serialized as JSON (also the wire format
/// used when the leader sends a snapshot to a follower).
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
    /// Database where snapshots are persisted (shared with the Raft log store).
    db: Arc<Database>,
}

impl DanubeStateMachine {
    /// Open the state machine, restoring it from the last persisted snapshot
    /// if there is one. openraft then replays committed log entries after it.
    pub fn open(db: Arc<Database>) -> Result<Self, StorageError<NodeId>> {
        let mut data = StateMachineData {
            kv: BTreeMap::new(),
            watchers: Arc::new(DashMap::new()),
            ttl_entries: BTreeMap::new(),
            counters: BTreeMap::new(),
            global_revision: 0,
            last_applied_log: None,
            last_membership: StoredMembership::default(),
        };

        // Create the snapshot table if it does not exist yet.
        let write_txn = db
            .begin_write()
            .map_err(|e| snapshot_err(e, ErrorVerb::Write))?;
        {
            let _ = write_txn
                .open_table(SNAPSHOT_TABLE)
                .map_err(|e| snapshot_err(e, ErrorVerb::Write))?;
        }
        write_txn
            .commit()
            .map_err(|e| snapshot_err(e, ErrorVerb::Write))?;

        if let Some((meta, raw)) = read_persisted_snapshot(&db)? {
            let snap = decode_snapshot(&raw)?;
            from_snapshot(&mut data, snap);
            info!(
                snapshot_id = %meta.snapshot_id,
                last_log_id = ?meta.last_log_id,
                keys = data.kv.len(),
                "state machine restored from persisted snapshot"
            );
        }

        Ok(Self {
            data: Arc::new(tokio::sync::RwLock::new(data)),
            db,
        })
    }

    /// Last log id applied to the state machine (restored from the snapshot on open).
    pub async fn last_applied_log_id(&self) -> Option<LogId<NodeId>> {
        self.data.read().await.last_applied_log
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
            if !d.watchers.is_empty() {
                notify_watchers(
                    &d.watchers,
                    WatchEvent::Put {
                        key: key.into_bytes(),
                        value: serde_json::to_vec(&value).unwrap_or_default(),
                        mod_revision: Some(d.global_revision),
                        version: Some(ver),
                    },
                );
            }
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
            if !d.watchers.is_empty() {
                notify_watchers(
                    &d.watchers,
                    WatchEvent::Put {
                        key: key.into_bytes(),
                        value: serde_json::to_vec(&value).unwrap_or_default(),
                        mod_revision: Some(d.global_revision),
                        version: Some(ver),
                    },
                );
            }
            RaftResponse::Ok
        }

        RaftCommand::Delete { key } => {
            d.global_revision += 1;
            d.kv.remove(&key);
            if !d.watchers.is_empty() {
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

        RaftCommand::DeletePrefix { prefix } => {
            d.global_revision += 1;
            let keys_to_delete: Vec<String> =
                d.kv.range(prefix.clone()..)
                    .take_while(|(k, _)| k.starts_with(&prefix))
                    .map(|(k, _)| k.clone())
                    .collect();
            let has_watchers = !d.watchers.is_empty();
            for key in keys_to_delete {
                d.kv.remove(&key);
                if has_watchers {
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

        RaftCommand::ExpireTTLKeys { keys } => {
            d.global_revision += 1;
            let now = now_ms();
            let expired_timestamps: Vec<u64> =
                d.ttl_entries.range(..=now).map(|(ts, _)| *ts).collect();
            for ts in expired_timestamps {
                d.ttl_entries.remove(&ts);
            }
            let has_watchers = !d.watchers.is_empty();
            for key in keys {
                if d.kv.remove(&key).is_some() && has_watchers {
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
                if !d.watchers.is_empty() {
                    notify_watchers(
                        &d.watchers,
                        WatchEvent::Put {
                            key: key.into_bytes(),
                            value: serde_json::to_vec(&new_value).unwrap_or_default(),
                            mod_revision: Some(d.global_revision),
                            version: Some(ver),
                        },
                    );
                }
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
    if watchers.is_empty() {
        return;
    }
    let key_bytes = match &event {
        WatchEvent::Put { key, .. } | WatchEvent::Delete { key, .. } => key.as_slice(),
    };
    let key_str = String::from_utf8_lossy(key_bytes);
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
            db: self.db.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let raw = snapshot.into_inner();
        let snap = decode_snapshot(&raw)?;
        // Persist first: once this returns, openraft may purge the log it covers.
        persist_snapshot(&self.db, meta, &raw)?;
        let mut d = self.data.write().await;
        from_snapshot(&mut d, snap);
        debug!(snapshot_id = %meta.snapshot_id, "installed and persisted snapshot");
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(read_persisted_snapshot(&self.db)?.map(|(meta, raw)| Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(raw)),
        }))
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

        drop(d);

        let last = snap.last_applied_log.unwrap_or_default();
        let meta = SnapshotMeta {
            last_log_id: snap.last_applied_log,
            last_membership: snap.last_membership,
            snapshot_id: format!("snap-{}-{}", last.leader_id, last.index),
        };
        // Persist before returning: openraft purges the log up to this snapshot
        // right after it is built, so it must survive a restart.
        persist_snapshot(&self.db, &meta, &data)?;
        debug!(snapshot_id = %meta.snapshot_id, "built and persisted snapshot");

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

fn snapshot_err(e: impl std::fmt::Display, verb: ErrorVerb) -> StorageError<NodeId> {
    StorageError::from_io_error(
        ErrorSubject::Snapshot(None),
        verb,
        std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
    )
}

fn decode_snapshot(raw: &[u8]) -> Result<StateMachineSnapshot, StorageError<NodeId>> {
    serde_json::from_slice(raw).map_err(|e| snapshot_err(e, ErrorVerb::Read))
}

/// Write snapshot meta and data in one redb transaction (fsynced on commit).
fn persist_snapshot(
    db: &Database,
    meta: &SnapshotMeta<NodeId, Node>,
    raw: &[u8],
) -> Result<(), StorageError<NodeId>> {
    let meta_bytes = serde_json::to_vec(meta).map_err(|e| snapshot_err(e, ErrorVerb::Write))?;
    let write_txn = db
        .begin_write()
        .map_err(|e| snapshot_err(e, ErrorVerb::Write))?;
    {
        let mut table = write_txn
            .open_table(SNAPSHOT_TABLE)
            .map_err(|e| snapshot_err(e, ErrorVerb::Write))?;
        table
            .insert(SNAPSHOT_META_KEY, meta_bytes.as_slice())
            .map_err(|e| snapshot_err(e, ErrorVerb::Write))?;
        table
            .insert(SNAPSHOT_DATA_KEY, raw)
            .map_err(|e| snapshot_err(e, ErrorVerb::Write))?;
    }
    write_txn
        .commit()
        .map_err(|e| snapshot_err(e, ErrorVerb::Write))?;
    Ok(())
}

/// Read the persisted snapshot, if any.
fn read_persisted_snapshot(
    db: &Database,
) -> Result<Option<(SnapshotMeta<NodeId, Node>, Vec<u8>)>, StorageError<NodeId>> {
    let read_txn = db
        .begin_read()
        .map_err(|e| snapshot_err(e, ErrorVerb::Read))?;
    let table = match read_txn.open_table(SNAPSHOT_TABLE) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(snapshot_err(e, ErrorVerb::Read)),
    };
    let meta = table
        .get(SNAPSHOT_META_KEY)
        .map_err(|e| snapshot_err(e, ErrorVerb::Read))?;
    let data = table
        .get(SNAPSHOT_DATA_KEY)
        .map_err(|e| snapshot_err(e, ErrorVerb::Read))?;
    match (meta, data) {
        (Some(meta), Some(data)) => {
            let meta: SnapshotMeta<NodeId, Node> = serde_json::from_slice(meta.value())
                .map_err(|e| snapshot_err(e, ErrorVerb::Read))?;
            Ok(Some((meta, data.value().to_vec())))
        }
        _ => Ok(None),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
