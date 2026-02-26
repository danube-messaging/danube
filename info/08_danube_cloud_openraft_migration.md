# Danube Cloud: Replace ETCD with Embedded openraft

## 1. Goal

**Completely remove ETCD** from the Danube cloud cluster. Replace it with an embedded Raft consensus layer (`openraft` + `redb`) so that the brokers themselves form the metadata cluster. This is a one-to-one migration — every capability ETCD provides today must be replicated by the new system with equivalent or better robustness.

After migration:
- **Zero external dependencies** for metadata — no ETCD to deploy, manage, or monitor
- **Same consistency guarantees** — linearizable reads/writes (Raft quorum)
- **Same fault tolerance** — survives N/2 broker failures (identical to ETCD's 3-of-5 or 2-of-3)
- **Faster failure detection** — Raft heartbeat (~1-2s) vs ETCD lease TTL (14-32s)
- **Simpler operations** — one binary, one cluster, one set of logs

---

## 2. What ETCD Does Today (Quick Reference)

See `info/07_etcd_dependency_analysis.md` for the full inventory. Summary of the 6 patterns:

| # | Pattern | ETCD Feature | Replacement Strategy |
|---|---------|--------------|---------------------|
| P1 | Broker Registration & Liveness | Leases + TTL + Keep-Alive | Raft membership + heartbeat |
| P2 | Leader Election | Lease CAS | Raft leader = cluster leader (**free**) |
| P3 | Metadata KV (topics, namespaces, schemas) | KV CRUD | Raft state machine KV |
| P4 | Watch/Event Bus | Prefix watches | State machine → broadcast channels |
| P5 | Load Reporting & Coordination | KV put + watches | Same Raft KV + watches |
| P6 | Persistent Storage Metadata (WAL) | KV CRUD | Same Raft KV |

**Key files that touch ETCD** (all changes concentrate here):

```
danube-metadata-store/
  src/lib.rs              — MetadataStorage enum (lease methods deleted in Phase 2)
  src/store.rs            — MetadataStore trait, MetaOptions (leaks etcd_client types)
  src/watch.rs            — WatchStream, WatchEvent (imports etcd_client)
  src/errors.rs           — ETCD-specific errors
  src/providers/etcd.rs   — EtcdStore implementation
  src/providers/in_memory.rs — MemoryStore (testing)
  Cargo.toml              — etcd-client dependency

danube-broker/
  src/main.rs                          — creates MetadataStorage::Etcd
  src/danube_service.rs                — orchestrates all services
  src/danube_service/broker_register.rs — ETCD lease-based registration
  src/danube_service/leader_election.rs — ETCD lease-based leader election
  src/danube_service/load_manager.rs    — watches ETCD for load reports
  src/danube_service/broker_watcher.rs  — watches ETCD for topic assignments
  src/danube_service/local_cache.rs     — populates from ETCD, watches for updates
  src/service_configuration.rs          — MetaStoreConfig (etcd host/port)

danube-persistent-storage/
  src/etcd_metadata.rs   — WAL metadata stored in ETCD
  src/wal_factory.rs     — uses EtcdMetadata
```

---

## 3. Technology Selection

### 3.1 openraft

| Attribute | Detail |
|-----------|--------|
| Crate | `openraft` (latest: 0.10.x) |
| Language | Pure Rust, async-native (Tokio) |
| Maturity | Production use: Databend (cloud data warehouse), CnosDB (time-series), GreptimeDB |
| API | Clean `RaftStateMachine` trait — maps directly to Danube's `MetadataStore` |
| Persistence | Pluggable: bring your own log storage and state machine storage |
| Network | Pluggable: bring your own RPC transport (gRPC, TCP, etc.) |
| License | MIT / Apache-2.0 |

### 3.2 redb (State Machine + Log Storage)

| Attribute | Detail |
|-----------|--------|
| Crate | `redb` (latest: 2.x) |
| Language | Pure Rust |
| Maturity | Stable (1.0+ since 2023), ACID with fsync |
| API | B-tree tables, transactions, zero-copy reads |
| Binary size | Tiny (~200KB added) |
| Cross-compile | Trivial (pure Rust, no C/C++ deps) |

**Why redb over rocksdb/sled?**
- `rocksdb`: C++ FFI, painful cross-compile, massive binary size. Overkill for metadata.
- `sled`: Author explicitly warns not production-ready (0.34). No stable release.
- `redb`: Stable, ACID, pure Rust, simple API. Metadata workload is B-tree-friendly (small KV, prefix scans, not write-heavy LSM).

---

## 4. Architecture

### 4.1 Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Danube Broker Node                      │
│                                                              │
│  ┌──────────┐    ┌──────────────────────────────────────┐   │
│  │ LocalCache│◄───│ RaftStore (MetadataStore trait impl) │   │
│  │ (DashMap) │    │                                      │   │
│  └──────────┘    │  ┌──────────────────────────────┐    │   │
│                  │  │ DanubeStateMachine            │    │   │
│                  │  │  ├── BTreeMap<String, Value>  │    │   │
│                  │  │  ├── broadcast watchers       │    │   │
│                  │  │  └── TTL expiration queue     │    │   │
│                  │  └──────────────────────────────┘    │   │
│                  │                                      │   │
│                  │  ┌──────────────────────────────┐    │   │
│                  │  │ openraft::Raft                │    │   │
│                  │  │  ├── Log store (redb)         │    │   │
│                  │  │  └── Vote/term state (redb)   │    │   │
│                  │  └──────────────────────────────┘    │   │
│                  └─────────────────┬────────────────────┘   │
│                                    │ gRPC (RaftTransport)    │
└────────────────────────────────────┼────────────────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
    ┌─────────┴──────────┐  ┌───────┴────────────┐  ┌──────┴──────────┐
    │  Broker Node 2     │  │  Broker Node 3     │  │  Broker Node N  │
    │  (Raft follower)   │  │  (Raft follower)   │  │  (Raft follower)│
    └────────────────────┘  └────────────────────┘  └─────────────────┘
```

### 4.2 Data Flow

**Writes** (e.g., create topic, register broker, update schema):
1. Caller invokes `RaftStore::put(key, value)` on any broker
2. If this broker is the Raft leader → proposes the entry to the Raft log
3. If this broker is a follower → forwards to the current leader (or returns redirect)
4. Raft replicates the log entry to a quorum of nodes
5. On commit, the leader applies the entry to `DanubeStateMachine`
6. State machine emits `WatchEvent` via broadcast channels
7. `LocalCache` receives the event and updates its `DashMap`

**Reads** (e.g., get topic policy, check subscription):
- **From LocalCache** (current behavior, unchanged): Fast local read from `DashMap`. Already eventual consistency — the cache is fed by watch events.
- **Direct from state machine** (for strong reads): Read from the local `BTreeMap` after a Raft `ensure_linearizable` check (leader confirms it's still leader). Used where ETCD's linearizable reads were critical.

**Watches** (e.g., LocalCache population, LoadManager, BrokerWatcher):
- On every state machine `apply()`, emit `WatchEvent::Put` or `WatchEvent::Delete` to `tokio::sync::broadcast` channels filtered by prefix.
- Identical semantics to current ETCD watches — same `WatchStream` type, same consumer code.

---

## 5. Raft State Machine Design

### 5.1 State Machine

```rust
use std::collections::BTreeMap;
use tokio::sync::broadcast;
use serde::{Serialize, Deserialize};

/// The core state machine replicated via Raft.
/// Replaces ETCD's KV store for all Danube metadata.
pub struct DanubeStateMachine {
    /// The replicated KV store. Keys are path strings (e.g., "/topics/default/my-topic/policy").
    kv: BTreeMap<String, VersionedValue>,

    /// Prefix-based watch channels. On every apply, matching watchers get notified.
    watchers: DashMap<String, broadcast::Sender<WatchEvent>>,

    /// TTL expiration queue: (expiry_instant, key). A background task on the leader
    /// periodically proposes "expire" commands for stale keys.
    ttl_entries: BTreeMap<u64, Vec<String>>,  // timestamp_ms → keys

    /// Monotonically increasing version per key (replaces ETCD's mod_revision).
    global_revision: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VersionedValue {
    pub value: serde_json::Value,
    pub version: i64,         // per-key version (like ETCD's version field)
    pub mod_revision: i64,    // global revision at last modification
    pub ttl_ms: Option<u64>,  // optional TTL (milliseconds from creation)
    pub created_at_ms: u64,   // creation timestamp
}
```

### 5.2 Raft Log Commands

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RaftCommand {
    /// Standard KV put
    Put { key: String, value: serde_json::Value },

    /// Put with TTL (replaces ETCD lease + put_with_lease)
    PutWithTTL { key: String, value: serde_json::Value, ttl_ms: u64 },

    /// Delete a key
    Delete { key: String },

    /// Delete keys matching a prefix (used for cleanup)
    DeletePrefix { prefix: String },

    /// Expire TTL keys (proposed periodically by the leader)
    ExpireTTLKeys { keys: Vec<String> },

    /// Compare-and-swap for atomic metadata transitions.
    CompareAndSwap {
        key: String,
        expected: Option<serde_json::Value>,  // None = key must not exist
        new_value: serde_json::Value,
        ttl_ms: Option<u64>,
    },

    /// Atomically increment and return a monotonic counter (e.g., schema IDs).
    AllocateMonotonicId { counter_key: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RaftResponse {
    Ok,
    Value(Option<VersionedValue>),
    CasFailed { current: Option<VersionedValue> },
    AllocatedId(u64),
}
```

### 5.3 Watch Implementation

```rust
impl DanubeStateMachine {
    /// Called by openraft after a log entry is committed and applied.
    fn apply_entry(&mut self, cmd: RaftCommand) -> RaftResponse {
        match cmd {
            RaftCommand::Put { key, value } => {
                self.global_revision += 1;
                let ver = self.kv.get(&key).map(|v| v.version + 1).unwrap_or(1);
                self.kv.insert(key.clone(), VersionedValue {
                    value: value.clone(),
                    version: ver,
                    mod_revision: self.global_revision,
                    ttl_ms: None,
                    created_at_ms: now_ms(),
                });
                self.notify_watchers(WatchEvent::Put {
                    key: key.into_bytes(),
                    value: serde_json::to_vec(&value).unwrap(),
                    mod_revision: Some(self.global_revision),
                    version: Some(ver),
                });
                RaftResponse::Ok
            }
            RaftCommand::Delete { key } => {
                self.global_revision += 1;
                self.kv.remove(&key);
                self.notify_watchers(WatchEvent::Delete {
                    key: key.into_bytes(),
                    mod_revision: Some(self.global_revision),
                    version: None,
                });
                RaftResponse::Ok
            }
            // ... PutWithTTL, CompareAndSwap, ExpireTTLKeys follow same pattern
            _ => todo!()
        }
    }

    fn notify_watchers(&self, event: WatchEvent) {
        let key_str = match &event {
            WatchEvent::Put { key, .. } => String::from_utf8_lossy(key).to_string(),
            WatchEvent::Delete { key, .. } => String::from_utf8_lossy(key).to_string(),
        };
        for entry in self.watchers.iter() {
            if key_str.starts_with(entry.key()) {
                let _ = entry.value().send(event.clone()); // ignore closed receivers
            }
        }
    }

    /// Register a prefix watch — returns a WatchStream compatible with current code.
    pub fn watch(&self, prefix: &str) -> WatchStream {
        let (tx, rx) = broadcast::channel(256);
        self.watchers.insert(prefix.to_string(), tx);
        WatchStream::from_broadcast(rx)
    }
}
```

Note: `WatchEvent` must derive `Clone` (currently doesn't — small change needed in `watch.rs`).

---

## 6. Replacing Each ETCD Pattern

### 6.1 P1: Broker Registration & Liveness

**Current** (`broker_register.rs`):
- Creates ETCD lease (TTL=32s)
- Puts broker info at `/cluster/register/{broker_id}` with lease
- Background task calls `keep_lease_alive()` every ~10s
- If broker dies → lease expires → key deleted → LoadManager detects via watch

**New**:
- Broker writes to `/cluster/register/{broker_id}` using `RaftCommand::PutWithTTL { ttl_ms: 32_000 }`
- Background task re-writes the key every ~10s (renews TTL, goes through Raft)
- Leader runs a TTL expiration loop: every 5s, scans `ttl_entries`, proposes `ExpireTTLKeys` for expired keys
- Expired key deletion → same `WatchEvent::Delete` → LoadManager detects failure

**Behavioral equivalence**: Identical to the current system. Broker failure → TTL expires → key deleted → watch event. The only difference is TTL expiration is proposed through Raft consensus (so all nodes agree on the deletion), vs ETCD where the server handles it internally.

**Code changes**:
- `broker_register.rs`: Remove `match MetadataStorage::Etcd(_)` block. Call `store.put_with_ttl(path, payload, Duration::from_secs(32))` instead of `create_lease` + `put_with_lease`. Background task calls `put_with_ttl` instead of `keep_lease_alive`.

### 6.2 P2: Leader Election → Eliminated

**Current** (`leader_election.rs`):
- Every 10s, checks `/cluster/leader` key
- If empty → `try_to_become_leader()`: creates ETCD lease, puts broker_id with lease
- Background keep-alive maintains the lease
- If leader dies → lease expires → key deleted → next broker's check triggers election

**New**:
- **Delete `leader_election.rs` entirely** (or reduce to a thin wrapper)
- The Raft leader IS the cluster leader. `openraft` handles leader election as part of the Raft protocol.
- Replace `LeaderElection::get_state()` with:
  ```rust
  pub fn is_leader(&self) -> bool {
      self.raft.current_leader() == Some(self.node_id)
  }
  ```
- `LoadManager` and other components that check `leader_election.get_state()` call this instead.

**Behavioral equivalence**: Better. Raft leader election is faster (heartbeat-based, ~1-2s failover) and formally proven. No risk of split-brain. No TTL tuning needed.

**Code changes**:
- Delete `LEADER_ELECTION_PATH` and all `/cluster/leader` writes.
- Update `resources/cluster.rs::get_cluster_leader()` and broker admin leader endpoints to query Raft runtime state (`current_leader`) rather than reading a metadata key.

### 6.3 P3: Metadata KV Store

**Current**: All `Resources` (cluster, namespace, topic, schema) use `MetadataStore::put/get/delete`.

**New**: Identical interface. `RaftStore` implements `MetadataStore`. Writes go through Raft consensus. Reads come from local state machine (same as current LocalCache behavior) or from `ensure_linearizable` for strong reads.

**Code changes**: Most files in `resources/` remain unchanged. Exceptions: cluster-leader lookup must read Raft runtime state, and schema ID allocation should use atomic Raft commands instead of read-modify-write.

### 6.4 P4: Watch/Event Bus

**Current**: `MetadataStore::watch(prefix)` returns a `WatchStream` driven by ETCD's watch API. Used by `LocalCache`, `LoadManager`, `BrokerWatcher`.

**New**: `RaftStore::watch(prefix)` returns a `WatchStream` driven by `tokio::sync::broadcast`. The state machine emits events on every `apply()`. The `WatchStream` type is unchanged — consumers don't know the difference.

**Critical detail**: In the current system, `WatchStream::from_etcd()` converts ETCD watch responses to `WatchEvent`. The new system uses `WatchStream::new()` (already exists) with a broadcast receiver stream. The `from_etcd()` method becomes dead code and is removed.

**Code changes**: `watch.rs` — add `Clone` derive to `WatchEvent`, add `WatchStream::from_broadcast()` constructor. Remove `from_etcd()` import of `etcd_client`.

**Watch correctness contract**:
- Broadcast lag (`RecvError::Lagged`) must be treated as recoverable desynchronization, not ignored.
- `LocalCache`, `LoadManager`, and `BrokerWatcher` should resync on lag: `get_bulk(prefix)` + recreate watch stream.
- Keep `mod_revision` monotonic and use it for observability/debugging during catch-up.

### 6.5 P5: Load Reporting & Coordination

**Current**: Brokers periodically write load reports to `/cluster/load/{broker_id}`. LoadManager watches this prefix.

**New**: Identical flow, but writes go through Raft and watches come from broadcast. No special handling needed.

### 6.6 P6: Persistent Storage Metadata (WAL)

**Current** (`etcd_metadata.rs`): WAL object descriptors and sealed state stored in ETCD via `MetadataStorage`.

**New**: Same `MetadataStorage` calls, but backed by `RaftStore`. No changes to `etcd_metadata.rs` except renaming it (it no longer has anything to do with ETCD).

---

## 7. Cluster Lifecycle — Robustness Guarantees

### 7.1 Cluster Bootstrap (First Start)

The broker configuration is **shared** (identical across all brokers). It contains no per-broker identity like `node_id` or peer lists. Cluster topology is managed exclusively through `danube-admin`.

**Node identity — auto-persisted**: On first boot, the broker generates a stable `node_id` (random u64) and writes it to `{data_dir}/node_id`. On subsequent boots it reads the file. This is the standard pattern used by CockroachDB, TiKV, and Consul.

```
/var/lib/danube/raft/
├── node_id              # e.g., "1" — written once on first boot, stable forever
├── raft_log.redb
├── raft_state.redb
└── snapshots/
```

**Bootstrap procedure (admin-driven, like CockroachDB `cockroach init`)**:

1. Start N brokers (typically 3 or 5) with the same config. Each broker:
   - Generates/reads its `node_id` from `{data_dir}/node_id`
   - Starts the Raft gRPC transport on the configured `raft` port
   - Enters a **"waiting for cluster"** state — accepts Raft RPCs but does not initiate elections
   - Starts the admin API normally
   - Logs: `"raft: waiting for cluster initialization..."`

2. Operator runs a single command:
   ```bash
   danube-admin cluster init \
     --nodes broker1:6680,broker2:6680,broker3:6680
   ```
   This contacts one broker's admin API, which:
   - Discovers each node's `node_id` by calling their Raft transport ports
   - Calls `openraft::Raft::initialize()` with the full membership set
   - All N become voters, Raft elects a leader within ~1-2s
   - Returns: `"Cluster initialized: leader=broker2 (node_id=3), 3 voters"`

   Safety guardrails:
   - `cluster init` must be idempotent.
   - If membership already exists, return `AlreadyInitialized` (no-op) instead of reinitializing.
   - Reserve a `--force-new-cluster` style recovery path for explicit disaster-recovery workflows only.

3. All subsequent metadata operations go through Raft consensus.

4. **Subsequent restarts require nothing** — membership is persisted in the Raft log. Brokers read their peers from persisted state and re-form the cluster automatically.

**Robustness**: Identical to ETCD cluster bootstrap. ETCD also requires a bootstrap step (initial cluster token or `etcdctl member add`). The one-time `cluster init` replaces what happens implicitly today when all brokers point at the same ETCD.

**K8s / Docker Compose automation**: The `cluster init` becomes an init-job:
```yaml
# docker-compose.yml (excerpt)
services:
  broker-1:
    image: danube-broker
    command: ["--config", "/etc/danube/broker.yml"]
  broker-2:
    image: danube-broker
    command: ["--config", "/etc/danube/broker.yml"]
  broker-3:
    image: danube-broker
    command: ["--config", "/etc/danube/broker.yml"]

  cluster-init:
    image: danube-admin
    depends_on: [broker-1, broker-2, broker-3]
    command: ["cluster", "init", "--nodes", "broker-1:6680,broker-2:6680,broker-3:6680"]
    restart: "on-failure"  # retry until all brokers are up
```
In Kubernetes, a one-shot `Job` or the StatefulSet's first pod runs `cluster init` if no Raft data exists.

### 7.2 Normal Operation

- **Writes**: Client → any broker → forwarded to Raft leader → replicated to quorum → applied to state machine → watch events emitted
- **Reads**: From LocalCache (eventual consistency, ~milliseconds behind) or from Raft leader with linearizable read guarantee
- **Heartbeat**: Raft leader sends heartbeats every ~500ms. Follower considers leader dead after ~2-3s of no heartbeats.

### 7.3 Broker Failure

**Scenario**: One broker in a 3-node cluster crashes.

1. **Raft detects** the failure within ~2-3s (missed heartbeats) — much faster than ETCD lease TTL (14-32s)
2. If the crashed broker was a **follower**: No impact on consensus. The remaining 2 of 3 nodes still form a quorum. Metadata operations continue normally.
3. If the crashed broker was the **leader**: Remaining nodes elect a new leader within ~1-2s. Brief write unavailability during election (~2-4s total).
4. **TTL-based registration** expires: The dead broker's `/cluster/register/{id}` key has a TTL. The new leader's expiration loop deletes it. `LoadManager` sees the `Delete` watch event and reassigns topics.

**Comparison to ETCD today**:
- ETCD: Broker dies → lease TTL (32s) expires → key deleted → LoadManager reacts. **Total: ~32-40s**.
- Raft: Broker dies → Raft detects (~2s) → new leader elected (~1s) → TTL expiration proposed → applied. **Total: ~5-10s**.

### 7.4 Full Cluster Restart

**Scenario**: All 3 brokers are stopped and restarted (e.g., rolling upgrade, power outage).

1. Each broker restarts and reads its **persisted Raft state** from `redb` on local disk:
   ```
   /var/lib/danube/raft/
   ├── raft_log.redb        # Committed Raft log entries
   ├── raft_state.redb      # Current term, voted_for, membership config
   └── snapshots/           # Periodic state machine snapshots
   ```
2. The Raft log is replayed to reconstruct the in-memory `DanubeStateMachine` (the `BTreeMap`)
3. Brokers discover each other using the persisted membership config (no need for external discovery)
4. Raft elects a new leader
5. Any log entries that were committed but not yet applied are applied (crash recovery)
6. **All metadata is fully recovered** — topics, namespaces, schemas, subscriptions, everything

**Robustness**: **Identical to ETCD**. ETCD does exactly the same thing — replays its own WAL (boltdb) on restart. The data durability guarantee is the same: committed = written to a quorum of disks.

**Snapshots**: To avoid unbounded log replay on restart, the state machine is periodically snapshotted. On restart, the broker loads the latest snapshot + replays only the log entries after the snapshot. This keeps restart time O(seconds), not O(total-history).

### 7.5 New Broker Joins the Cluster

**Scenario**: Scaling from 3 to 5 brokers.

1. Start the new broker with the **same shared config** (no special per-broker settings). It generates its own `node_id` on first boot.
2. Operator adds the node via admin CLI:
   ```
   danube-admin cluster add-node --addr broker4:6680
   ```
   The admin command contacts broker4's Raft port, discovers its `node_id`, then tells the Raft leader to add it.
3. Raft leader adds the node as a **learner** (non-voting member)
4. Leader sends the new node a **snapshot** of the current state machine (all metadata as a binary blob)
5. Leader then streams all new log entries to the new node in real-time
6. Once the node is caught up, operator promotes it to **voter**:
   ```
   danube-admin cluster promote-node --addr broker4:6680
   ```
7. The node is now a full voting member of the Raft group

**Comparison to ETCD today**: Simpler from the broker's perspective. Currently, a new broker just connects to ETCD and writes its registration key — but ETCD itself had to be pre-scaled separately (adding ETCD members is its own procedure). With embedded Raft, scaling the metadata cluster and scaling the broker cluster is the **same operation**.

### 7.6 Broker Removed from Cluster

**Scenario**: Scaling from 5 to 3 brokers (decommission).

1. Operator drains the broker:
   ```
   danube-admin broker drain --addr broker5:6680
   ```
2. LoadManager reassigns all topics from the broker to others (existing flow, unchanged)
3. Operator removes the node from Raft membership:
   ```
   danube-admin cluster remove-node --addr broker5:6680
   ```
4. Raft leader proposes a membership change, removing the node from the voter set
5. The node can be shut down. Its data directory can be deleted.

### 7.7 Network Partition

**Scenario**: Network splits a 5-node cluster into 3+2.

- The **majority partition** (3 nodes) continues operating normally — it has quorum
- The **minority partition** (2 nodes) cannot commit writes — it correctly refuses operations
- When the partition heals, the minority nodes catch up from the leader's log
- **No split-brain, no data loss, no inconsistency** — this is the fundamental guarantee of Raft consensus

**Identical to ETCD** (which is also Raft-based internally).

---

## 8. Persistence Details

### 8.1 What Is Persisted

| Data | Storage | Purpose |
|------|---------|---------|
| Raft log entries | `raft_log.redb` | Ordered sequence of all committed commands |
| Raft hard state (term, vote) | `raft_state.redb` | Election state; survives restarts |
| State machine snapshot | `snapshots/` directory | Compressed `BTreeMap` serialization |
| Membership configuration | Inside Raft log (special entries) | Which nodes are voters/learners |

### 8.2 Disk Layout

```
/var/lib/danube/raft/
├── node_id                  # Auto-generated on first boot, stable across restarts
├── raft_log.redb            # ~10-100 MB (compacted after snapshot)
├── raft_state.redb          # <1 KB (just term + voted_for)
└── snapshots/
    ├── snap-00000100.bin    # Snapshot at log index 100
    └── snap-00000500.bin    # Snapshot at log index 500 (older snapshots pruned)
```

### 8.3 Durability Guarantee

- Every Raft log entry is `fsync`'d to `redb` before being acknowledged
- A write is committed only after a **quorum** of nodes have persisted it
- This means: if you get an `Ok` response, the data is on `⌈N/2⌉+1` disks
- Single-disk failure = no data loss (other copies exist on quorum nodes)
- This is **exactly** the same guarantee ETCD provides (it uses the same Raft algorithm)

### 8.4 Snapshot Policy

- Snapshot triggered when log grows beyond a configurable threshold (e.g., 10,000 entries)
- Snapshot = serialize the `BTreeMap<String, VersionedValue>` to a compact binary format (e.g., bincode or MessagePack)
- After snapshot, old log entries before the snapshot index are purged
- Keeps disk usage bounded regardless of cluster uptime

---

## 9. New Crate: `danube-raft`

To keep the architecture clean, the Raft consensus logic lives in a dedicated crate:

```
danube-raft/
├── Cargo.toml
└── src/
    ├── lib.rs                 # Public API: RaftStore, init functions
    ├── state_machine.rs       # DanubeStateMachine (BTreeMap + watchers + TTL)
    ├── log_store.rs           # openraft LogStore impl backed by redb
    ├── network.rs             # openraft RaftNetwork impl (gRPC transport)
    ├── config.rs              # RaftConfig (data_dir, tuning params; node_id auto-read from disk)
    ├── commands.rs            # RaftCommand, RaftResponse enums
    └── ttl.rs                 # TTL expiration background task
```

**Dependencies**:
```toml
[dependencies]
openraft = { version = "0.10", features = ["serde"] }
redb = "2"
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
bincode = "1"           # for snapshot serialization
tonic = { workspace = true }  # for Raft gRPC transport
dashmap = { workspace = true }
tracing = { workspace = true }
```

### 9.1 RaftStore (MetadataStore Implementation)

```rust
/// The main entry point. Implements MetadataStore so it can replace EtcdStore.
pub struct RaftStore {
    raft: openraft::Raft<DanubeTypeConfig>,
    state_machine: Arc<DanubeStateMachine>,
    node_id: u64,
}

#[async_trait]
impl MetadataStore for RaftStore {
    async fn get(&self, key: &str, _opts: MetaOptions) -> Result<Option<Value>> {
        // Read directly from the local state machine
        // For linearizable reads, call self.raft.ensure_linearizable() first
        Ok(self.state_machine.get(key).map(|v| v.value.clone()))
    }

    async fn get_childrens(&self, prefix: &str) -> Result<Vec<String>> {
        // BTreeMap range scan for prefix
        Ok(self.state_machine.get_children(prefix))
    }

    async fn put(&self, key: &str, value: Value, _opts: MetaOptions) -> Result<()> {
        // Propose through Raft consensus
        let cmd = RaftCommand::Put { key: key.to_string(), value };
        self.raft.client_write(cmd).await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let cmd = RaftCommand::Delete { key: key.to_string() };
        self.raft.client_write(cmd).await?;
        Ok(())
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        Ok(self.state_machine.watch(prefix))
    }
}
```

### 9.2 Raft gRPC Transport

Add a small gRPC service for Raft inter-node communication. This runs on `broker.ports.raft` (e.g., 6680) alongside the client port (6650) and admin port (50051):

```protobuf
// raft_transport.proto
service RaftTransport {
    rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
    rpc InstallSnapshot(stream InstallSnapshotChunk) returns (InstallSnapshotResponse);
    rpc Vote(VoteRequest) returns (VoteResponse);
    rpc ForwardWrite(ForwardWriteRequest) returns (ForwardWriteResponse);
}
```

This is internal-only (broker-to-broker). No client ever calls it.

---

## 10. Migration Plan

### Phase 0: Clean the Abstraction Layer [1-2 weeks]

**Goal**: Make `MetadataStore` truly backend-agnostic. No ETCD types leak outside `providers/etcd.rs`.

1. **Replace `MetaOptions`**:
   ```rust
   // Before (store.rs)
   pub enum MetaOptions {
       None,
       EtcdGet(GetOptions),   // etcd_client type!
       EtcdPut(PutOptions),   // etcd_client type!
   }

   // After
   pub enum MetaOptions {
       None,
       WithPrefix,
       WithPrevKey,
   }
   ```
   Update all callers in `resources/` to use the new variants. `EtcdStore` internally maps `WithPrefix` → `GetOptions::new().with_prefix()`.

2. **Add `put_with_ttl` and `get_bulk` to the `MetadataStore` trait**:
   ```rust
   async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<()>;
   async fn get_bulk(&self, prefix: &str) -> Result<Vec<KeyValueVersion>>;
   ```
   Currently `get_bulk` is only on `MetadataStorage` impl, and lease methods are ETCD-specific. Replace lease methods with `put_with_ttl` on the trait. The ETCD-specific `create_lease`/`keep_lease_alive`/`put_with_lease` methods are deleted in Phase 2.

3. **Refactor `broker_register.rs` and `leader_election.rs`** to call trait methods (`put_with_ttl`) instead of `create_lease` / `put_with_lease` / `keep_lease_alive`. The `EtcdStore` implements `put_with_ttl` by wrapping its existing lease logic internally. This keeps things compiling until Phase 2 deletes ETCD entirely.

4. **Make `WatchEvent` cloneable**: Add `#[derive(Clone)]` to `WatchEvent` in `watch.rs`.

5. **Implement `watch()` for `MemoryStore`**: Using `tokio::sync::broadcast`. This also unblocks testing without ETCD.

6. **Add watch lag recovery behavior**:
   - `WatchStream::from_broadcast()` surfaces lag conditions as watch errors.
   - `local_cache.rs`, `load_manager.rs`, and `broker_watcher.rs` recover by reloading current state from `get_bulk()` and resubscribing.

**Tests**: Existing integration tests should still pass (ETCD backend behavior is unchanged, just the abstraction is cleaner).

### Phase 1: Implement `danube-raft` Crate [3-4 weeks]

**Goal**: A working Raft KV store that implements `MetadataStore`.

Week 1-2:
- Scaffold `danube-raft` crate
- Implement `DanubeStateMachine` (BTreeMap + apply + watch channels)
- Implement `RedbLogStore` (openraft `LogStore` trait backed by redb)
- Implement snapshot save/load (bincode serialization of BTreeMap)

Week 2-3:
- Implement gRPC Raft transport (`raft_transport.proto` + tonic service)
- Implement `RaftStore` (MetadataStore trait wrapper around openraft::Raft)
- Implement TTL expiration background task

Week 3-4:
- Implement `put_with_ttl` flow (PutWithTTL command + expiration queue)
- Implement read forwarding (follower → leader) or `ensure_linearizable`
- Implement `AllocateMonotonicId` command and wire schema ID generation to it (remove read-modify-write race)
- Unit tests: single-node Raft, 3-node Raft, failover, snapshot recovery

### Phase 2: Integrate into Danube Broker & Delete ETCD [2-3 weeks]

**Goal**: Replace ETCD with Raft in the broker and **delete all ETCD code** in one phase. There is no transition mode — ETCD is fully removed.

1. **Replace `MetadataStorage` enum** — remove the `Etcd` variant entirely:
   ```rust
   pub enum MetadataStorage {
       Raft(RaftStore),       // production
       InMemory(MemoryStore), // testing
   }
   ```
   Implement all `MetadataStore` trait methods for the `Raft` variant (delegating to `RaftStore`).

2. **Update `main.rs`** — Raft is the only backend:
   ```rust
   // node_id is auto-read from {data_dir}/node_id (generated on first boot)
   // Raft transport binds to {broker.host}:{broker.ports.raft}
   let raft_store = RaftStore::new(&service_config.meta_store, raft_addr).await?;
   let metadata_store = MetadataStorage::Raft(raft_store);
   ```

3. **Update configuration** (shared across all brokers — no per-broker fields):
   ```yaml
   # Before
   meta_store:
     host: "127.0.0.1"
     port: 2379

   # After
   meta_store:
     data_dir: "/var/lib/danube/raft"
   ```
   The `host`/`port` fields for ETCD are gone. The Raft transport port is in `broker.ports.raft`. No `backend` field needed — Raft is the only option.

4. **Delete `leader_election.rs`**: The Raft leader IS the cluster leader. Replace all `leader_election.get_state()` calls with:
   ```rust
   pub fn is_leader(&self) -> bool {
       self.raft.current_leader() == Some(self.node_id)
   }
   ```

5. **Update cluster/admin leader lookup**:
   - Remove `LEADER_ELECTION_PATH` usage.
   - `resources/cluster.rs::get_cluster_leader()` reads from Raft runtime status.
   - Admin broker leader endpoints use the same Raft status path.

6. **Rewrite `broker_register.rs`**: Use `put_with_ttl()` instead of ETCD leases. Background task calls `put_with_ttl()` periodically to renew. Delete all `create_lease` / `put_with_lease` / `keep_lease_alive` code paths.

7. **Fix schema ID allocation path**: Replace current read-modify-write ID generation with atomic Raft counter command (`AllocateMonotonicId`).

8. **Start Raft transport gRPC server**: In `DanubeService::start()`, start the Raft transport server before any metadata operations.

9. **Update `danube-persistent-storage`**: Rename `etcd_metadata.rs` to `storage_metadata.rs`. No logic changes — it calls `MetadataStorage` methods, which now route to Raft.

10. **Delete all ETCD code**:
   - Delete `danube-metadata-store/src/providers/etcd.rs`
   - Remove `etcd-client` from `danube-metadata-store/Cargo.toml`
   - Remove `WatchStream::from_etcd()` from `watch.rs`
   - Remove ETCD-specific error variants from `errors.rs`
   - Remove `pub use etcd_client::GetOptions as EtcdGetOptions` from `lib.rs`
   - Remove `MetaStoreConfig` (etcd host/port) from `service_configuration.rs`
   - Delete ETCD-specific match arms from `MetadataStorage` impl (lease methods, etc.)
   - Update all imports and any remaining ETCD references
   - Update Docker/K8s deployment manifests to remove ETCD sidecar/service

### Phase 3: Hardening & Testing [2-3 weeks]

1. **Integration tests** (ETCD is gone — all tests run against the Raft backend):
   - 3-node cluster: create topics, produce/consume, verify metadata consistency
   - Broker failure: kill one broker, verify topics reassigned, restart broker, verify rejoin
   - Leader failure: kill leader, verify new leader elected within ~3s, verify no data loss
   - Full restart: stop all brokers, restart all, verify all metadata recovered
   - Network partition simulation (using iptables/tc): verify majority partition works, minority blocks
   - Watch lag recovery: force slow consumer lag, verify automatic resync from `get_bulk()`
   - Schema ID race test: concurrent schema registrations produce unique monotonic IDs
   - `cluster init` idempotency: repeated init returns `AlreadyInitialized` and does not mutate membership

2. **Benchmarks**:
   - Metadata operations/sec (put, get, watch event latency)
   - Compare against ETCD baseline
   - Expected: get (local read) faster, put (Raft consensus) comparable, watch event delivery faster (in-process vs network)

3. **Stress tests**:
   - 100+ topics, rapid create/delete cycles
   - Schema registry rapid registration
   - Concurrent broker registration/deregistration

4. **Operational tooling**:
   - `danube-admin cluster status` — show Raft state (leader, term, log index, members)
   - `danube-admin cluster add-node` / `remove-node` — membership changes
   - `danube-admin cluster snapshot` — trigger manual snapshot

---

## 11. Configuration Reference

This configuration is **identical across all brokers** — no per-broker fields:

```yaml
# Full example: danube-broker.yaml (after migration)
cluster_name: "production"

broker:
  host: "0.0.0.0"
  ports:
    client: 6650
    admin: 50051
    prometheus: 9040
    raft: 6680                             # NEW — Raft inter-broker transport

meta_store:
  data_dir: "/var/lib/danube/raft"         # Raft log, snapshots, and auto-generated node_id
  # Tuning (optional, sane defaults)
  heartbeat_interval_ms: 500               # default: 500ms
  election_timeout_ms: 3000                # default: 3000ms (3s)
  snapshot_threshold: 10000                # entries before snapshot
  max_payload_entries: 300                 # max entries per AppendEntries RPC

bootstrap_namespaces: []
auto_create_topics: true

policies:
  max_producers: 0
  max_subscriptions: 0
  max_consumers: 0
  max_message_size: 0
  retention_time_minutes: 0
  retention_size_mb: 0

wal_cloud:
  wal:
    dir: "/var/lib/danube/wal"
  uploader:
    interval_seconds: 60
  cloud:
    backend: s3
    root: "danube-production"
  metadata:
    # No more etcd_endpoint here — metadata goes through the same Raft store
    in_memory: false

auth:
  mode: "none"

load_manager:
  assignment_strategy: "round_robin"
  load_report_interval_seconds: 30
  rebalancing:
    enabled: true
    check_interval_seconds: 120
    aggressiveness: "moderate"
    max_moves_per_hour: 10
```

**Cluster management is done entirely through `danube-admin`**:
```bash
# First-time bootstrap (once per cluster lifetime)
danube-admin cluster init --nodes broker1:6680,broker2:6680,broker3:6680

# Status
danube-admin cluster status

# Scale out
danube-admin cluster add-node --addr broker4:6680
danube-admin cluster promote-node --addr broker4:6680

# Scale in
danube-admin broker drain --addr broker5:6680
danube-admin cluster remove-node --addr broker5:6680

# Manual snapshot
danube-admin cluster snapshot
```

---

## 12. Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| openraft bugs in edge cases | High | Extensive integration tests; openraft is used in production by Databend, CnosDB. Pin to a stable release. |
| redb data corruption | Medium | redb uses ACID transactions with fsync. Corruption = hardware failure, mitigated by Raft replication across nodes. |
| Performance regression on metadata writes | Medium | Raft consensus adds ~1-2ms vs direct ETCD write. Benchmark and optimize. LocalCache reads (majority of operations) are unaffected. |
| Complex cluster bootstrapping | Medium | One-time `danube-admin cluster init` command. In K8s/Compose, an init-job automates this. Subsequent restarts need no action. |
| TTL expiration lag | Low | Expiration is leader-proposed, runs every ~5s. Worst case: key lives 5s past TTL. Same order as ETCD (which checks leases periodically too). |
| Snapshot size for large clusters | Low | Metadata is small (even 10K topics = ~10MB of BTreeMap). Snapshots are fast. |
| Migration disruption | Low (current non-prod usage) | This is a **breaking change** — ETCD is fully removed, not kept as fallback. A live metadata migration path is explicitly out of scope for now. |

---

## 13. What Mostly Doesn't Change

Most components continue to program to `MetadataStore` and `WatchStream`, but a few targeted updates are required:

- `resources/namespace.rs`, `topic.rs` — mostly unchanged
- `resources/cluster.rs` — update leader lookup to Raft runtime state (remove `/cluster/leader` dependency)
- `resources/schema.rs` — switch schema ID allocation to atomic Raft counter command
- `local_cache.rs`, `load_manager.rs`, `broker_watcher.rs` — logic mostly unchanged, but add watch-lag resync path
- `broker_service.rs` — no major logic changes
- `broker_server/` — no major logic changes
- `admin/` — mostly unchanged, plus Raft status/cluster management commands
- `danube-client` — no changes (doesn't know about metadata layer)
- `danube-cli`, `danube-admin` — minimal changes (new cluster management commands)

---

## 14. Timeline Summary

```
Phase 0: Clean abstraction layer                    [1-2 weeks]
Phase 1: Implement danube-raft crate                [3-4 weeks]
Phase 2: Integrate into broker & delete ETCD        [2-3 weeks]
Phase 3: Hardening, testing, operational tooling     [2-3 weeks]
                                                    ─────────────
Total:                                               8-12 weeks
```

---

## 15. Summary

| Aspect | Current (ETCD) | After (Embedded Raft) |
|--------|---------------|----------------------|
| External deps | ETCD cluster (3-5 nodes) | **None** |
| Deployment | Broker + ETCD | **Broker only** |
| Consistency | Linearizable (ETCD Raft) | Linearizable (openraft) — **same algorithm** |
| Fault tolerance | Survives ⌊N/2⌋ ETCD failures | Survives ⌊N/2⌋ broker failures — **identical** |
| Failure detection | Lease TTL (14-32s) | Raft heartbeat (~2-3s) — **faster** |
| Leader election | ETCD lease CAS + 10s polling | Raft leader = cluster leader — **built-in** |
| Watch events | ETCD Watch API (network) | In-process broadcast — **faster, simpler** |
| Metadata persistence | ETCD boltdb + WAL | redb + Raft log — **same durability** |
| Cluster restart | ETCD replays its WAL | Broker replays Raft log — **same mechanism** |
| New broker join | Connect to ETCD + write key | `danube-admin cluster add-node` + Raft snapshot transfer — **same cluster** |
| Operational burden | Manage separate ETCD cluster | **Zero** — brokers are the cluster |
| Code complexity | ETCD types leak into broker | Clean trait-based abstraction |

**The Raft consensus algorithm is identical.** ETCD is a Raft implementation; openraft is a Raft implementation. The theoretical guarantees (consistency, fault tolerance, partition tolerance) are mathematically the same. The practical benefit is eliminating an entire external system from the deployment.
