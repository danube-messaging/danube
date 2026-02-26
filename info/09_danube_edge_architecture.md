# Danube Edge: Architecture & Replicator Design

## 1. Vision

**Danube Edge** (a.k.a. "Danube Lite") is a single-binary, standalone broker designed for constrained environments — factory floors, vehicles, retail stores, IoT gateways — that seamlessly bridges to a central **Danube Cloud** cluster.

```
   Edge Sites                                   Central Cloud
   ─────────                                    ─────────────
┌──────────────┐                           ┌──────────────────┐
│ Factory Floor │                           │                  │
│  Danube Edge  │───── WAN (cellular/VPN)──▶│  Danube Cloud    │
│  + Sensors    │                           │  (3-5 brokers,   │
└──────────────┘                           │   openraft)       │
                                            │                  │
┌──────────────┐                           │  ┌────────────┐  │
│ Vehicle Fleet │                           │  │ Consumers  │  │
│  Danube Edge  │───── Satellite/LTE ──────▶│  │ (analytics,│  │
│  + CAN bus    │                           │  │  dashboards│  │
└──────────────┘                           │  └────────────┘  │
                                            │                  │
┌──────────────┐                           │  ┌────────────┐  │
│ Retail Store  │                           │  │ Cloud WAL  │  │
│  Danube Edge  │───── Internet ───────────▶│  │ + Object   │  │
│  + POS data   │                           │  │   Storage  │  │
└──────────────┘                           └──────────────────┘
```

Key properties:
- **Single static binary** (~15-30 MB, no JVM, no ETCD, no external deps)
- **Embedded metadata store** (redb — pure Rust, ACID, tiny)
- **Local WAL** for durable message storage
- **Offline-capable**: Continues accepting messages when WAN is down
- **Replicator**: Asynchronously forwards messages to the central cloud cluster
- **Runs on**: Raspberry Pi 4 (1GB RAM), ARM gateways, industrial PCs, edge K3s nodes

**This effort is independent from the ETCD → openraft migration (see `08_danube_cloud_openraft_migration.md`).** The cloud cluster must work with openraft first. Edge can be built afterward, reusing the embedded store work.

---

## 2. Architecture

### 2.1 Danube Edge Broker

```
┌─────────────────────────────────────────────────────────────┐
│                     Danube Edge Broker                        │
│                                                              │
│  ┌─────────────┐     ┌──────────────────────────────────┐   │
│  │ gRPC Server  │     │ EmbeddedStore (redb)             │   │
│  │ (clients)    │     │  ├── KV metadata (topics, etc.)  │   │
│  └──────┬──────┘     │  ├── WAL metadata (segments)      │   │
│         │            │  ├── Replicator checkpoints        │   │
│         ▼            │  └── Watch (broadcast channels)    │   │
│  ┌─────────────┐     └──────────────┬───────────────────┘   │
│  │ LocalCache   │◄── watch events ──┘                        │
│  │ (DashMap)    │                                            │
│  └──────┬──────┘                                            │
│         │                                                    │
│  ┌──────┴──────┐     ┌──────────────────────────────────┐   │
│  │ BrokerService│     │ WAL (per-topic)                  │   │
│  │ (produce/    │────▶│  ├── Local disk segments          │   │
│  │  consume)    │     │  └── Cloud uploader (OpenDAL)     │   │
│  └─────────────┘     └──────────────┬───────────────────┘   │
│                                      │                       │
│  ┌──────────────────────────────────┴───────────────────┐   │
│  │ Replicator                                            │   │
│  │  ├── Reads committed WAL entries                      │   │
│  │  ├── Batches + compresses (zstd)                      │   │
│  │  ├── Produces to central cloud cluster                │   │
│  │  ├── Tracks checkpoint in EmbeddedStore               │   │
│  │  └── Handles WAN disconnection (retry + backoff)      │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  [Disabled: LeaderElection, LoadManager, BrokerRegister,     │
│   BrokerWatcher, LoadReport]                                 │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 What Runs vs What's Disabled

| Component | Cloud Mode | Edge Mode | Notes |
|-----------|-----------|-----------|-------|
| gRPC server (broker) | ✅ | ✅ | Clients still connect normally |
| gRPC server (admin) | ✅ | ✅ | Admin API still works |
| LocalCache | ✅ | ✅ | Needed for read performance |
| Cluster metadata setup | ✅ | ✅ (local) | Namespaces, topics created locally |
| WAL + Cloud uploader | ✅ | ✅ | Messages persisted to local WAL + S3 |
| Broker Registration | ✅ | ❌ Skip | No cluster to register with |
| Leader Election | ✅ | ❌ Skip | Always leader (single broker) |
| Load Manager | ✅ | ❌ Skip | No peers to balance across |
| Load Report | ✅ | ❌ Skip | No one to report to |
| Broker Watcher | ✅ | ❌ Skip | No assignments from a leader |
| Syncronizer | ❌ Unused | ❌ Unused | — |
| **Replicator** | ❌ N/A | ✅ **New** | Edge-only, forwards to cloud |

### 2.3 Standalone Mode Toggle

In `DanubeService::start()`, a `standalone: true` flag conditionally skips cluster services:

```rust
if !self.service_config.standalone {
    // Cloud mode: full cluster services
    register_broker(...).await?;
    leader_election.start(...);
    load_manager.bootstrap(...);
    load_manager.start(...);
    post_broker_load_report(...);
    broker_watcher::watch_events_for_broker(...);
} else {
    // Edge mode: self-contained
    info!("running in standalone mode — cluster services disabled");
    // Topics are self-assigned (no LoadManager needed)
    // LeaderElection::is_leader() always returns true
    // Replicator started instead (if configured)
    if let Some(replicator_cfg) = &self.service_config.replicator {
        replicator::start(replicator_cfg, ...).await;
    }
}
```

---

## 3. Embedded Metadata Store

### 3.1 Why Not Reuse the Raft Store from Cloud Mode?

The cloud cluster uses `openraft` for replicated consensus across N brokers. For a single edge broker, Raft is overkill — there's no one to replicate to. A simple embedded KV store (redb) with broadcast-based watches is sufficient and much simpler.

However, the **`MetadataStore` trait interface is identical**. The edge `EmbeddedStore` and the cloud `RaftStore` both implement the same trait. Code in `resources/`, `local_cache.rs`, `broker_service.rs` etc. doesn't know or care which backend is active.

### 3.2 EmbeddedStore Design

```rust
pub struct EmbeddedStore {
    db: redb::Database,
    watchers: Arc<DashMap<String, broadcast::Sender<WatchEvent>>>,
}

#[async_trait]
impl MetadataStore for EmbeddedStore {
    async fn get(&self, key: &str, _opts: MetaOptions) -> Result<Option<Value>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(METADATA_TABLE)?;
        Ok(table.get(key)?.map(|v| serde_json::from_slice(v.value()).unwrap()))
    }

    async fn get_childrens(&self, prefix: &str) -> Result<Vec<String>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(METADATA_TABLE)?;
        let range = table.range(prefix..)?;
        Ok(range
            .take_while(|r| r.as_ref().map(|(k, _)| k.value().starts_with(prefix)).unwrap_or(false))
            .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
            .collect())
    }

    async fn put(&self, key: &str, value: Value, _opts: MetaOptions) -> Result<()> {
        let bytes = serde_json::to_vec(&value)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(METADATA_TABLE)?;
            table.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;

        // Notify prefix watchers
        self.notify_watchers(WatchEvent::Put {
            key: key.as_bytes().to_vec(),
            value: bytes,
            mod_revision: None,
            version: None,
        });
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(METADATA_TABLE)?;
            table.remove(key)?;
        }
        txn.commit()?;

        self.notify_watchers(WatchEvent::Delete {
            key: key.as_bytes().to_vec(),
            mod_revision: None,
            version: None,
        });
        Ok(())
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        let (tx, rx) = broadcast::channel(256);
        self.watchers.insert(prefix.to_string(), tx);
        Ok(WatchStream::from_broadcast(rx))
    }
}
```

### 3.3 Durability

- **ACID transactions with fsync**: redb writes are crash-safe. Power loss during a write → transaction is rolled back, no corruption.
- **Single-disk**: No replication. If the disk dies, metadata is lost. But:
  - WAL segments have already been uploaded to cloud storage (S3/GCS/Azure) via the existing uploader
  - Replicator has forwarded messages to the central cluster
  - The edge broker can be re-provisioned from a config file; metadata is reconstructed as topics are recreated

---

## 4. The Replicator Service

### 4.1 Purpose

The Replicator bridges the edge broker to the central cloud cluster. It reads committed messages from the local WAL and forwards them to the cloud, maintaining durable checkpoints so it never re-sends or loses data.

### 4.2 Data Flow

```
Local WAL                    Replicator                     Cloud Cluster
─────────                    ──────────                     ─────────────

┌────────────┐              ┌────────────────┐            ┌──────────────┐
│ WAL Segment │              │ Read committed  │            │ Cloud Broker │
│  (sealed)   │──────────▶  │ WAL entries     │            │  (topic:     │
│             │              │                 │  produce   │  /global/    │
│ WAL Segment │              │ Batch + compress│───────────▶│   telemetry) │
│  (active)   │              │ (zstd)          │            │              │
└────────────┘              │                 │            └──────────────┘
                             │ Update          │
                             │ checkpoint      │
                             │ (embedded store)│
                             └────────────────┘
                                    │
                                    ▼
                             ┌────────────────┐
                             │ Checkpoint:     │
                             │ topic=telemetry │
                             │ offset=42871    │
                             │ (in redb)       │
                             └────────────────┘
```

### 4.3 Design

```rust
pub struct Replicator {
    /// Embedded store for checkpoint persistence
    checkpoint_store: EmbeddedStore,
    /// Danube client connected to the central cloud cluster
    cloud_client: DanubeClient,
    /// Per-topic replication state
    topics: HashMap<String, TopicReplicator>,
    /// Configuration
    config: ReplicatorConfig,
}

pub struct TopicReplicator {
    /// Local topic name
    local_topic: String,
    /// Remote topic name (may differ via namespace mapping)
    remote_topic: String,
    /// Last successfully acked offset
    checkpoint_offset: u64,
    /// Danube Producer to cloud cluster
    producer: Producer,
}

#[derive(Deserialize)]
pub struct ReplicatorConfig {
    /// Cloud cluster address
    target_cluster: String,
    /// Batch size (number of messages per batch)
    batch_size: usize,
    /// Compression algorithm
    compression: Compression,  // zstd, lz4, none
    /// Retry interval when WAN is down
    retry_interval_seconds: u64,
    /// Topic mapping: local namespace → cloud namespace
    topic_mapping: HashMap<String, String>,
    /// Which topics to replicate (glob patterns)
    include_topics: Vec<String>,
    /// Topics to exclude from replication
    exclude_topics: Vec<String>,
}
```

### 4.4 Replication Protocol

The Replicator uses the **Danube Producer protocol** — it acts as a normal Danube client producing to the cloud cluster. This means:

- **Zero new protocol code** — reuses existing gRPC produce API
- **Authentication**: Uses the same TLS/JWT auth as any other client
- **Backpressure**: Respects cloud cluster's rate limiting and flow control
- **Schema propagation**: If the edge topic has a schema, the Replicator registers it on the cloud side too

### 4.5 Checkpoint Tracking

Checkpoints are stored in the embedded metadata store:

```
Key:   /replicator/checkpoints/{topic_name}
Value: { "offset": 42871, "last_acked_at": "2025-01-15T10:30:00Z" }
```

On startup, the Replicator reads its checkpoints and resumes from the last acked offset. This provides **at-least-once delivery** semantics:
- If the Replicator crashes after sending but before checkpointing → some messages may be re-sent on restart
- The cloud consumer should be idempotent or use deduplication

### 4.6 Offline Tolerance

When WAN is unavailable:
1. The Replicator detects connection failure (gRPC error / timeout)
2. It enters a **retry loop** with exponential backoff (starting at `retry_interval_seconds`, capped at 5 minutes)
3. Local producers continue publishing to the edge broker normally — WAL accumulates on disk
4. When WAN reconnects, the Replicator resumes from its checkpoint
5. It works through the backlog at maximum throughput until caught up

**Edge storage sizing**: The local WAL + cloud uploader handle retention. If WAL retention is set to 24 hours and the link is down for 12 hours, the WAL holds 12 hours of data locally. The cloud uploader (if configured) independently pushes sealed WAL segments to object storage, providing a second path to the cloud.

### 4.7 Topic Namespace Mapping

Edge topics often have site-specific names that should be mapped to global namespaces in the cloud:

```yaml
replicator:
  topic_mapping:
    "default": "edge-site-001"         # /default/telemetry → /edge-site-001/telemetry
    "local": "edge-site-001/local"     # /local/alerts → /edge-site-001/local/alerts
```

The Replicator prepends/replaces the namespace when producing to the cloud, so the cloud cluster can aggregate data from many edge sites into a unified namespace hierarchy.

---

## 5. Edge Configuration

```yaml
# danube-edge.yaml
cluster_name: "edge-site-001"

broker:
  host: "0.0.0.0"
  ports:
    client: 6650
    admin: 6660

# Edge mode: embedded store, no ETCD, no Raft
meta_store:
  backend: "embedded"
  data_dir: "/var/lib/danube/meta"

# This is the key switch
standalone: true

# WAL + Cloud storage (works same as cloud mode)
wal_cloud:
  wal:
    dir: "/var/lib/danube/wal"
    rotation:
      max_bytes: 67108864    # 64 MB segments
      max_hours: 1
    retention:
      time_minutes: 1440     # 24 hours local retention
      size_mb: 1024          # 1 GB max local storage
  uploader:
    interval_seconds: 60
    root_prefix: "/edge-site-001"
  cloud:
    backend: s3
    root: "danube-edges"
    region: "us-east-1"
  metadata:
    in_memory: false

# Replicator: forward messages to central cluster
replicator:
  target_cluster: "https://danube-cloud.example.com:6650"
  batch_size: 500
  compression: "zstd"
  retry_interval_seconds: 5
  include_topics:
    - "default/*"            # replicate all topics in default namespace
    - "sensors/*"
  exclude_topics:
    - "local/*"              # keep local-only topics on the edge
  topic_mapping:
    "default": "edge-site-001"
    "sensors": "edge-site-001/sensors"

# Minimal policies for constrained hardware
policies:
  max_producers: 100
  max_subscriptions: 50
  max_consumers: 50
  max_message_size: 1048576  # 1 MB

# Auth (optional — may use mTLS for WAN, none for local)
auth:
  mode: "none"

bootstrap_namespaces:
  - "sensors"
  - "local"
```

---

## 6. Hardware Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 1 core ARM (Cortex-A53) | 2+ cores ARM (Cortex-A72) |
| RAM | 256 MB | 512 MB - 1 GB |
| Disk | 1 GB (metadata only) | 4-16 GB (with WAL retention) |
| Network | Intermittent WAN | Stable WAN for real-time replication |
| OS | Linux (aarch64 or x86_64) | Linux with systemd for service management |

**Target devices**:
- Raspberry Pi 4/5 (1-8 GB RAM)
- NVIDIA Jetson Nano / Orin
- Industrial gateways (Advantech, Siemens IOT2050)
- Any ARM64/x86_64 Linux device

**Binary size target**: < 20 MB static binary (musl-linked, stripped, with LTO).

---

## 7. Build & Cross-Compile

```bash
# Native build (x86_64)
cargo build --release --bin danube-broker --features embedded

# ARM64 cross-compile (Raspberry Pi, etc.)
cross build --release --target aarch64-unknown-linux-musl --bin danube-broker --features embedded

# Minimal binary (strip + LTO)
# In Cargo.toml profile:
# [profile.release]
# lto = true
# strip = true
# codegen-units = 1
```

The `embedded` feature flag enables the `EmbeddedStore` provider and optionally disables the ETCD/Raft provider to minimize binary size:

```toml
# danube-metadata-store/Cargo.toml
[features]
default = ["raft"]
raft = ["danube-raft"]
embedded = ["redb"]
# No "etcd" feature after the cloud migration is complete
```

---

## 8. Relationship to Cloud Migration

```
                           Dependency Graph
                           ─────────────────

  ┌─────────────────────────────────────────────────────┐
  │  Phase 0: Clean MetadataStore abstraction            │
  │  (shared prerequisite for both cloud and edge)       │
  └────────────────────────┬────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
  ┌─────────────────────┐   ┌─────────────────────────┐
  │ Cloud: openraft      │   │ Edge: EmbeddedStore      │
  │ (08_danube_cloud_    │   │ + standalone mode         │
  │  openraft_migration) │   │ + Replicator              │
  │                      │   │ (this document)           │
  │ Priority: HIGH       │   │ Priority: AFTER cloud     │
  │ Timeline: 9-13 weeks │   │ Timeline: 6-10 weeks      │
  └─────────────────────┘   └─────────────────────────┘
```

**Phase 0 is shared**: Cleaning `MetaOptions`, adding `put_with_ttl` to the trait, implementing `watch()` for MemoryStore — all of this benefits both tracks.

**Cloud must come first**: The edge Replicator needs a working cloud cluster to replicate to. The cloud migration (ETCD → openraft) should be completed and stable before building the edge.

**Code reuse**: The `EmbeddedStore` (redb-backed KV with broadcast watches) is structurally similar to `DanubeStateMachine` from the Raft crate — just without the Raft consensus layer on top. They may share internal code (BTreeMap operations, watch infrastructure).

---

## 9. Implementation Phases (Edge-Specific)

These phases start **after** the cloud openraft migration is complete and stable.

### Phase E0: EmbeddedStore Provider [2-3 weeks]

- Implement `EmbeddedStore` backed by redb
- Implement `MetadataStore` trait (get, put, delete, watch, get_bulk)
- Watch via `tokio::sync::broadcast` channels
- Add `MetadataStorage::Embedded` variant
- Unit tests for all operations
- Cross-compile test for `aarch64-unknown-linux-musl`

### Phase E1: Standalone Broker Mode [1-2 weeks]

- Add `standalone: true` config flag
- Conditional skip of cluster services in `DanubeService::start()`
- Self-assign topics directly (no LoadManager roundtrip)
- `LeaderElection::is_leader()` returns `true` in standalone mode
- Integration test: standalone broker with embedded store, produce/consume cycle

### Phase E2: Replicator Service [3-4 weeks]

- Implement `Replicator` struct with per-topic `TopicReplicator`
- WAL reader: read committed entries from sealed + active segments
- Checkpoint tracking in embedded store
- Danube Producer to cloud cluster (reuse `danube-client`)
- Offline tolerance: retry loop with exponential backoff
- Topic namespace mapping
- zstd compression for batched messages
- Integration test: edge broker → cloud cluster end-to-end

### Phase E3: Hardening [1-2 weeks]

- Test on Raspberry Pi 4 (1 GB RAM): memory footprint, throughput benchmark
- WAN failure simulation: disconnect network, verify WAL accumulates, reconnect, verify catch-up
- Long-running test: 24h continuous ingestion with periodic WAN drops
- Binary size optimization (LTO, strip, musl)
- Systemd service file + documentation

```
Total Edge timeline: 7-11 weeks (after cloud migration is stable)
```

---

## 10. Competitive Positioning

| Feature | Danube Edge | MQTT Broker (Mosquitto) | Kafka (Strimzi on K3s) | NATS (leaf nodes) |
|---------|-------------|------------------------|----------------------|-------------------|
| Binary size | ~15-20 MB | ~1 MB | ~200+ MB (JVM) | ~20 MB |
| Memory footprint | ~50-100 MB | ~10 MB | ~512+ MB | ~50 MB |
| Message durability | WAL + cloud upload | QoS 2 (no WAL) | Kafka log | JetStream (Raft) |
| Cloud replication | Built-in Replicator | MQTT bridge (no WAL) | MirrorMaker (complex) | Leaf node (real-time) |
| Offline tolerance | Full (WAL accumulates) | Limited (QoS queues) | No | Partial |
| Schema support | Built-in registry | No | Confluent SR (separate) | No |
| Cloud storage integration | OpenDAL (S3/GCS/Azure) | No | Tiered storage (complex) | No |
| Protocol | gRPC | MQTT | Kafka protocol | NATS protocol |

**Danube Edge's unique value**: Integrated WAL + cloud storage + schema registry + Replicator in a single lightweight binary. No other system offers this full stack at edge scale.

---

## 11. Summary

Danube Edge transforms Danube from a cloud-only messaging system into an **edge-to-cloud data pipeline**:

1. **Edge devices** publish sensor/telemetry data to the local Danube Edge broker at sub-millisecond latency
2. **WAL** ensures messages survive power loss and network outages
3. **Cloud uploader** (OpenDAL) independently pushes WAL segments to S3/GCS for durable archival
4. **Replicator** forwards messages to the central Danube Cloud cluster for real-time analytics and aggregation
5. **Local consumers** on the edge can process data immediately (edge analytics, alerting, local control loops)

The edge broker is a **full Danube broker** minus the multi-broker coordination. Same gRPC API, same client libraries, same admin tools. An application written for Danube Cloud works on Danube Edge with zero code changes — only the broker config differs.
