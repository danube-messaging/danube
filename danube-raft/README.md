# danube-raft

Embedded Raft consensus layer for the Danube messaging cluster. Replaces etcd as the metadata store using [openraft](https://github.com/databendlabs/openraft) for consensus and [redb](https://github.com/cberner/redb) for durable on-disk log storage.

Every broker runs a Raft node in-process — there is no external dependency.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  danube-broker                                          │
│    calls MetadataStore trait: get / put / watch / ...   │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│  raft_store.rs — RaftMetadataStore                      │
│    Reads  → local state machine (in-memory BTreeMap)    │
│    Writes → propose through Raft (with leader forward)  │
│    Watch  → broadcast channels on state machine         │
└──────────┬──────────────────────────────┬───────────────┘
           │ writes                       │ reads
┌──────────▼──────────┐    ┌──────────────▼───────────────┐
│  openraft::Raft     │    │  state_machine.rs            │
│  (consensus engine) │───▶│  DanubeStateMachine          │
│                     │    │  (BTreeMap + watchers + TTL)  │
└──────┬──────┬───────┘    └──────────────────────────────┘
       │      │
┌──────▼──┐ ┌─▼──────────────┐
│log_store│ │ network.rs     │◄──► server.rs (gRPC handlers)
│ (redb)  │ │ (gRPC client)  │     on peer nodes
└─────────┘ └────────────────┘
```

## Write Path

1. Broker calls `put(key, value)` on `RaftMetadataStore`
2. `propose()` calls `raft.client_write(RaftCommand::Put{...})`
3. If this node isn't the leader → transparently forwarded via gRPC `ClientWrite` RPC
4. Leader appends the entry to its local redb log
5. Leader replicates the entry to followers via `AppendEntries` gRPC RPCs
6. Followers persist to their own redb logs and ACK
7. Once a quorum has persisted → entry is **committed**
8. All nodes call `DanubeStateMachine::apply()` → inserts into the in-memory `BTreeMap`, broadcasts `WatchEvent` to subscribers
9. Data is now available for reads on every node

## Read Path

Reads go directly to the local in-memory `BTreeMap` via `SharedStateMachineData` — zero network hops, zero disk I/O.

## Raft Log vs. State Machine

| | Raft Log (redb) | State Machine (BTreeMap) |
|---|---|---|
| **What** | Append-only sequence of `RaftCommand` entries | The derived KV store |
| **Where** | On-disk, per node | In-memory, per node |
| **Purpose** | Durability + crash recovery | Serving reads |
| **When updated** | Before commit (steps 4-6) | After commit (step 8) |

On restart, persisted log entries are replayed through `apply()` to reconstruct the in-memory state. Snapshots compact this — instead of replaying millions of entries, a node loads a snapshot and only replays entries after it.

## Modules

- **`typ.rs`** — Openraft type configuration. Wires `RaftCommand`, `RaftResponse`, `NodeId(u64)`, `BasicNode` to all generics.
- **`commands.rs`** — Defines `RaftCommand` (Put, PutWithTTL, Delete, DeletePrefix, CompareAndSwap, AllocateMonotonicId, ExpireTTLKeys) and `RaftResponse`.
- **`state_machine.rs`** — In-memory replicated KV store (`BTreeMap`) with watch channels and TTL index. Applies committed log entries and emits `WatchEvent`s.
- **`log_store.rs`** — Persistent Raft log using redb. Stores log entries (JSON) and Raft metadata (vote, purge state). Ensures durability across restarts.
- **`network.rs`** — Outbound gRPC transport. Sends `AppendEntries`, `Vote`, `InstallSnapshot` RPCs to peers (JSON-serialized over gRPC).
- **`server.rs`** — Inbound gRPC transport. Receives Raft RPCs from peers, plus `GetNodeInfo` (peer discovery) and `ClientWrite` (follower → leader forwarding).
- **`raft_store.rs`** — `MetadataStore` trait implementation. Reads from local state machine, writes via Raft consensus. Transparently forwards writes to leader if needed.
- **`node.rs`** — Entry point. Creates all components, starts gRPC server and TTL worker. Handles cluster bootstrap (single-node auto-init or multi-node seed discovery).
- **`leadership.rs`** — Lightweight handle for the broker to check Raft leadership status without depending on openraft.
- **`ttl_worker.rs`** — Background task (leader-only) that periodically expires TTL keys through Raft consensus, replacing etcd leases.

## Cluster Formation

Cluster formation is config-driven via `seed_nodes`:

```yaml
meta_store:
  data_dir: "./danube-data/raft"
  seed_nodes:
    - "0.0.0.0:7650"
    - "0.0.0.0:7651"
    - "0.0.0.0:7652"
```

- **Single-node** (empty/omitted `seed_nodes`): auto-initializes with zero config.
- **Multi-node**: all peers listed in `seed_nodes`. On first boot, nodes discover each other via `GetNodeInfo`, the lowest `node_id` initializes the cluster, and all nodes wait for leader election before proceeding.
- **Idempotent**: if the cluster is already initialized (persisted membership from a previous run), bootstrap returns immediately.

## Serialization

- **Raft RPCs** (network): JSON (`serde_json`) — required because `RaftCommand` contains `serde_json::Value` which is incompatible with bincode's `deserialize_any`
- **Raft log entries** (redb): JSON (`serde_json`)
- **Raft metadata** (vote, purge state): bincode (simple fixed-size types)
- **Snapshots**: JSON (`serde_json`)
