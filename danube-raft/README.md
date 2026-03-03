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

## Node Lifecycle

Each broker persists its `node_id` in `{data_dir}/node_id` on first boot. The bootstrap logic (`bootstrap_cluster`) returns a `BootstrapResult` enum that tells the broker how to proceed:

| Scenario | Detection | BootstrapResult | Broker behavior |
|---|---|---|---|
| **Fresh node, no peers** | No `node_id` file, empty `seed_nodes` | `Initialized` | Auto-inits single-node cluster |
| **Fresh node, first boot** | No `node_id` file, seed peers have no leader | `Initialized` | Lowest `node_id` bootstraps; others become followers |
| **Fresh node, existing cluster** | No `node_id` file, a seed peer reports `has_leader = true` | `JoinExisting` | Starts in "drained" state; must be added via `danube-admin cluster add-node` then activated |
| **Restart** | `node_id` file exists | `Restart` | Waits for leader election then resumes; membership is already persisted |

The `has_leader` flag is served by the `GetNodeInfo` gRPC RPC so that fresh nodes can distinguish between "cluster doesn't exist yet" and "cluster is already running". This eliminates the need for shell wrappers or init-containers in Kubernetes, the broker detects the right action automatically.

## Serialization

- **Raft RPCs** (network): JSON (`serde_json`) — required because `RaftCommand` contains `serde_json::Value` which is incompatible with bincode's `deserialize_any`
- **Raft log entries** (redb): JSON (`serde_json`)
- **Raft metadata** (vote, purge state): bincode (simple fixed-size types)
- **Snapshots**: JSON (`serde_json`)
