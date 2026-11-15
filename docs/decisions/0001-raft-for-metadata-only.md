# 0001: Raft replicates metadata, never message data

**Status:** Accepted

## Context

Danube needs a consistent source of truth for cluster state, and durable storage for message streams. A recurring question is: "Raft is already embedded, why not replicate topic data through it too?"

## Decision

Raft (`danube-raft`, openraft + redb) stores **metadata only**:

- cluster membership, broker registration (TTL keys), leader election
- topic ownership/assignment, namespaces, policies, intents (`/topics/{ns}/{topic}/intent`)
- subscriptions, failure policies, debounced subscription progress (cursors)
- RBAC roles, bindings, revoked subjects, the encrypted auto-TLS cluster CA
- schema registry subjects and versions
- cluster-wide config (storage mode, WAL retention, write buffer, load manager)
- segment descriptors that index the durable history of each topic

Message payloads go to a **per-topic WAL owned by one broker** (`danube-persistent-storage`). Sealed WAL segments are exported to a shared filesystem or object store and indexed in Raft.

## What must never go into Raft

- message payloads, or any write per message
- per-message acks or offsets (cursors are flushed on a debounce, about 5s)
- high-frequency telemetry (load reports are periodic and small, about every 30s)
- anything whose size grows with traffic instead of with cluster/topic count

Rule of thumb: Raft write rate should scale with **admin actions and topic count**, never with **message rate**.

## Why

- **Leader bottleneck:** every Raft write is serialized through one leader. Cluster throughput would be capped by a single node.
- **Write amplification:** each byte would be written to N nodes' disks and sent N-1 times over the network, on top of tiering.
- **Unbounded logs:** a stream log grows forever. Raft snapshot and compaction were not designed for terabytes of payload.
- **Elasticity:** with payloads outside Raft, moving a topic is a metadata change (see 0002). With payloads inside a replicated log, scaling means copying data.
- **Durability is delegated:** object storage already provides very high durability for sealed history at low cost.

## Trade-offs

- The active (unsealed) WAL tail lives on one broker until it is exported. Losing that disk loses the tail unless the Valkey write buffer is on (0003).
- Reads of old data come from tiered storage, which is slower than local disk.

## Rejected alternatives

- **Raft group per topic or partition:** thousands of groups means heartbeat and election storms, and complex rebalancing.
- **BookKeeper-style quorum storage:** an extra cluster to run, exactly what Danube exists to remove.
- **Kafka-style ISR replication between brokers:** requires per-partition leaders, replica sets and catch-up protocols inside Danube.

## Revisit if

A workload needs synchronous multi-broker durability for the tail **and** Valkey is not acceptable. The preferred direction is still tail replication outside Raft.
