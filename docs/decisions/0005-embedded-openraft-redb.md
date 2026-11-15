# 0005: Embedded openraft + redb for metadata

**Status:** Accepted (replaced ETCD in v2)

## Context

Danube v1 used ETCD for metadata. Every deployment needed a separate coordination cluster, which is the main operational burden Danube wants to remove.

## Decision

Each broker embeds a Raft node (`danube-raft`):

- consensus: `openraft`; storage: `redb` (log + state machine) under `<data-dir>/raft/`
- implements `danube_core::metadata::MetadataStore` (get/put/delete/watch, `PutWithTTL`, leader-proposed `ExpireTTLKeys`), so broker code is storage-agnostic
- node identity: a stable `node_id` generated on first boot in `<data-dir>/raft/node_id`. The broker id **is** the Raft node id.
- bootstrap: no seeds means single-node init; `--seed-nodes` discovers peers and the lowest node id initializes, or the node auto-joins a running cluster; `--join` waits to be added with `danube cluster node add`
- Raft transport is gRPC on `--raft-port` (default 7650), with mTLS when the broker runs with TLS

## Why

- One binary, nothing else to deploy or upgrade in lockstep.
- TTL keys replace ETCD leases for broker liveness and ephemeral ownership.
- A single-node cluster is the same code path as a five-node cluster.

## Trade-offs

- Danube owns consensus correctness and upgrades of the on-disk format.
- Losing quorum stops metadata writes. The break-glass path is `danube cluster export-config` + `danube up ... --recovery-config` (0006).

## Rules for changes

- Never block the Raft apply path on I/O outside redb.
- New metadata keys must follow 0001 (bounded write rate) and be added to the docs `architecture/metadata-paths` page.
- Keep the voter cap logic (0010) in mind when touching membership.

