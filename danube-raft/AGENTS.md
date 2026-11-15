# AGENTS.md: danube-raft

Embedded metadata store: openraft consensus + redb storage, exposed through `danube_core::metadata::MetadataStore`. Read ADRs 0001, 0005 and 0010.

## Entry points

- `src/node.rs`: `RaftNode::start`, stable `node_id` (`<data-dir>/raft/node_id`), `bootstrap_cluster` (single-node init, seed discovery, auto-join).
- `src/state_machine.rs`, `src/raft_store.rs`, `src/log_store.rs`: redb-backed state machine and log.
- `src/commands.rs`: replicated commands (`Put`, `PutWithTTL`, `Delete`, `ExpireTTLKeys`, ...).
- `src/ttl_worker.rs`: the leader proposes expiry of TTL keys (broker liveness, ephemeral ownership).
- `src/network.rs`, `src/server.rs`: gRPC transport between nodes (mTLS when configured).
- `src/leadership.rs`: leadership handle used by leader-only services (load manager).

## Invariants

- Metadata only. Reject designs that put message payloads or per-message updates into Raft (ADR 0001).
- State machine apply must be deterministic and must not do external I/O.
- TTL expiry is decided by the leader and replicated as a command, never by local clocks on followers.
- Membership: voters are capped (`target_voter_count`, default 5) and extra nodes stay learners.

## Pitfalls

- The advertised Raft address is derived from the broker's advertised host plus `--raft-port`, so several nodes on one host need distinct ports.
- Changing the on-disk format needs a migration plan: existing data dirs resume their state.

## Tests

```bash
cargo test -p danube-raft            # includes cluster_and_leadership, watch_and_ttl
```
