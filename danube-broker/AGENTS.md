# AGENTS.md: danube-broker

Builds the `danube` binary: CLI root, node boot, broker service, dispatcher, admin gRPC, security, load manager. Read the root `AGENTS.md` first.

## Entry points

- `src/cli/mod.rs`, `src/cli/up.rs`: clap root, grouped help, `danube up` modes, data-dir marker (`danube-node.json`), startup summary, `--dry-run`.
- `src/main.rs`: command dispatch, logging profiles, `run_node()` boot sequence (config → Raft → storage factory → services).
- `src/service_configuration.rs`, `src/storage_configuration.rs`: in-memory node config built from flags + Raft (no YAML).
- `src/broker_service.rs`, `src/topic*.rs`, `src/producer.rs`, `src/consumer.rs`: topic lifecycle and client sessions.
- `src/subscription.rs`, `src/dispatcher/`: delivery engines (exclusive, shared, key_shared), dispatch window, pending delivery, poison handler. See `src/dispatcher/README.md`.
- `src/stream_intent.rs`: intent presets → `ExpandedStreamSettings` (ADR 0007).
- `src/admin/`: admin gRPC services (topics, cluster, desired-state `apply`, security).
- `src/danube_service/`: load manager, broker registration/watcher, leader election, resource monitor (ADR 0010).
- `src/security/`: auth modes, auto-TLS, JWT, RBAC authz (ADR 0009).
- `src/resources/`: typed access to metadata paths in Raft.

## Invariants

- Producer/consumer hot paths never write to Raft per message. Subscription progress is flushed on a debounce (`sub_progress_flush_interval`, default 5s). See ADR 0001.
- Topic ownership is exclusive. Non-owners answer lookups with a redirect and never proxy data (ADR 0002).
- `BrokerMode` (standalone / cluster / edge) gates behaviour: cluster-only admin calls return "not available in standalone/edge mode".
- Intent settings are read from `/topics/{ns}/{topic}/intent`, never re-derived from the intent name.
- New authz checks use the narrowest resource scope (topic > namespace > cluster).

## Pitfalls

- Port 6650 multiplexes client, schema and admin gRPC. A `--join` node opens it only after joining, so add-node needs `--admin-listen`.
- Changing `danube up` flags means updating the workflows, docker, Makefile and docs (see root "When you change X").
- `metadata_storage.rs` wraps the Raft store, so tests can use the same `MetadataStore` trait.

## Tests

```bash
cargo test -p danube-broker --bin danube            # unit tests incl. cli::, dispatcher, intents
cargo test -p danube-broker --test <name>           # integration (needs a running broker, see tests/test_utils.rs)
```

