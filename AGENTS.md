# AGENTS.md

Context for coding agents working on Danube. Read this first, then the `AGENTS.md` in the crate you are changing. Long-form rationale lives in [`docs/decisions/`](docs/decisions/README.md): open the relevant record before changing anything it covers.

## What Danube is

A messaging and event-streaming platform plus agent-coordination primitives, shipped as one Rust binary (`danube`). It has embedded Raft for metadata, a per-topic WAL for message data, and segments tiered to a shared filesystem or object storage. Requires no ZooKeeper, no BookKeeper, no JVM and no broker YAML.

## Repo map

- `danube-core`: shared types, protobuf/gRPC definitions, `MetadataStore` trait, topic paths, JWT helpers. Depends on nothing internal.
- `danube-raft`: embedded openraft + redb metadata store (`MetadataStore` impl, TTL keys, bootstrap/join).
- `danube-persistent-storage`: per-topic WAL, segment export to shared FS / object store, Valkey write buffer, recovery.
- `danube-schema`: schema registry logic (JSON Schema, Avro, Protobuf, compatibility).
- `danube-client`: async Rust SDK (producer, consumer, lookup/redirect, agent primitives).
- `danube-edge`: MQTT gateway + WAL-backed replicator for `danube up edge`.
- `danube-iceberg`: feature-gated lakehouse export (sealed segments to Parquet + Iceberg).
- `danube-admin`: client-side CLI commands, connection/contexts, web console backend, MCP server.
- `danube-broker`: the `danube` binary. CLI root (`src/cli`), boot (`src/main.rs`), broker service, dispatcher, admin gRPC, security, load manager.

Dependency direction: `core` ← (`raft`, `schema`, `persistent-storage`, `client`) ← (`edge`, `iceberg`, `admin`) ← `broker`. Never add a dependency that points the other way.

## Commands

```bash
cargo build -p danube-broker                     # builds ./target/debug/danube
cargo test -p danube-broker --bin danube cli::   # CLI tree + `up` parsing (fast)
cargo test -p danube-admin --lib                 # client CLI, contexts, MCP units
cargo test --workspace --lib --bins              # all unit tests
cargo test --workspace --no-run                  # make sure integration tests compile
target/debug/danube up standalone --dry-run      # resolved node config, starts nothing
target/debug/danube up standalone                # local node on 127.0.0.1:6650
```

Integration tests under `*/tests/` need a running broker. End-to-end coverage lives in `.github/workflows/*-e2e.yml` (cli, security, failover, restart, rebalance, edge, write buffer, shared-fs topic move).

## Invariants (do not break)

1. **Message payloads never go through Raft.** Raft holds metadata only, and per-message work must never write to it. Per-subscription progress is debounced (about 5s). See [0001](docs/decisions/0001-raft-for-metadata-only.md).
2. **A topic has exactly one owning broker.** Clients are redirected to the owner; brokers do not forward message data to each other. See [0002](docs/decisions/0002-single-owner-topics-and-elastic-scaling.md).
3. **The WAL is append-only.** Storage is reclaimed by whole segments, never by deleting individual messages. See [0004](docs/decisions/0004-append-only-wal-and-segment-retention.md).
4. **No startup YAML.** Node-local settings are `danube up` flags or env vars. Cluster-wide settings live in Raft and change via `danube cluster ...` or `danube apply`. See [0006](docs/decisions/0006-zero-yaml-configuration-layers.md).
5. **Intents expand once.** `--intent` is resolved into explicit settings when the topic is created and persisted. Never re-derive settings from the intent name later. See [0007](docs/decisions/0007-stream-intents.md).
6. **Optional dependencies stay optional.** Valkey, object storage, Prometheus and Iceberg must never become required to run a node.
7. **`unsafe` is forbidden** workspace-wide (`[workspace.lints]`).
8. **CLI shape:** the clap tree lives in `danube-broker/src/cli` plus the `Args` structs in `danube-admin/src/cli`. Keep `cli::` tests green, and every command needs an `about` and examples.

## Key decisions (index)

- [0001](docs/decisions/0001-raft-for-metadata-only.md) Raft replicates metadata, not messages: leader bottleneck, N times write amplification, unbounded logs.
- [0002](docs/decisions/0002-single-owner-topics-and-elastic-scaling.md) Single-owner topics + tiered segments: moving a topic is a metadata change, so brokers scale in and out fast.
- [0003](docs/decisions/0003-valkey-write-buffer-optional.md) Valkey write buffer is optional: it covers the unexported WAL tail when a disk is lost. Not required.
- [0004](docs/decisions/0004-append-only-wal-and-segment-retention.md) Append-only WAL, segment retention. "Delete on ack" is planned, not built.
- [0005](docs/decisions/0005-embedded-openraft-redb.md) Embedded openraft + redb instead of etcd or ZooKeeper.
- [0006](docs/decisions/0006-zero-yaml-configuration-layers.md) Config layers: flags, Raft, manifest, break-glass recovery file.
- [0007](docs/decisions/0007-stream-intents.md) Stream intents: workload presets expanded at creation.
- [0008](docs/decisions/0008-dual-dispatch.md) Non-reliable (memory) and reliable (WAL) dispatch side by side.
- [0009](docs/decisions/0009-security-architecture.md) Security: auth mode fixed at startup, JWT + RBAC in Raft, auto-TLS CA encrypted in Raft.
- [0010](docs/decisions/0010-load-manager-and-voter-cap.md) Load manager placement and rebalancing, voter cap with learners.
- [0011](docs/decisions/0011-embedded-schema-registry.md) Schema registry embedded, state in Raft, served on the broker port.
- [0012](docs/decisions/0012-edge-gateway.md) Edge consolidates MQTT into one ingest topic per edge node.
- [0013](docs/decisions/0013-lakehouse-isolation.md) Lakehouse export reads sealed segments on an isolated runtime.
- [0014](docs/decisions/0014-single-binary-cli-and-mcp.md) One binary, one CLI tree, MCP with guardrails.

## Known pitfalls

- A new subscription starts at the **end** of a reliable topic: start consumers before producers in tests and examples.
- A CLI producer on a reliable topic needs `danube produce -r`; the broker rejects a mismatched dispatch strategy.
- A `--join` node only opens its shared `--listen` port after it becomes a member, so it needs `--admin-listen` for `danube cluster node add`.
- Every data dir records its mode in `danube-node.json`; tests must not reuse a dir across modes.
- Consume and receive permission checks currently happen at cluster scope only (topic-scoped bindings are not enough).
- Multi-node `--auth auto-tls` generates per-node CAs before they sync through Raft: an open issue.
- `danube cluster export-config` output contains secrets (JWT key, object-store credentials).
- Some READMEs and `danube_positioning.md` can lag behind the code. The code and `docs/decisions/` are the source of truth.

## When you change X, also update Y

- **CLI flags or commands:** the `cli::` tests, `.github/workflows/*`, `Makefile`, `docker/`, `README.md`, `CHANGELOG.md` (Unreleased), and the docs repo (`danube-docs`, especially `getting-started/the-danube-cli` and `reference/broker-configuration`).
- **Metadata paths:** `danube-docs` `architecture/metadata-paths`.
- **MCP tools or prompts:** `danube-admin/src/mcp`, and `admin/mcp` in the docs.
- **Intents, failure policies, storage modes:** the matching ADR and the concepts/durability docs.
- **A decision listed above:** update or supersede its ADR in the same PR.

## Writing style (docs, help text, CHANGELOG)

Short sentences, lead with an example, lists instead of wide tables, no em dashes ("—"); use "-", "," or ":". Documentation lives in the separate `danube-docs` repo (Astro Starlight).

