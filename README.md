<p align="center">
  <img src="Danube_logo.png" alt="Danube Logo" width="400">
</p>

<p align="center">
  <b>Agent-ready, zero-YAML streaming and messaging platform built in Rust. One binary.</b>
</p>

<p align="center">
  <a href="https://danube-messaging.com/"><img src="https://img.shields.io/badge/📑-Documentation-blue" alt="Documentation"></a>
  <a href="docker/README.md"><img src="https://img.shields.io/badge/🐳-Docker%20Ready-2496ED" alt="Docker"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/🦀-Rust-000000" alt="Rust"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/📜-Apache%202.0-green" alt="License"></a>
</p>

**Danube v2** is an open-source distributed messaging, event streaming, and agent coordination platform built in Rust on [Tokio](https://tokio.rs/) and embedded [openraft](https://github.com/databendlabs/openraft) (`redb`). It brings together sub-millisecond pub/sub, durable WAL + cloud-tiered streaming, zero-ETL Apache Iceberg lakehouse export, resilient edge MQTT ingestion, and native AI agent orchestration primitives inside **one self-contained binary (`danube`)** — with **zero YAML files** and **zero external dependencies**.

📖 **[Full Documentation](https://danube-messaging.com/)** · 🧭 **[Why Danube v2? Positioning & Architecture](danube_positioning.md)** · 🤖 **[CLI & MCP Control Plane](danube-admin/README.md)** · 🐳 **[Docker Guide](docker/README.md)**

---

## Try It in Seconds

Download the `danube` binary from [releases](https://github.com/danube-messaging/danube/releases) (or build with `cargo build --release -p danube-broker`):

```bash
# 1. Start a broker (zero config files required)
danube up --mode standalone

# Or run the container image
docker run -p 6650:6650 -p 9040:9040 \
  ghcr.io/danube-messaging/danube:latest \
  up --mode standalone --data-dir /danube-data --broker-addr 0.0.0.0:6650
```

Broker, Raft metadata, Schema Registry, and Admin gRPC are served cleanly over **`127.0.0.1:6650`**.

Test messaging, declarative state, or MCP from the **exact same binary**:

```bash
# Terminal 1: consume
danube ctl consume -s http://127.0.0.1:6650 -t /default/demo -m my_sub

# Terminal 2: produce
danube ctl produce -s http://127.0.0.1:6650 -t /default/demo -c 10 -m "Hello, Danube v2!"

# Apply declarative namespaces, topics (with StreamIntents), schemas, and RBAC
danube ctl apply -f cluster-state.yaml --dry-run
danube ctl apply -f cluster-state.yaml

# Start the MCP server for Claude Code, Cursor, VS Code, or Antigravity
danube mcp --broker-endpoint http://127.0.0.1:6650
```

Or connect from your application using the [client libraries](#client-libraries) (Rust, Go, Java, Python).

---

## Why Danube v2

Danube v2 collapses the legacy multi-binary, YAML-heavy messaging stack into an autonomous platform designed for both human operators and AI agents:

### 1. One Binary (`danube`): Broker, CLI, UI Server, and MCP
No separate broker, CLI, admin, or bridge binaries to keep in sync:
- `danube up` — Runs the broker (`cluster`, `standalone`, or `edge` mode; also the default when no subcommand is given)
- `danube ctl` — Full operator & developer CLI (`produce`, `consume`, `apply`, `topics`, `schemas`, `brokers`, `cluster`, `namespaces`, `security`, `serve`)
- `danube mcp` — Built-in Model Context Protocol (MCP) server (stdio or Streamable HTTP) with built-in cluster telemetry, broker log access, schema inference, and guarded destructive operations

### 2. Zero-YAML Operations, StreamIntents & Declarative State
- **Zero-config startup**: Start single-node or multi-node clusters purely via CLI flags and environment variables; runtime cluster state lives in embedded Raft (`--recovery-config` remains available strictly as a break-glass local override if Raft quorum is lost).
- **StreamIntent Profiles**: Create topics by workload intent (`agent_event_mesh`, `agent_task_queue`, `tool_invocation_log`, `telemetry_firehose`, `lakehouse_feed`, `custom`) — expanded into explicit Raft settings so upgrades never silently mutate running streams.
- **Declarative `danube ctl apply`**: GitOps-friendly YAML/JSON reconciliation that computes a dry-run plan first, is strictly additive by default, and requires explicit `--prune` (and `--force` for non-empty topics) to delete.

### 3. Native Primitives for AI Agents & Stateful Workflows
- **Execution Lease Heartbeats (`extend_lease`)**: Long-running LLM calls and tool steps extend processing leases without triggering redelivery storms.
- **Persisted Progress Checkpoints (`report_progress`)**: Workers checkpoint `checkpoint_data` into durable topic state so failover resumes at the last completed step instead of restarting expensive workflows from zero.
- **Cascading Task-Tree Cancellation (`cancel_task_tree`)**: Cancel an entire parent task tree across `x-correlation-id` / `x-parent-msg-id` so queued descendant messages are skipped cleanly at dispatch.
- **Copy-on-Write Stream Branching (`danube ctl topics branch`)**: Fork any topic at an offset or timestamp using reference-counted WAL and object-store segments for zero-copy replay, debugging, and counterfactual evaluation.
- **Request-Reply & Schema Inference**: Built-in `DanubeClient::request_reply` with correlation matching and `infer_topic_schema` to synthesize JSON Schema Draft-07 contracts from live samples.

### 4. High-Throughput Dual-Dispatch Engine & Honest Durability
- **Non-Reliable & Reliable Side-by-Side**: Run zero-disk-I/O in-memory streams next to durable WAL + Cloud Storage topics in the same cluster.
- **Pipelined Reliable Dispatch**: In-flight sliding windows (`DispatchWindow` / `InFlightWindow`, `max_unacked_messages`) eliminate single-message stop-and-wait bottlenecks while preserving per-key ordering under **Key-Shared** dispatch.
- **Honest WAL Group Commit**: Choose between periodic `Async`, OS-page-cache `OsCacheFlush`, or `fdatasync`-backed `FsyncBatch` durability modes.
- **Dead-Letter Queues**: Configurable retry budgets with exponential backoff and automatic DLQ routing.

### 5. Zero-Touch Security, Auto-TLS & Voter Safety
- **Embedded Auto-TLS (`--auth-mode auto-tls`)**: Automatically initializes an embedded Cluster CA in Raft (encrypted at rest via `DANUBE_CLUSTER_SECRET` or local secret) and distributes node certificates.
- **Strict RBAC & Payload Guardrails**: Default-deny RBAC including dedicated `ReadPayload` permission for payload peeking, `dry_run` defaults on destructive MCP tools, Raft audit trail, and break-glass bootstrap token recovery so admins cannot lock themselves out.
- **Production Quorum Safety**: Caps Raft voters (`target_voter_count`), promotes surplus brokers as non-voting learners, and gracefully drains on `SIGTERM` instead of churning Raft membership.

### 6. Lakehouse-Native & Edge-Ready
- **Embedded Lakehouse Library (`danube-iceberg`)**: Feature-gated (`--features danube-broker/lakehouse`) Apache Iceberg Parquet + statistics writer running on an isolated Tokio runtime with `--node-role` workload separation (`broker`, `lakehouse`, `all`) and REST (including AWS SigV4) or SQL catalog support.
- **Consolidated Edge Gateway**: Edge mode (`--mode edge`) bridges MQTT v3.1.1/v5.0 devices through a local WAL into a single consolidated cloud ingest topic (`/edge/{edge-name}/mqtt` with `x-mqtt-topic` metadata), protecting cloud Raft metadata from per-device topic explosion.

---

## Three Deployment Modes

### Standalone
Zero dependencies, ideal for local development, CI, and single-host services:

```bash
danube up --mode standalone --data-dir ./danube-data
```

### Cluster
Multi-node Raft consensus with automated topic placement, load-based rebalancing, and <100ms topic mobility over shared filesystem or object storage (S3/GCS/Azure):

```bash
# Node 1
danube up \
  --broker-addr 0.0.0.0:6650 \
  --raft-addr 0.0.0.0:7650 \
  --data-dir ./data/node1 \
  --seed-nodes "node1:7650,node2:7650,node3:7650"
```

Optional network isolation for control-plane traffic: add `--admin-addr 0.0.0.0:50051` to bind Admin gRPC on a separate listener, or `--node-role lakehouse|broker|all` to specialize nodes.

Deploy with [Docker Compose](docker/README.md) or [Kubernetes](https://danube-messaging.com/getting-started/kubernetes/).

### Edge
Lightweight MQTT gateway that buffers device messages in a local WAL during WAN outages and continuously replicates them over gRPC to a consolidated ingest topic in your cloud cluster:

```bash
danube up --mode edge \
  --edge-name factory-floor-1 \
  --cloud-endpoint http://cluster-node:6650 \
  --mqtt-addr 0.0.0.0:1883 \
  --data-dir ./edge-data
```

```
MQTT devices ──► Danube Edge (up --mode edge) ──► Local WAL ──► Cloud Cluster
                 (MQTT v3.1.1 / v5.0)             (survives     (/edge/<name>/mqtt
                                                   outages)      with x-mqtt-topic)
```

---

## Client Libraries

- **Rust**: [`danube-client`](danube-client/README.md) · [Examples](danube-client/examples/README.md)
- **Go**: [`danube-go`](https://pkg.go.dev/github.com/danube-messaging/danube-go) · [Examples](https://github.com/danube-messaging/danube-go/tree/main/examples)
- **Java**: [`danube-java`](https://central.sonatype.com/namespace/com.danube-messaging) · [Examples](https://github.com/danube-messaging/danube-java/tree/main/examples)
- **Python**: [`danube-client`](https://pypi.org/project/danube-client/) · [Examples](https://github.com/danube-messaging/danube-py/tree/main/examples)

## Project Structure

- **[`danube-broker`](danube-broker/)** — Unified `danube` binary (`up`, `ctl`, `mcp`), dispatcher, security, and cluster orchestration
- **[`danube-admin`](danube-admin/README.md)** — Control-plane CLI (`ctl`), HTTP UI/API server, and AI MCP server (`mcp`)
- **[`danube-edge`](danube-edge/)** — Edge MQTT v3.1.1/v5.0 gateway and WAL-backed cloud replicator
- **[`danube-iceberg`](danube-iceberg/README.md)** — Embedded Iceberg/Parquet lakehouse runtime (`--features danube-broker/lakehouse`)
- **[`danube-schema`](danube-schema/)** — Embedded Schema Registry (JSON Schema, Avro, Protobuf)
- **[`danube-raft`](danube-raft/README.md)** — Embedded `openraft` + `redb` consensus and metadata store
- **[`danube-persistent-storage`](danube-persistent-storage/README.md)** — Group-commit WAL, object-store tiering, and CoW segment lifecycle
- **[`danube-client`](danube-client/README.md)** — Async Rust client SDK
- **[`danube-core`](danube-core/README.md)** — Shared protocol definitions, topic paths, JWT helpers, and core types

## Building from Source

```bash
# Standard build (produces ./target/release/danube)
cargo build --release -p danube-broker

# With embedded Apache Iceberg lakehouse support (and AWS REST SigV4)
cargo build --release -p danube-broker --features lakehouse
cargo build --release -p danube-broker --features lakehouse-aws
```

## Contributing

Danube is actively developed with new features added regularly. See the [contribution guide](https://danube-messaging.com/contributing/) for setting up your local environment, running workspace tests (`cargo test --workspace --lib --bins`), and submitting pull requests.

**[🐛 Report Issues](https://github.com/danube-messaging/danube/issues)** · **[💡 Request Features](https://github.com/danube-messaging/danube/issues/new)** · **[📖 Documentation](https://danube-messaging.com/)**
