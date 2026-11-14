<p align="center">
  <img src="Danube_logo.png" alt="Danube Logo" width="400">
</p>

<p align="center">
  <b>Agent-ready, zero-YAML messaging and event streaming built in Rust. One binary.</b>
</p>

<p align="center">
  <a href="https://danube-messaging.com/"><img src="https://img.shields.io/badge/%F0%9F%93%91-Documentation-blue" alt="Documentation"></a>
  <a href="docker/README.md"><img src="https://img.shields.io/badge/%F0%9F%90%B3-Docker%20Ready-2496ED" alt="Docker"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/%F0%9F%A6%80-Rust-000000" alt="Rust"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/%F0%9F%93%9C-Apache%202.0-green" alt="License"></a>
</p>

**Danube** is an open-source distributed messaging, event streaming, and agent coordination platform built in Rust on [Tokio](https://tokio.rs/) and embedded [openraft](https://github.com/databendlabs/openraft) (`redb`). It combines sub-millisecond pub/sub, durable WAL + cloud-tiered streams, zero-ETL Apache Iceberg lakehouse export, edge MQTT ingestion, and native AI agent orchestration primitives in **one self-contained binary (`danube`)**: **zero YAML files**, **no JVM**, and **no external coordinators** like ZooKeeper or BookKeeper.

📖 **[Full Documentation](https://danube-messaging.com/)** · 🧭 **[Positioning & Architecture](danube_positioning.md)** · 🤖 **[CLI & MCP Control Plane](danube-admin/README.md)** · 🐳 **[Docker Guide](docker/README.md)**

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

Client messaging, discovery, Schema Registry, and Admin gRPC are served together on **`127.0.0.1:6650`**.

Send messages, apply declarative state, or connect an AI agent using the **same binary**:

```bash
# Terminal 1: consume messages
danube ctl consume -s http://127.0.0.1:6650 -t /default/demo -m my_sub

# Terminal 2: produce messages
danube ctl produce -s http://127.0.0.1:6650 -t /default/demo -c 10 -m "Hello, Danube!"

# Declaratively provision namespaces, topics (with intents), schemas, and RBAC
danube ctl apply -f cluster-state.yaml --dry-run
danube ctl apply -f cluster-state.yaml

# Start the built-in MCP server for Claude Code, Cursor, VS Code, or Antigravity
danube mcp --broker-endpoint http://127.0.0.1:6650
```

Or connect directly from your applications via the [client libraries](#client-libraries) (Rust, Go, Java, Python).

---

## Core Streaming Capabilities

Danube gives you a complete, cloud-native streaming engine out of the box without assembling half a dozen separate infrastructure projects:

| Capability | What You Get |
| :--- | :--- |
| **Dual-Dispatch Engine** | Run **Non-Reliable** (in-memory, zero-disk-I/O, sub-millisecond) and **Reliable** (WAL + cloud-tiered) topics side by side in the same cluster. Reliable delivery pipelines messages via sliding windows (`max_unacked_messages`), honest group-commit durability (`Async`, `OsCacheFlush`, `FsyncBatch`), exponential backoff retries, and Dead-Letter Queues (DLQ). |
| **Flexible Subscription Models** | Supports **Exclusive**, **Failover**, **Shared**, and **Key-Shared** (consistent-hash per-key ordering with glob key filters) subscriptions, decoupling consumer scaling from partition counts. |
| **Instant Topic Mobility** | Metadata lives in embedded Raft while historical segments live in shared filesystems or object storage (S3, GCS, Azure). Rebalancing and failover move topic ownership across brokers in `<100ms` without copying gigabytes of log data. |
| **Embedded Schema Registry** | Built-in **JSON Schema**, **Avro**, and **Protobuf** registry with versioned subjects and compatibility validation on port `6650`. |
| **Zero-ETL Iceberg Lakehouse** | Feature-gated [`danube-iceberg`](danube-iceberg/README.md) (`--features lakehouse`) converts sealed WAL segments into ZSTD Parquet files with column stats on an isolated Tokio pool and commits directly to Apache Iceberg REST (including AWS SigV4) or SQL catalogs. |
| **Edge-to-Cloud MQTT Gateway** | `danube up --mode edge` embeds an MQTT v3.1.1/v5.0 broker backed by a local WAL that consolidates unmatched edge topics into `/{edge-name}/ingest` (preserving `mqtt.topic` and `routing_key`) so IoT fleets survive WAN outages without exploding cloud Raft metadata. |

---

## Simple for Humans (Zero-Ops UX)

Danube is built so a single developer or small platform team can go from laptop prototype to production Kubernetes without fighting YAML sprawl:

* **One Binary, One Port**: `danube up` runs the broker, `danube ctl` handles CLI admin, UI serving, and data testing, and `danube mcp` exposes AI tooling. Everything multiplexes cleanly onto port `6650` by default (`--admin-addr` is available if you want separate firewall listeners).
* **Zero-YAML Startup & Dynamic Raft Config**: Clusters boot from CLI flags and environment variables while policies, schemas, storage tiering, and RBAC live dynamically in embedded Raft (with `--recovery-config` reserved strictly for break-glass recovery).
* **Intent-Driven Provisioning (`--intent`)**: Create topics by workload role instead of guessing flush flags and retry policies (`realtime`, `event_stream`, `task_queue`, `audit_log`). Intents expand at creation time into explicit Raft settings for delivery strategy, WAL sync durability, and default subscription retry/DLQ behavior so upgrades never silently change live topics.
* **Safe Additive GitOps (`danube ctl apply`)**: Reconcile namespaces, topics, intents, schemas, and RBAC from a single manifest. Previews a `--dry-run` plan first and is strictly additive by default (never deleting existing topics unless `--prune` and `--force` are passed).
* **One-Flag Security & Safe Rolling Restarts**: Enable encrypted transport and bootstrap RBAC in seconds with `--auth-mode auto-tls` (encrypted Cluster CA in Raft + `0600` bootstrap admin token) or mount `cert-manager` PEMs. Raft caps voters automatically (`target_voter_count`), promotes surplus brokers as active non-voting learners, and drains topics cleanly on `SIGTERM` without quorum churn.

---

## Built for AI Agents

General-purpose brokers force AI teams to bolt external databases onto message queues for long-lived tasks, and force AI SREs to scrape raw Prometheus logs to debug clusters. **Danube treats agents as first-class citizens**: both as **application workloads** running on streams and as **platform operators** managing the cluster.

### 1. Running Agent Workloads on Danube (Agent-Native Runtime)

Agent workflows run for minutes instead of milliseconds, spawn dynamic sub-task trees, crash mid-trajectory, and need reproducible branching for evaluation. Danube embeds these coordination primitives directly into the broker and client SDK:

* **Heartbeated Task Leases (`extend_lease`)**: Long-running LLM calls and tool steps send lease heartbeats to keep ownership alive without timing out or triggering duplicate work across workers.
* **Mid-Task Progress Checkpoints (`report_progress`)**: Workers persist structured step checkpoints (`checkpoint_data`) directly with pending delivery state. When a worker crashes or a topic moves, Danube redelivers the task with its last checkpoint (`x-danube-checkpoint-b64`) so execution resumes at the failed step instead of restarting from scratch.
* **Cascading Task-Tree Cancellation (`cancel_task_tree`)**: Cancel an entire parent task tree across `x-correlation-id` and `x-parent-msg-id` headers in one call. The broker's dispatcher skips queued descendant tasks cleanly so orphaned sub-agents do not waste compute or model tokens.
* **Zero-Copy Stream Branching (`danube ctl topics branch`)**: Fork any topic at an offset or nanosecond timestamp into an independent writable branch topic (`topics branch <src> <dst> --at-offset N`) with reference-counted segment protection: ideal for counterfactual replay, A/B prompt evaluation, and "Git for agent trajectories."
* **Native Request-Reply (`DanubeClient::request_reply`)**: Built-in correlation matching (`x-danube-correlation-id` / `x-danube-reply-to`) over low-latency `Non-Reliable` reply topics for synchronous agent-to-tool and agent-to-agent calls.

### 2. Operating & Debugging with AI Agents (`danube mcp`)

Connect Claude Code, Cursor, VS Code, or Antigravity directly to your cluster over stdio or HTTP/SSE:

```bash
danube mcp --broker-endpoint http://127.0.0.1:6650
```

* **Zero External Monitoring Required**: Built-in in-memory telemetry and log ring buffers give MCP clients instant access to broker metrics, logs, and composite one-call root-cause diagnostics (`explain_consumer_stall`, `inspect_subscription`).
* **Non-Destructive Peeking & Schema Inference**: Agents can sample live traffic without advancing consumer cursors (`peek_messages` / `danube ctl topics peek`) and automatically generate JSON Schema Draft-07 contracts from real payloads (`infer_topic_schema` / `danube ctl topics infer-schema`).
* **Guardrails & Auditability by Default**:
  * Destructive actions (`delete_topic`, `unload_broker`, `truncate`) default to `dry_run: true` and require explicit confirmation.
  * Payload inspection requires a dedicated `ReadPayload` RBAC permission separate from cluster metadata access.
  * Control-plane mutations are recorded to an internal Raft audit log.

---

## Three Deployment Modes

### Standalone
Zero dependencies, ideal for local development, CI, and single-host services:

```bash
danube up --mode standalone --data-dir ./danube-data
```

### Cluster
Multi-node Raft consensus with automated topic placement, load-based rebalancing, and `<100ms` topic mobility over shared filesystem or object storage (S3/GCS/Azure):

```bash
# Node 1
danube up \
  --broker-addr 0.0.0.0:6650 \
  --raft-addr 0.0.0.0:7650 \
  --data-dir ./data/node1 \
  --seed-nodes "node1:7650,node2:7650,node3:7650"
```

Optional flags: `--admin-addr 0.0.0.0:50051` to separate Admin gRPC, `--seed-nodes <addrs> --join` to join an existing cluster, or `--node-role lakehouse|broker|all` to dedicate Iceberg export nodes (`DANUBE_LAKEHOUSE_CONFIG`).

Deploy with [Docker Compose](docker/README.md) or [Kubernetes](https://danube-messaging.com/getting-started/kubernetes/).

### Edge
Lightweight MQTT gateway that buffers device messages in a local WAL during WAN outages and continuously replicates them over gRPC into a single consolidated ingest topic in your cloud cluster:

```bash
danube up --mode edge \
  --edge-name factory-floor-1 \
  --cloud-endpoint http://cluster-node:6650 \
  --mqtt-addr 0.0.0.0:1883 \
  --data-dir ./edge-data
```

```text
MQTT devices ──► Danube Edge (up --mode edge) ──► Local WAL ──► Cloud Cluster
                 (MQTT v3.1.1 / v5.0)             (survives     (/<edge-name>/ingest
                                                   outages)      with mqtt.topic & routing_key)
```

---

## Client Libraries

- **Rust**: [`danube-client`](danube-client/README.md) · [Examples](danube-client/examples/README.md)
- **Go**: [`danube-go`](https://pkg.go.dev/github.com/danube-messaging/danube-go) · [Examples](https://github.com/danube-messaging/danube-go/tree/main/examples)
- **Java**: [`danube-java`](https://central.sonatype.com/namespace/com.danube-messaging) · [Examples](https://github.com/danube-messaging/danube-java/tree/main/examples)
- **Python**: [`danube-client`](https://pypi.org/project/danube-client/) · [Examples](https://github.com/danube-messaging/danube-py/tree/main/examples)

## Project Structure

- **[`danube-broker`](danube-broker/)** : Unified `danube` binary (`up`, `ctl`, `mcp`), dispatcher, security, and cluster orchestration
- **[`danube-admin`](danube-admin/README.md)** : Control-plane CLI (`ctl`), HTTP UI/API server, and AI MCP server (`mcp`)
- **[`danube-edge`](danube-edge/)** : Edge MQTT v3.1.1/v5.0 gateway and WAL-backed cloud replicator
- **[`danube-iceberg`](danube-iceberg/README.md)** : Embedded Iceberg/Parquet lakehouse runtime (`--features danube-broker/lakehouse`)
- **[`danube-schema`](danube-schema/)** : Embedded Schema Registry (JSON Schema, Avro, Protobuf)
- **[`danube-raft`](danube-raft/README.md)** : Embedded `openraft` + `redb` consensus and metadata store
- **[`danube-persistent-storage`](danube-persistent-storage/README.md)** : Group-commit WAL, object-store tiering, and CoW segment lifecycle
- **[`danube-client`](danube-client/README.md)** : Async Rust client SDK
- **[`danube-core`](danube-core/README.md)** : Shared protocol definitions, topic paths, JWT helpers, and core types

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
