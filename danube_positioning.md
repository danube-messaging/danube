# Danube Messaging (v2): Positioning & Technical Architecture

## 1. Summary & Identity

**Danube v2** is a distributed, agent-ready streaming and messaging platform built in Rust. It was redesigned around a core conviction: modern streaming infrastructure should require **one binary, zero YAML files, and zero external coordination services**, while giving both human operators and autonomous AI agents first-class durability, introspection, and workflow primitives.

Built on [Tokio](https://tokio.rs/) and embedded [openraft](https://github.com/databendlabs/openraft) (`redb`), Danube v2 eliminates the operational sprawl of traditional messaging stacks:

* **True Single Binary (`danube`):** One executable ships the broker (`danube up`), operator & data CLI (`danube ctl`), UI API server (`danube ctl serve`), and AI MCP gateway (`danube mcp`), served over a single gRPC listener (`6650`, with optional `--admin-addr` isolation).
* **Zero-YAML & Declarative State:** Clusters boot cleanly from CLI flags and environment variables; dynamic cluster config, RBAC, and topic policies live in Raft, managed declaratively via `danube ctl apply` or `StreamIntent` profiles (`--recovery-config` provides a local break-glass override if Raft quorum is lost).
* **Pipelined Dual-Dispatch Engine:** Sub-millisecond **Non-Reliable** (in-memory) and **Reliable** (group-commit WAL + S3/GCS/Azure object storage) topics run side-by-side with sliding in-flight dispatch windows (`max_unacked_messages`).
* **Native Agentic Runtime:** Built-in execution lease heartbeats (`extend_lease`), persisted progress checkpoints (`report_progress`), cascading task-tree cancellation (`cancel_task_tree`), Copy-on-Write stream branching (`branch`), and guardrailed MCP tooling.
* **Embedded Lakehouse Engine:** Feature-gated Apache Iceberg export (`danube-iceberg`) runs inside an isolated Tokio worker pool (`--node-role lakehouse` or `all`) to write Parquet files with full column statistics directly to Iceberg REST (including AWS SigV4) or SQL catalogs.
* **Edge-to-Cloud Continuum:** `--mode edge` turns the same binary into an MQTT v3.1.1/v5.0 gateway backed by a local WAL that replicates into a single consolidated cloud ingest topic (`/edge/{edge-name}/mqtt`), preventing per-device topic explosion in cloud Raft metadata.

---

## 2. Defining Capabilities: How Danube v2 Delivers an Edge

Rather than cloning legacy broker topologies, Danube v2 makes deliberate architectural choices tailored for cloud-native systems and long-running agentic workloads:

### 1. Pipelined Dual-Dispatch Engine & Honest WAL Durability
* **How Danube does it:** Topics choose between two dispatch strategies in the same cluster and over the same client connections:
  * **Non-Reliable (Fire-and-Forget / In-Memory):** Messages flow from producers into consumer channels via Tokio async queues with zero disk I/O and sub-millisecond dispatch. Ideal for metrics, heartbeats, and ephemeral telemetry.
  * **Reliable (Pipelined At-Least-Once / Durable):** Messages enter a local per-topic WAL using batched group commit with three explicit durability levels (`WalSyncMode::Async`, OS page-cache `WalSyncMode::OsCacheFlush`, or `fdatasync`-backed `WalSyncMode::FsyncBatch`), then dispatch through sliding in-flight windows (`DispatchWindow` / `InFlightWindow`, bounded by `max_unacked_messages`) with exponential NACK backoff and Dead Letter Queues (DLQ). Sealed WAL segments offload asynchronously to Cloud Object Storage.
  * **StreamIntent Presets:** Operators and agents can provision topics via high-level intents (`agent_event_mesh`, `agent_task_queue`, `tool_invocation_log`, `telemetry_firehose`, `lakehouse_feed`, `custom`) which expand into explicit, immutable settings persisted in Raft so broker upgrades never silently alter live topics.
* **The Industry Contrast:** Apache Kafka forces every message through partitioned disk logs even for disposable telemetry; other stacks force teams to pair Redis Pub/Sub with Kafka or Pulsar. Danube v2 unifies both modes in one engine while exposing honest durability semantics (`OsCacheFlush` vs `FsyncBatch`).

### 2. Instant Topic Mobility & Copy-on-Write Stream Branching
* **How Danube does it:** Historical segments live in shared storage or object storage (S3/GCS/Azure) indexed by Raft metadata. Moving a topic during load rebalancing seals the active WAL segment on the old broker and claims ownership on the new broker in `<100ms`. Using reference-counted segments, `danube ctl topics branch` (or MCP `branch_topic`) forks any topic at a specific offset or nanosecond timestamp without copying historical payloads.
* **The Industry Contrast:** Partition-bound systems like Kafka and Redpanda must copy gigabytes of broker-local log files over the network during rebalances, and duplicating a stream for replay or A/B evaluation requires building external mirror jobs. In Danube v2, both topic movement and stream branching are metadata-level operations.

### 3. Embedded Lakehouse Integration (Isolated Zero-ETL to Iceberg)
* **How Danube does it:** `danube-iceberg` compiles directly into the broker behind `--features lakehouse` (or `lakehouse-aws`) and executes on a dedicated `danube-lakehouse` Tokio runtime so Parquet conversion never stalls hot-path message delivery. Operators can dedicate nodes to analytics export with `--node-role lakehouse` or run mixed `--node-role all` nodes. Sealed WAL segments decode using Schema Registry definitions (JSON Schema, Avro) into ZSTD Parquet files with column bounds and null counts, committed to Iceberg REST (including AWS SigV4) or SQL catalogs.
* **The Industry Contrast:** Traditional event-to-lakehouse pipelines require Kafka Connect, Flink/Spark jobs, and separate schema synchronization services. Danube v2 converts sealed segments on an isolated runtime with zero external ETL infrastructure.

### 4. Key-Shared Subscriptions with Non-Blocking Per-Key Ordering
* **How Danube does it:** Alongside `Exclusive`, `Failover`, and `Shared`, Danube provides `Key-Shared` dispatch via consistent hashing and optional glob key filters. In v2, the Key-Shared reliable dispatcher checks subscription lag cleanly without blocking heartbeats or unrelated keys when one key awaits acknowledgment.
* **The Industry Contrast:** Partition-only brokers cap consumer group parallelism at the partition count; scaling past that requires risky partition splits. Danube v2 decouples consumer elasticity from physical partitioning while preserving strict per-key sequence.

### 5. Edge-to-Cloud Continuum (Single-Ingest-Topic Protection)
* **How Danube does it:** Starting `danube up --mode edge --cloud-endpoint ...` turns the binary into an MQTT v3.1.1/v5.0 gateway backed by a local WAL. To prevent thousands of edge device topics from overwhelming cloud Raft metadata, edge mode consolidates MQTT ingestion into `/edge/{edge-name}/mqtt` while preserving the original MQTT topic in the `x-mqtt-topic` message attribute.
* **The Industry Contrast:** Deploying Mosquitto + custom bridge scripts + separate cloud brokers creates fragile, unvalidated pipelines, while naive 1-to-1 MQTT-to-cloud topic mirroring causes metadata explosions. Danube v2 protects the cloud control plane while preserving full MQTT topic routing context.

### 6. Zero-YAML Autonomous Operations, Voter Safety & Auto-TLS
* **How Danube does it:**
  * **Declarative desired state (`danube ctl apply`):** Reconciles namespaces, topics, schemas, and RBAC additively by default, prints a dry-run plan before acting, and only deletes unmentioned user topics when `--prune` (plus `--force` for non-empty topics) is explicitly passed.
  * **Quorum-safe cluster lifecycle:** Raft voter membership caps automatically at `target_voter_count` so additional brokers join as non-voting learners (serving topics without slowing Raft commits), while `SIGTERM` drains node workloads rather than churning Raft membership.
  * **Embedded `--auth-mode auto-tls` & Guardrails:** Bootstraps an embedded Cluster CA stored encrypted in Raft (`DANUBE_CLUSTER_SECRET` or local key), supports break-glass local admin recovery, and requires `--auth-mode` as a deliberate startup setting rather than hot-swapping plaintext/TLS underneath live clients.
* **The Industry Contrast:** Managing ZooKeeper/etcd, external PKI, Confluent Schema Registry, and fragile multi-broker YAML inventories dominates real-world streaming ops. Danube v2 puts self-healing metadata, schema governance, and cluster PKI into one zero-YAML binary.

### 7. Native Primitives for AI Agents & Guardrailed Control Plane
* **How Danube does it:** Long-lived agent tasks do not fit short RPC timeouts or naive message queues. Danube v2 adds native protocol primitives and an AI-safe control plane:
  * **Execution Lease Heartbeats & Checkpoints:** Consumers call `extend_lease` during multi-step inference or tool execution and `report_progress` with structured `checkpoint_data` stored alongside topic state, letting restarted workers resume mid-task.
  * **Cascading Cancellation & Request-Reply:** `cancel_task_tree` propagates cancellation across `x-correlation-id` / `x-parent-msg-id` so queued descendant messages are skipped at dispatch; `DanubeClient::request_reply` correlates replies over standard non-reliable topics using `x-danube-correlation-id`.
  * **Schema Inference & Audited MCP Control Plane:** `danube mcp` connects assistants (Claude Code, Cursor, Antigravity) to live broker telemetry rings, in-memory log rings, `infer_topic_schema` (JSON Schema Draft-07 synthesis), and `branch_topic`. Destructive MCP actions default to `dry_run: true`, payload inspection requires dedicated `ReadPayload` RBAC permission, and control-plane mutations write to an internal audit log.
* **The Industry Contrast:** General-purpose message brokers leave leases, step checkpoints, tree cancellation, schema inference, and safe agent tooling to ad-hoc application databases and wrappers. Danube v2 treats AI agent coordination and safe LLM-driven operations as core broker primitives.

### 8. Predictable Rust Efficiency & Single-Port Simplicity
* **How Danube does it:** Built on Rust, Tokio async I/O, and lock-free hot paths with a memory footprint measured in megabytes, Danube v2 multiplexes client data, discovery, Schema Registry, and Admin gRPC onto port `6650` by default (with `--admin-addr` available when firewall separation is desired).
* **The Industry Contrast:** JVM brokers incur GC pauses, warm-up latency, and multi-gigabyte baseline footprints that complicate dense container and edge deployments.

---

## 3. Danube v2 vs. Apache Pulsar

Danube drew early inspiration from Pulsar's multi-tenant namespaces, tiered storage vision, and `Exclusive`/`Shared`/`Failover`/`Key-Shared` subscriptions, then simplified the runtime and added native agentic capabilities:

* **Deployment Footprint (`danube` vs. 3 Separate Clusters):** Production Pulsar requires ZooKeeper + BookKeeper bookies + Pulsar brokers. Danube v2 collapses broker, consensus (`openraft` + `redb`), Schema Registry, CLI, UI server, MCP gateway, and optional Iceberg lakehouse writer into one Rust binary with zero external daemons.
* **Zero-YAML & Declarative Ops:** Rather than managing configuration files across three tiers of JVM services, Danube v2 boots with CLI flags and reconciles desired state declaratively via `danube ctl apply` and `StreamIntent` presets.
* **Runtime Predictability:** Native Rust eliminates JVM heap tuning and stop-the-world GC pauses that trigger spurious heartbeats and consumer rebalances.
* **Agentic & Edge Native:** Pulsar lacks built-in execution leases, persisted step checkpoints, cascading task-tree cancellation, MCP guardrails, and an ultra-lightweight MQTT edge mode in the same binary.

---

## 4. Danube v2 vs. NATS

Because both Danube and NATS ship lightweight native binaries without the JVM, developers often compare them. Their architectural centers remain distinct:

* **Data & Workflow Model:** NATS excels at ephemeral wildcard subject routing and lightweight pub/sub; Danube v2 focuses on structured multi-tenant namespaces (`/ns/topic`), durable WAL + cloud-tiered streams, `Key-Shared` consistent-hash ordering, and long-running workflow state (leases, step checkpoints, CoW stream branching, and task-tree cancellation).
* **Lakehouse & Schema Governance:** NATS is payload-agnostic and leaves Iceberg/Parquet pipelines to external tooling. Danube v2 embeds a Schema Registry (JSON Schema, Avro, Protobuf) with live Draft-07 schema inference and an isolated `danube-iceberg` runtime for zero-ETL Parquet + Iceberg commits.
* **Unified Edge & Admin Plane:** Danube v2 pairs MQTT-to-consolidated-topic edge ingestion directly with embedded Raft governance, `danube ctl` declarative management, and an audited MCP server out of the box.
