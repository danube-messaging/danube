<!-- v0.14.1 START -->
## v0.14.1 - 2026-05-30

**Danube Admin, the Unified Cluster Management Tool**

This release focuses on `danube-admin`, expanding it into a full-featured administration tool. The Admin UI server now serves a web dashboard for cluster, topic, schema, and RBAC management. The MCP server grew from 32 to 58 tools with new security, diagnostics, and metrics capabilities. Several gRPC proto changes support the new admin features.

### 🖥️ Admin UI

* **Dedicated UI server mode**: `danube-admin serve --mode ui` serves the Admin UI dashboard over HTTP. by @danrusei in a926382
* **Cluster & RBAC pages** (#225): Refactored cluster overview page and new RBAC administration page for managing roles and bindings. by @danrusei in c0a0f0e
* **Schema registry details** (#224): Admin UI now displays schema subjects, versions, and compatibility settings. by @danrusei in 0529eb3
* **Security bindings**: Retrieve and display all security bindings in the admin UI. by @danrusei in bc4e283

### 🤖 MCP Server

* **58 tools** (#226): Added security (RBAC roles, bindings, JWT tokens), diagnostics (health checks, consumer lag, recommendations), metrics (Prometheus queries), and log access tools. by @danrusei in 8708687

### 📦 Maintenance

* **Dependency upgrades** (#227): Updated workspace dependencies. by @danrusei in ff6cf2a
* **Documentation**: Updated links and added edge mode to README. by @danrusei in 2b05bf4, b88a6de
<!-- v0.14.1 END -->

<!-- v0.14.0 START -->
## v0.14.0 - 2026-05-14

**Danube Edge — IoT Ingestion at the Edge**

This release introduces **Danube Edge**, a lightweight edge broker that accepts MQTT device connections and replicates data to the central Danube cluster. IoT devices publish via standard MQTT v3.1.1/v5.0; the edge broker validates payloads against schemas, batches messages into a local WAL, and continuously replicates to the cloud, bridging the gap between constrained edge networks and Danube's cluster-scale processing.

### 🌐 Edge Broker

* **MQTT ingestion gateway** (#217, #218) — Full MQTT v3.1.1 and v5.0 support with per-connection session management, topic routing via wildcard patterns (`device/+/telemetry`), and attribute extraction from topic segments. by @danrusei in 68fd53b, 2a40171

* **WAL-based edge replication** (#220) — Messages are batched into a local WAL and tail-replicated to the cluster with checkpoint-based resumption. No data is lost on transient network failures. by @danrusei in c45eb04

* **Schema enforcement at the edge** (#221, #222) — Schemas are resolved from the cluster registry at startup and enforced locally. Invalid payloads receive protocol-appropriate feedback: MQTT v5 gets `PUBACK` with reason code `0x99` (Payload format invalid); v3.1.1 gets accept-but-drop to prevent infinite retries. by @danrusei in 7f52e31, 7a23715

### 🛡️ Production Hardening

* **Keep-alive timeout & max payload size** (#223) — Sessions respect the MQTT keep-alive contract (1.5× timeout per spec); oversized packets are rejected at the codec level before memory allocation. by @danrusei in bf3033f

* **Graceful shutdown** — MQTT server and ingester flush loop respond to shutdown signals; in-flight WAL batches are flushed before exit.

* **Connection limits & observability** — Configurable `max_connections` semaphore prevents resource exhaustion; Prometheus metrics cover connections, message throughput, validation drops, and flush latency.

### 🏗️ Infrastructure

* **Schema registry extraction** (#219) — Moved the schema registry into its own `danube-schema` crate, enabling the edge broker to validate payloads without pulling in the full broker. by @danrusei in 1b852e1

* **Broker startup refactor** (#215, #216) — Untangled cluster orchestration from the core `DanubeService`, enabling standalone and edge modes to start without Raft voter overhead. by @danrusei in 806e527, 3fe9604
<!-- v0.14.0 END -->

<!-- v0.12.0 START -->
## v0.12.0 - 2026-04-23

**Key-Shared Subscriptions & Dispatcher Safety**

This release introduces the **Key-Shared subscription model**, a fourth subscription type that routes messages to consumers based on their routing key using consistent hashing. All messages with the same key go to the same consumer, in order, while different keys are processed in parallel. It also hardens the dispatcher with per-consumer backpressure, inactive consumer eviction, and centralized poison handling.

### Key-Shared Subscriptions

* **Consistent hash ring routing** (#213) — 100-vnode FNV-1a ring with ~1/N key remapping on consumer changes. by @danrusei in fc0f5e7
* **Key filtering** (#213) — Glob-based patterns (`"payment"`, `"ship*"`, `"eu-west-?"`) for explicit key-to-consumer assignment. by @danrusei in fc0f5e7
* **Multi-message in-flight window** (#213) — Per-key ordering with contiguous cursor advancement for safe broker restart. by @danrusei in fc0f5e7
* **`send_with_key()` and `with_key_filter()` client APIs** — Producer routing key tagging and consumer key filter declaration.

### 🛡️ Dispatcher Safety

* **Per-consumer backpressure, inactive eviction, centralized poison handler, dispatcher deadlock fix** (#214) by @danrusei in 873818d
<!-- v0.12.0 END -->

<!-- v0.11.0 START -->
## v0.11.0 - 2026-04-05

**Authentication, Authorization & Cluster Stability**

This release adds a complete security layer to Danube: multi-method authentication, fine-grained RBAC with default-deny semantics, and management APIs for roles and bindings. It also fixes several cluster stability issues around topic convergence in multi-broker deployments.

### 🔒 Security

* **Authentication** — Brokers authenticate every request via API-key service accounts, JWT bearer tokens, or mTLS-derived broker-internal identity. Auth mode is configurable (`none`, `tls`). JWT validation results are cached (30s TTL) to avoid cryptographic overhead on hot paths.

* **RBAC authorization** — 9 permission types (`Lookup`, `Produce`, `Consume`, `ManageTopic`, `ManageSchema`, etc.) scoped to 5 resource types (`Cluster`, `Namespace`, `Topic`, etc.). Bindings resolve hierarchically (topic → namespace → cluster). All admin and data-plane RPCs are gated.

* **Security management** — `SecurityAdmin` gRPC service with role and binding CRUD. `danube-admin security` CLI for managing authorization state and generating JWT tokens.

### 📦 Client

* **Dynamic token rotation** — `with_token_supplier()` accepts a closure called on every request, enabling runtime token refresh from K8s projected volumes or secret managers.

* **Explicit topic auto-creation guard** — Only producer requests can auto-create topics; consumer requests are explicitly prevented via an `allow_auto_create` flag.

### 🐛 Cluster Stability

* **Eager topic loading** — Brokers now eagerly load topics assigned to them when a client request arrives before the watch event fires, eliminating the convergence race that caused intermittent failures in multi-broker clusters.

* **Idempotent topic registry** — Concurrent topic loading (from watch events and eager-load) can no longer replace a live topic instance, preventing "producer not attached" errors.

* **Reliable DLQ dispatch** — The replicator now creates reliable producers for DLQ topics, matching the topic's dispatch strategy.
<!-- v0.11.0 END -->

<!-- v0.10.0 START -->
## v0.10.0 - 2026-03-28

**Reliable Dispatch Failure Handling**

This release adds configurable failure policies to reliable subscriptions, giving operators control over what happens when a message cannot be delivered, from automatic retries with backoff to dead-letter queues for poisoned messages.

### 🛡️ Failure Policies & Poison Message Handling

* **Consumer NACK support** (#210) — Consumers can now explicitly reject a message via `nack(msg, delay_ms, reason)`. The broker schedules a redelivery using the subscription's failure policy (fixed or exponential backoff), preserving the at-least-once guarantee while giving consumers control over retry timing. by @danrusei in 9de24b5

* **Configurable failure policies** (#210) — Each subscription can be configured with `max_redelivery_count`, `ack_timeout_ms`, backoff strategy (Fixed / Exponential), base and max redelivery delays, and a poison policy. Policies are set via `danube-admin topics set-failure-policy` and persist in Raft metadata. by @danrusei in 9de24b5

* **Poison message policies** (#210) — When a message exhausts its retry budget, operators choose one of three outcomes: **DeadLetter** (route to a configurable DLQ topic with full origin metadata), **Drop** (skip the message and resume progress), or **Block** (halt the subscription until manual intervention). by @danrusei in 9de24b5

* **Dead-letter queue with origin metadata** (#210) — DLQ messages carry `x-original-topic`, `x-original-subscription`, `x-poison-policy`, `x-delivery-attempt`, `x-failure-reason`, and original offset/producer attributes, enabling full traceability from the DLQ back to the source. by @danrusei in 9de24b5

<!-- v0.10.0 END -->

<!-- v0.9.0 START -->
## v0.9.0 - 2026-03-23

**Persistent Storage Revamp & Zero-Config Single-Node Mode**

This release rebuilds the `danube-persistent-storage` crate around a sealed-segment architecture and introduces a zero-config single-node mode so you can start a broker with a single command.

### 💾 Persistent Storage Revamp

* **Sealed-segment storage engine** (#208) — Rebuilt `danube-persistent-storage` around a three-layer model: per-topic local WAL for fast writes, immutable durable segments for historical reads and recovery, and Raft metadata for segment descriptors and topic mobility. Replaces the previous cloud-object storage path. by @danrusei in 7cbea0d

* **Three storage modes** (#208) — `local` (WAL-only), `shared_fs` (WAL + shared filesystem), `object_store` (WAL + S3/GCS/Azure Blob via OpenDAL). Background segment export, tiered reads, and local WAL retention run automatically in `shared_fs` and `object_store` modes. by @danrusei in 7cbea0d

* **Topic mobility** (#208) — Topics move between brokers with full offset continuity. The old broker seals and exports remaining WAL data; the new broker resumes from durable segments and metadata. by @danrusei in 7cbea0d

### 🚀 Zero-Config Single-Node Mode

* **`--single-node --data-dir` CLI** (#209) — Run a broker without a config file. Auto-generates defaults (broker `127.0.0.1:6650`, admin `127.0.0.1:50051`, Raft `127.0.0.1:7650`, local storage). Bootstraps a single-node Raft cluster on first boot; data persists across restarts. by @danrusei in 5e4a9e9

* **Default logging** (#209) — Info-level logs on stderr by default, OpenRaft noise suppressed. Overridable via `RUST_LOG`. by @danrusei in 5e4a9e9
<!-- v0.9.0 END -->

<!-- v0.8.1 START -->
## v0.8.1 - 2026-03-14
### Performance Highlights

This release focuses on reducing broker hot-path overhead. The biggest gains come from simplifying concurrency on publish and dispatch paths, isolating slow consumers so they no longer stall fan-out, and reducing message-copying costs.

* **Hot-path concurrency cleanup** (#203) — Removed topic workers from the publish and delivery path, reducing queue boundaries, scheduler wakeups, and the mix of actor-style and lock-based coordination. by @danrusei in c490d4f

* **Slow-consumer isolation for non-reliable subscriptions** (#204) — Non-reliable dispatch now uses non-blocking enqueue behavior so a saturated consumer no longer stalls topic fan-out. Exclusive subscriptions drop on full channels, while shared subscriptions skip saturated consumers before dropping only when all healthy targets are full. by @danrusei in 464b62d

* **Lower payload-copying overhead** (#205) — Migrated message payload handling from `Vec<u8>` to `Bytes`, enabling shallow clones and reducing allocation and memory-copy costs across fan-out and retry paths. by @danrusei in fe506e4

* **Reliable-path wakeup and lock reduction** (#206, #207) — Removed extra `Notify` indirection and helper wakeups in reliable dispatch, and replaced mutex-heavy hot-path checks with lighter coordination where safe. This reduces background work and contention in reliable subscription flows. by @danrusei in 3b3550c and cc353f1
<!-- v0.8.1 END -->

<!-- v0.8.0 START -->
## v0.8.0 - 2026-03-02

🚀 **Embedded Raft Consensus — etcd Removed**

Danube no longer depends on an external etcd cluster for metadata storage. All metadata is now managed by an embedded Raft consensus layer built on [openraft](https://github.com/databendlabs/openraft) with [redb](https://github.com/cberner/redb) as the persistent log store. This eliminates an entire operational dependency, reduces deployment complexity, and improves latency by keeping metadata reads local to each broker.

### 🏗️ Raft Metadata Engine

* **Embedded Raft consensus** (#199) — Replaced etcd with an in-process Raft state machine (`danube-raft` crate). Each broker participates as a Raft voter; reads are served from local state, writes are proposed through the Raft leader. Persistent storage uses redb (single-file, zero-config embedded database). by @danrusei in d015e18

* **Restart resilience** (#200) — Brokers detect restarts via persisted `node_id` and rejoin the existing Raft group automatically, preserving cluster membership across pod restarts and rolling upgrades. by @danrusei in d9cb9a2

### 📈 Cluster Scaling

* **Auto-detection of existing clusters** (#202) — During peer discovery, fresh nodes check whether seed peers already have an elected Raft leader. If so, the node enters join mode automatically (registers as "drained"), eliminating the need for shell wrappers or init-containers in Kubernetes. by @danrusei in a4abe78

* **Raft cluster management via CLI & MCP** (#201) — Added `danube-admin cluster add-node`, `remove-node`, and `promote-node` commands (CLI + MCP tools) for managing Raft membership during scale-up and scale-down operations. by @danrusei in a266c8d

<!-- v0.8.0 END -->

<!-- v0.7.3 START -->
## v0.7.3 - 2026-02-15

### What's Changed

* **Proxy-mode connectivity** (#195) - Brokers now advertise separate `broker_url` (internal identity) and `connect_url` (client-facing proxy address) via the new `advertised_listeners` config section. When a gRPC proxy or Kubernetes ingress sits in front of the cluster, the client automatically inserts an `x-danube-broker-url` metadata header on every RPC so the proxy can route to the correct backend broker. by @danrusei in 807317a
<!-- v0.7.3 END -->

<!-- v0.7.2 START -->
## v0.7.2 - 2026-02-09

### 🏗️ Broker Internals

* **Dispatcher refactor** (#194) - Consolidated the 3-layer enum hierarchy (`Dispatcher` → `ExclusiveDispatcher`/`SharedDispatcher` → leaf variants) into a single flat `Dispatcher` struct with four factory constructors. Eliminates ~300 lines of duplicated facade methods and match delegation. Pure structural change with no behavior change. by @danrusei in fbab80d

* **Background subscription cleanup** (#193) - Moved expired subscription removal to a background task instead of performing it inline during packet dispatch, reducing latency on the hot path. by @danrusei in 6df5816

### 📦 Client Improvements

* **Builders return Result instead of panicking** (#190) - `DanubeClient`, `ProducerBuilder`, and `ConsumerBuilder` now return `Result` from `.build()` instead of panicking on misconfiguration. by @danrusei in 7affa8c

* **Centralized retry/backoff logic** (#190) - Unified retry strategy across producers and consumers, replacing scattered inline retry loops. by @danrusei in 7affa8c

* **State machines for TopicConsumer/TopicProducer** (#190) - Refactored connection lifecycle into explicit state machines for clearer error handling and reconnection behavior. by @danrusei in 7affa8c

* **Schema registry ergonomics** (#190, #191) - Consolidated schema registry operations within the Danube client and removed references to `ErrorType`/`ErrorMessage` in favor of idiomatic error handling. by @danrusei in 7affa8c, 8c5df69

* **Client resilience fixes** (#192) - Resolve operations now return errors instead of panicking; consumers read the `stop_signal` field from health checks for graceful shutdown. by @danrusei in c6c9f8c
<!-- v0.7.2 END -->

<!-- v0.7.1 START -->
## v0.7.1 - 2026-02-02

🎉 **AI-Native Cluster Management**

Danube now integrates with AI assistants via Model Context Protocol (MCP), enabling natural language cluster management. We've also consolidated admin binaries into a unified tool.

📖 **Read more**: [AI-Native Messaging: Managing Danube with Natural Language](https://dev-state.com/posts/ai_native_messaging_with_danube/)

### 🤖 AI & MCP Integration

* **Model Context Protocol (MCP) Server** (#185) - 40+ tools and 7 guided prompts for natural language cluster management through Claude Desktop, Windsurf, VSCode, and other MCP-compatible IDEs. by @danrusei in 7c9bc2a

* **Enhanced Tool Documentation** (#187) - Detailed MCP tool descriptions with usage guidance, parameter explanations, and configuration requirements. by @danrusei in d2b3979

### 🔧 Binary Consolidation

* **Unified danube-admin** (#184) - Merged CLI and gateway into single binary with three modes (CLI, HTTP server, MCP). 40% smaller, ~400 lines of duplicate code eliminated. by @danrusei in f534b77

### 🐛 Bug Fixes

* Allow manual rebalancing when auto-rebalance is disabled (#189) by @danrusei in 680dc88
* Fixed topic distribution calculation affecting load balancing (#186) by @danrusei in ecae3a9
* Simplified Docker folder structure by @danrusei in b953138

<!-- v0.7.1 END -->

<!-- v0.7.0 START -->
## v0.7.0 - 2026-01-27

### 🎯 Major Features

* **Automated Cluster Rebalancing** (#182) - Proactive rebalancing using Coefficient of Variation (CV) metrics with configurable aggressiveness levels (Conservative/Balanced/Aggressive), rate limiting, cooldown periods, and topic blacklist support. Automatically moves topics between brokers to maintain optimal cluster balance. by @danrusei in b9bafbd

* **Intelligent Ranking Algorithms** (#181, #183) - Three topic assignment strategies: Fair (topic count), Balanced (multi-factor scoring with topic load + CPU + memory), and WeightedLoad (adaptive bottleneck detection). Topic load now considers message rate, throughput, connections, and backlog for smarter broker selection. by @danrusei in 85b896c, ae1a0b3

* **Cross-Platform Resource Monitoring** (#180) - System resource monitoring with automatic container detection (Docker/K8s) and cgroup-aware metrics. Tracks CPU, memory, disk I/O, and network I/O in real-time for accurate load calculations. by @danrusei in 7eeda83
<!-- v0.7.0 END -->

<!-- v0.6.2 START -->
## v0.6.2 - 2026-01-22

### What's Changed

* eliminate pending, final copy of the cloud object (#178) by @danrusei in 81d3b6f
* [BUG] reliable topic move to another broker (#177) by @danrusei in 7b1e9cc
* [BUG] delete partitioned topics across brokers (#176) by @danrusei in e6b65ef
* [BUG] allow to create either partitioned topic or normal topic against the same base. (#176) by @danrusei in e6b65ef
* standardize tracing usage and the logging structure (#175) by @danrusei in 5d7dea2
<!-- v0.6.2 END -->

<!-- v0.6.1 START -->
## v0.6.1 - 2026-01-06

This release completes the Schema Registry implementation with distributed schema ID generation, producer-level validation, and topic-level schema governance.

### 🔧 Schema Registry Enhancements

* **Distributed Schema ID Generation** - Replaced local counters with ETCD-based atomic counters, eliminating ID collisions in multi-broker environments. Added reverse index (`schema_id → subject`) for efficient consumer schema lookups via `get_schema_by_id()`.

* **Producer Schema Validation** - Enhanced producer creation to validate schema subjects. First producer assigns schema to topic; subsequent producers must use the same subject. Exported `SchemaRegistrationBuilder` from `danube-client` for improved API ergonomics.

* **Topic-Level Schema Governance** - Implemented admin APIs (`ConfigureTopicSchema`, `UpdateTopicValidationPolicy`, `GetTopicSchemaConfig`) enabling per-topic validation policies. Multiple topics can share the same schema subject while enforcing different validation rules (e.g., `Warn` for dev, `Enforce` for prod). All configurations persist to ETCD.

**Full details**: Schema registry improvements (#168) by @danrusei in 6d0ade1
<!-- v0.6.1 END -->

<!-- v0.6.0 START -->
## v0.6.0 - 2026-01-04

This release introduces the **Schema Registry** with full compatibility checking, reliability improvements for at-least-once delivery guarantees, and significant refactoring for maintainability.

### 🎯 Major Features

* **Schema Registry with JSON Schema Compatibility** (#166) - Implemented backward, forward, and full compatibility checking for JSON Schema in `danube-broker/src/schema/json/`. Complements existing Avro support. by @danrusei in 81ddac7

* **Complete Schema Registry Implementation** (#165) - Added centralized schema management with versioning, compatibility enforcement (Backward/Forward/Full/None modes), and support for JSON Schema, Avro, and Protobuf. See [Schema Registry Architecture](https://danube-messaging.com/architecture/schema-registry/) for details. by @danrusei in 6c6571a

### 🔧 Reliability & Performance

* **Hybrid Lag Monitor for At-Least-Once Delivery** (#162) - Implemented a dual-path dispatch system combining fast notification-based delivery with a 500ms heartbeat watchdog. Ensures reliable message delivery even when `tokio::Notify` coalescing, subscription races, consumer disconnects, or storage lags occur. Guarantees at-least-once delivery semantics. by @danrusei in 6f334b1

### 🏗️ Refactoring & Code Quality

* **Dispatcher Refactoring** (#164) - Restructured `danube-broker/src/dispatcher/` for improved readability, maintainability, and separation of concerns. by @danrusei in a05c5de

* **Consumer Lifecycle Simplification** (#161) - Introduced `ConsumerSession` to manage consumer state and lifecycle, simplifying connection handling and session management in `danube-broker/src/consumer.rs`. by @danrusei in ef70cd0

<!-- v0.6.0 END -->

<!-- v0.5.2 START -->
## v0.5.2 - 2025-11-22

### What's Changed

* include danube-admin-gateway in the release by @danrusei in 19a7c90
* get topic lists from admin grpc instead scrapping from prom (#157) by @danrusei in ea6340f
* reorder the list of topics to broker by @danrusei in a631895
* added cluster/topic actions for danube-admin-gateway (#156) by @danrusei in 280ae0a
* implement topics and namespaces endpoints for danube-admin-gateway (#155) by @danrusei in d4b358a
* Use prometheus instead of manually scraping the metrics (#154) by @danrusei in d9e3998
* refactored danube-admin-gateway, additional broker details added on registering (#153) by @danrusei in 9033c7b
* danube-admin-gateway crate created, is a BFF service that provides a unified HTTP/JSON API for the Danube Admin UI (#152) by @danrusei in 38aeef3
* update the arm64 github runner by @danrusei in 8a0248f
<!-- v0.5.2 END -->

<!-- v0.5.1 START -->
## v0.5.1 - 2025-11-04

### What's Changed

* Updated dependencies (#148) by @Dan Rusei in dfe51dc
* Extended metrics (#147) by @Dan Rusei in e831577
* Implemented producer and subscriptions policies (#145) by @Dan Rusei in 772d3cf
* Moved broker watcher out from danube service (#143) by @Dan Rusei in c78e1e3
* Unload a broker from the cluster for maintenance (#142) by @Dan Rusei in 368e0b1
* Unload topic from the broker, auto relocate to another available broker in the cluster (#141) by @Dan Rusei in 6f6463d
* Improved topic deletion logic (#140) by @Dan Rusei in 7f637e2
* Refactored the Broker Service, adding TopicAdmin and TopicCluster (#139) by @Dan Rusei in a4fc1c0
<!-- v0.5.1 END -->

<!-- v0.5.0 START -->
## v0.5.0

### What's Changed

* The major release, implemented the danube persistance layer
<!-- v0.5.0 END -->
