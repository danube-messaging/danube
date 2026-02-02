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

* **Complete Schema Registry Implementation** (#165) - Added centralized schema management with versioning, compatibility enforcement (Backward/Forward/Full/None modes), and support for JSON Schema, Avro, and Protobuf. See [Schema Registry Architecture](https://danube-docs.dev-state.com/architecture/schema_registry_architecture/) for details. by @danrusei in 6c6571a

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
