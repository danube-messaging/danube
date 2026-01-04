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
