# Danube Messaging: Positioning & Technical Architecture

## 1. Summary & Identity

**Danube** is a distributed, lightweight streaming and messaging platform built in Rust. It was designed from first principles to bridge the gap between high-throughput real-time messaging, operational simplicity, edge computing, and modern cloud-native lakehouses.

Built on [Tokio](https://tokio.rs/) and [openraft](https://github.com/databendlabs/openraft), Danube eliminates the operational bloat that historically characterized streaming platforms:
* **Self-Contained Embedded Consensus:** Embedded Raft with `redb` replaces external coordination services like ZooKeeper or etcd.
* **Dual-Dispatch Engine:** Native support for both **Non-Reliable** (in-memory fire-and-forget) and **Reliable** (WAL + Cloud storage at-least-once) topics running side-by-side in the same cluster.
* **Decoupled Compute & Storage:** Brokers act as lightweight routing engines with a local Write-Ahead Log (WAL) that offload sealed historical segments directly to Cloud Object Storage (S3, GCS, Azure Blob).
* **Lakehouse-Native:** Sealed segments are continuously converted into typed, queryable Apache Iceberg tables with automated schema evolution.
* **One Binary, Three Operational Modes:** The same binary deploys as **Standalone** (local dev), **Cluster** (high-availability production), or **Edge** (IoT ingestion with MQTT).
* **Predictable Systems Performance:** Memory footprint is measured in megabytes, with no JVM garbage collection pauses, warm-up times, or complex heap tuning.

---

## 2. Defining Capabilities: How Danube Delivers an Edge

Rather than replicating existing systems, Danube is architected around specific design decisions that provide distinct operational and architectural advantages over traditional streaming platforms:

### 1. Unified Dual-Dispatch Engine (Non-Reliable & Reliable Side-by-Side)
* **How Danube does it:** Danube natively supports two distinct dispatch strategies configured on a per-topic basis within the same cluster:
  * **Non-Reliable (Fire-and-Forget / In-Memory):** Bypasses disk and storage entirely. Incoming messages flow directly from producers into in-memory consumer streams via Tokio async channels with zero disk I/O, no ACK gating, and sub-millisecond dispatch. Ideal for real-time telemetry, live metrics, sensor heartbeats, and high-frequency ephemeral data.
  * **Reliable (At-Least-Once / Durable):** Messages are appended to the local WAL, tracked with cursor progress per subscription, pipelined with flow control (`max_unacked_messages`), protected with exponential NACK backoff and Dead Letter Queues (DLQ), and asynchronously exported to Cloud Storage for historical replay.
  * Crucially, both dispatch strategies share the same client connections, namespaces, security policies, and subscription patterns (`Exclusive`, `Shared`, `Failover`, `Key-Shared`).
* **The Industry Contrast:** In Apache Kafka, non-persistent topics do not exist—every message must hit a partitioned local disk log, incurring disk I/O even for disposable telemetry. In other architectures, teams are forced to operate two separate systems (e.g., Redis Pub/Sub for ephemeral metrics and Kafka/Pulsar for durable event logs). Danube consolidates both paradigms in a single cluster.

### 2. Instant Topic Mobility & Rebalancing (Decoupled Compute & Storage)
* **How Danube does it:** Brokers write recent reliable data to a fast local WAL, while a background exporter continuously seals completed segments and uploads them to Cloud Object Storage (S3/GCS/Azure). Because historical data lives in durable object storage rather than on broker-attached disks, topic reassignment is instantaneous (<100ms). When moving a topic between brokers during rebalancing, the old broker seals its active segment and the new broker claims ownership via Raft.
* **The Industry Contrast:** Traditional streaming platforms (such as Kafka or Redpanda) tie partition data strictly to local broker disks. Rebalancing an overloaded broker requires copying gigabytes of historical data across the network, saturating cluster bandwidth and taking hours to complete. In Danube, scaling and rebalancing are lightweight compute operations, not data-migration operations.

### 3. Native Lakehouse Integration (Zero-ETL Streaming to Iceberg)
* **How Danube does it:** Through `danube-iceberg`, Danube continuously converts streaming segments into Parquet files with ZSTD compression and registers them with an Apache Iceberg catalog (REST, AWS Glue, Polaris, Nessie). It resolves schemas from Danube's built-in Schema Registry (JSON Schema, Avro) to produce real, typed columns with per-column statistics (min/max bounds, null counts) and propagates evolving topic schemas automatically.
* **The Industry Contrast:** In traditional streaming architectures, getting data from topics into an analytical lakehouse requires designing, monitoring, and operating a multi-component ETL pipeline: Kafka Connect, Spark Streaming or Flink jobs, S3 sinks, and catalog synchronization. Danube makes streaming topics directly queryable by Trino, Spark, DuckDB, and Snowflake with zero ETL pipelines to maintain.

### 4. Key-Shared Subscriptions with Per-Key Ordering
* **How Danube does it:** Danube provides four distinct subscription models: `Exclusive`, `Failover`, `Shared`, and `Key-Shared`. Under Key-Shared dispatch, messages are distributed across a dynamic pool of parallel consumers using consistent hashing on routing keys. Messages sharing the same key are strictly guaranteed to route to the same consumer in order, while independent keys process concurrently in parallel across the consumer pool.
* **The Industry Contrast:** In partition-only systems (such as Kafka), consumer parallelism is strictly capped by the number of partitions (e.g., a topic with 4 partitions can have at most 4 active consumers in a consumer group). To increase parallelism, operators must manually increase partition counts—which can break hashing and message ordering. Danube decouples consumer concurrency from physical partitioning.

### 5. Edge-to-Cloud Continuum (One Binary, Three Modes)
* **How Danube does it:** Danube runs natively on resource-constrained devices in Edge Mode (`--mode edge`). The edge broker accepts standard MQTT v3.1.1/v5.0 device connections, validates payloads against schemas, buffers messages into a local disk WAL during network disconnections, and automatically replicates them to the central cloud cluster once connectivity is restored.
* **The Industry Contrast:** Enterprise streaming brokers are far too heavy (in memory, disk, and networking overhead) to deploy on factory floors, retail stores, or vehicles. Organizations are typically forced to assemble disparate edge brokers (e.g., Mosquitto) and write custom bridge applications to sync data to the cloud. Danube provides a single binary and a unified schema-aware messaging model from the edge to the cloud.

### 6. Self-Contained Embedded Governance (Zero External Daemons)
* **How Danube does it:** Cluster metadata, topic ownership, namespace access policies, and the Schema Registry are all embedded directly into the broker binary using `openraft` consensus backed by `redb`. Producers validate payloads against registered schemas (JSON Schema, Avro, Protobuf) with compatibility policies enforced at the broker boundary.
* **The Industry Contrast:** Traditional platforms often require operating separate auxiliary daemons and external databases for schema management (e.g., Confluent Schema Registry or Karapace) and external consensus clusters. Danube provides end-to-end data governance with zero external dependencies.

### 7. Predictable Resource Efficiency & Low Operational Overhead
* **How Danube does it:** Built in Rust using Tokio async I/O, zero-copy buffer slicing, and lock-free atomic topic contexts, Danube runs with a memory footprint measured in megabytes. It operates predictably under sustained throughput without GC pauses or thread-pool starvation.
* **The Industry Contrast:** Running JVM-based streaming systems requires managing heap allocations, garbage collection pauses (which cause spurious heartbeat timeouts and consumer rebalances), and high baseline memory costs. Danube is operationally lightweight and well-suited for containerized Kubernetes environments.

---

## 3. Danube vs. Apache Pulsar

Danube initially drew inspiration from several of Apache Pulsar's most powerful architectural concepts—notably its multi-tenant namespace model, tiered storage vision, and flexible subscription types (`Exclusive`, `Shared`, `Failover`, `Key-Shared`). However, Danube fundamentally departs from Pulsar in implementation, footprint, and operational complexity:

* **Operational Footprint (1 Binary vs. 3 Separate Clusters):** Operating Pulsar in production requires deploying and maintaining a ZooKeeper cluster for metadata, an Apache BookKeeper cluster for bookies/storage ledgers, and Pulsar broker instances (a minimum of 9+ JVM processes across multiple nodes). Danube collapses this entirely into a single, self-contained binary with embedded Raft consensus (`openraft` + `redb`) and zero external dependencies.
* **Storage Architecture & Lakehouse Integration:** Pulsar delegates all streaming storage to BookKeeper ledgers and requires Pulsar IO or external connectors to export data to cold storage. Danube writes to a fast local WAL, automatically exports sealed segments to Cloud Object Storage (S3/GCS/Azure), and natively converts them into queryable Apache Iceberg tables via `danube-iceberg`.
* **Runtime Efficiency & Predictability:** Pulsar runs on the JVM, requiring extensive heap sizing, garbage collection pause tuning, and substantial baseline memory overhead. Danube is written in native Rust on Tokio, operating with a memory footprint measured in megabytes, zero GC pauses, and instant startup times.
* **Edge Capabilities:** Pulsar's operational weight makes running it in edge or resource-constrained environments impractical. Danube includes a dedicated Edge Mode with native MQTT ingestion, local buffering, and cloud replication built directly into the same binary.

---

## 4. Danube vs. NATS

Because both Danube and NATS are modern, lightweight, single-binary systems that reject the JVM and heavy legacy architectures, developers frequently compare them. While they share an emphasis on speed, efficiency, and operational simplicity, their primary design targets and architectural paradigms differ substantially:

* **Routing Model & Primary Archetype:** NATS is fundamentally an ephemeral messaging fabric built around dynamic subject-based routing with token wildcards (`orders.eu.>`) and high-performance request-reply RPC. Danube is architected as a structured event-streaming platform organized around durable topics, namespaces, and explicit subscriber cursor management.
* **Data Lakehouse & Historical Analytics:** NATS JetStream provides an event log and key-value store, but has no native integration with analytical data lakes. Danube is lakehouse-native: it continuously maps streaming WAL segments into Parquet files and commits them to Apache Iceberg catalogs for direct SQL querying by Spark, Trino, and DuckDB.
* **Consumption Patterns & Ordering Guarantees:** NATS JetStream provides push and pull consumer groups. Danube provides Pulsar-style `Key-Shared` dispatch using consistent hashing, allowing parallel consumer pools to scale dynamically while guaranteeing strict sequential ordering per message key.
* **Data Governance & Schema Registry:** NATS is schema-agnostic, treating payloads as opaque byte buffers. Danube embeds a first-class Schema Registry (JSON Schema, Avro, Protobuf) with automatic compatibility validation and schema evolution across both streaming and lakehouse tiers.
* **Edge-to-Cloud Integration:** NATS links remote locations via Leaf Nodes (clustering over WAN). Danube Edge operates as an autonomous, schema-aware gateway with native MQTT device support, buffering data locally during disconnections and replicating to central storage.
