# 🌊 Danube Messaging

**A lightweight, cloud-native messaging platform built in Rust**

[![Documentation](https://img.shields.io/badge/📑-Documentation-blue)](https://danube-messaging.com/)
[![Docker](https://img.shields.io/badge/🐳-Docker%20Ready-2496ED)](https://github.com/danube-messaging/danube/tree/main/docker)
[![Rust](https://img.shields.io/badge/🦀-Rust-000000)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/📜-Apache%202.0-green)](LICENSE)

Danube is an open-source messaging platform built in Rust for teams that need reliable pub/sub and streaming without the operational overhead. Built on [Tokio](https://tokio.rs/) and [openraft](https://github.com/databendlabs/openraft), metadata is replicated through embedded Raft consensus, so there are no external dependencies to deploy or manage. Run it as a single-node **standalone** broker, scale to a multi-node **cluster**, or deploy at the **edge** to ingest MQTT device data into the cloud, all from the same binary.

📖 **Full documentation at [danube-messaging.com](https://danube-messaging.com/)**

---

## Try It in minutes

Download the latest binary from the [releases page](https://github.com/danube-messaging/danube/releases) or pull the container image:

```bash
# Binary
danube-broker --mode standalone --data-dir ./danube-data

# Or Docker
docker run -p 6650:6650 -p 50051:50051 \
  ghcr.io/danube-messaging/danube-broker:latest \
  --mode standalone --data-dir /data \
  --broker-addr 0.0.0.0:6650 --admin-addr 0.0.0.0:50051
```

That's it. Broker on `127.0.0.1:6650`, admin on `127.0.0.1:50051`, no config file needed.

Test with the CLI:

```bash
# Terminal 1: produce
danube-cli produce -s http://127.0.0.1:6650 -t /default/demo -c 10 -m "Hello, Danube!"

# Terminal 2: consume
danube-cli consume -s http://127.0.0.1:6650 -t /default/demo -m my_sub
```

Or use any of the [client libraries](#client-libraries) (Rust, Go, Java, Python).

---

## Three Deployment Modes

Danube runs as a single binary in three modes. Choose the one that fits your use case:

### 🖥️ Standalone

A single self-contained broker. Zero config, zero dependencies. Ideal for development, CI, and single-server deployments.

```bash
danube-broker --mode standalone --data-dir ./danube-data
```

### 🌐 Cluster

Multiple brokers forming a Raft consensus group with automated topic distribution, leader election, and load-based rebalancing. The recommended mode for production.

```bash
danube-broker --config-file danube_broker.yml \
  --broker-addr 0.0.0.0:6650 --raft-addr 0.0.0.0:7650 \
  --data-dir ./data/raft --seed-nodes "node1:7650,node2:7650,node3:7650"
```

Deploy with [Docker Compose](docker/README.md) or [Kubernetes + Helm](https://danube-messaging.com/getting-started/kubernetes/).

### 🏭 Edge

A lightweight MQTT gateway that ingests data from IoT devices at the edge and replicates it to the central cluster. Devices publish via standard MQTT; the edge broker validates payloads against schemas, buffers into a local WAL, and continuously replicates to the cloud.

```bash
danube-broker --mode edge --data-dir ./edge-data --edge-config edge.yaml
```

```
MQTT devices ──► Edge broker ──► Local WAL ──► Cluster
                  (MQTT v3.1/v5)   (survives     (gRPC
                                    outages)     replication)
```

Edge mode is designed for factory floors, remote sites, and any environment where constrained devices speak MQTT and need resilient data delivery to a central platform.

📖 **[Broker Modes documentation](https://danube-messaging.com/getting-started/broker-modes/)**

---

## Key Features

📨 **Messaging** : Topics (partitioned / non-partitioned), reliable (at-least-once) and non-reliable dispatch, dead-letter queues

🔄 **Subscriptions** : Exclusive, Shared, Failover, and Key-Shared (per-key ordering via consistent hashing)

💾 **Storage** : Local WAL, shared filesystem, or S3/GCS/Azure object store with tiered replay

📋 **Schema Registry** : JSON Schema, Avro, Protobuf with versioning and compatibility enforcement

🔒 **Security** : TLS/mTLS, JWT, API-key auth, RBAC with default-deny

🏗️ **Cluster** : Embedded Raft consensus, automated rebalancing, zero-downtime scaling

🏭 **Edge** : MQTT v3.1.1/v5.0 ingestion, schema validation at the edge, WAL-buffered replication

🔌 **[Danube Connect](https://danube-messaging.com/integrations/danube-connect/)** : Out-of-process connector ecosystem for databases, analytics, and IoT (MQTT, Delta Lake, Qdrant, SurrealDB, and more)

🤖 **AI Admin** : [MCP integration](https://danube-messaging.com/admin/mcp/) for managing your cluster with natural language via Claude, Cursor, or Windsurf

📖 **Learn more** : [Topics](https://danube-messaging.com/concepts/topics/) · [Subscriptions](https://danube-messaging.com/concepts/subscriptions/) · [Persistence](https://danube-messaging.com/concepts/persistence/) · [Security](https://danube-messaging.com/security/overview/) · [Architecture](https://danube-messaging.com/architecture/overview/)

---

## Client Libraries

- **Rust** : [danube-client](https://crates.io/crates/danube-client) · [examples](danube-client/examples/)
- **Go** : [danube-go](https://pkg.go.dev/github.com/danube-messaging/danube-go) · [examples](https://github.com/danube-messaging/danube-go/tree/main/examples)
- **Java** : [danube-java](https://central.sonatype.com/namespace/com.danube-messaging) · [examples](https://github.com/danube-messaging/danube-java/tree/main/examples)
- **Python** : [danube-client](https://pypi.org/project/danube-client/) · [examples](https://github.com/danube-messaging/danube-py/tree/main/examples)

## Tools

- **[danube-cli](https://danube-messaging.com/tools/danube-cli/)** : Command-line producer and consumer
- **[danube-admin](https://danube-messaging.com/admin/overview/)** : Cluster administration (CLI, AI/MCP, Web UI)

## Project Structure

- **[danube-broker](danube-broker/)** : Core messaging broker
- **[danube-edge](danube-edge/)** : Edge MQTT gateway and replicator
- **[danube-schema](danube-schema/)** : Schema registry (JSON Schema, Avro, Protobuf)
- **[danube-raft](danube-raft/)** : Embedded Raft consensus
- **[danube-persistent-storage](danube-persistent-storage/)** : WAL and durable storage engine
- **[danube-client](danube-client/)** : Async Rust client library
- **[danube-cli](danube-cli/)** : Command-line producer/consumer
- **[danube-admin](danube-admin/)** : Unified admin tool

## Contributing

Danube is actively developed with new features added regularly. See the [contribution guide](https://danube-messaging.com/contributing/) for how to set up a local development environment, run tests, and submit pull requests.

**[🐛 Report Issues](https://github.com/danube-messaging/danube/issues)** · **[💡 Request Features](https://github.com/danube-messaging/danube/issues/new)** · **[📖 Development Guide](https://danube-messaging.com/contributing/)**
