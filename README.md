# 🌊 Danube Messaging

**A self-contained, lightweight Cloud-Native Messaging Platform, built in Rust**

Danube is an open-source distributed messaging broker platform, designed to be cloud-native and cost-effective. It features embedded Raft consensus for metadata replication, built on Tokio and openraft. Paired with a Write-Ahead Log (WAL) architecture and cloud object storage, Danube delivers sub-second dispatch with operational simplicity and cloud economics.

[![Documentation](https://img.shields.io/badge/📑-Documentation-blue)](https://danube-docs.dev-state.com/)
[![Docker](https://img.shields.io/badge/🐳-Docker%20Ready-2496ED)](https://github.com/danube-messaging/danube/tree/main/docker)
[![Rust](https://img.shields.io/badge/🦀-Rust-000000)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/📜-Apache%202.0-green)](LICENSE)

## 🚀 Get Started with Danube

**Quick Start with Docker Compose** - Deploy a cluster in seconds:

Create a directory and download the required files:

```bash
mkdir danube-docker && cd danube-docker
```

Download the docker-compose and broker configuration file:

```bash
curl -O https://raw.githubusercontent.com/danube-messaging/danube/main/docker/quickstart/docker-compose.yml

curl -O https://raw.githubusercontent.com/danube-messaging/danube/main/docker/danube_broker.yml
```

Start the danube cluster:

```bash
docker-compose up -d
```

This launches a complete Danube cluster with:

- **3 High-Availability Brokers** for topics failover
- **Prometheus** for monitoring
- **danube-cli** to produce and consume messages

**Test the setup:**

### Produce messages with reliable delivery

```bash
docker-compose exec -it danube-cli danube-cli produce \
  --service-addr http://broker1:6650 \
  --topic "/default/persistent-topic" \
  --count 100 \
  --message "Persistent message" \
  --reliable
```

### Consume messages from the topic

```bash
docker-compose exec -it danube-cli danube-cli consume \
  --service-addr http://broker1:6650 \
  --topic "/default/persistent-topic" \
  --subscription "persistent-sub" \
  --sub-type exclusive
```

📖 **[Complete Docker Setup Guide →](docker/README.md)**

## Architecture

### 🏗️ **Cluster & Broker Characteristics**

- **Embedded Raft consensus**: Metadata replicated across brokers via openraft — no ETCD, no ZooKeeper, no external dependencies
- **Horizontal scaling**: Add brokers in seconds with zero-downtime expansion
- **Intelligent load balancing**: Automatic topic placement and rebalancing across brokers
- **Broker resilience**: Automatic leader election, failover, and topic reconciliation on restart
- **Security-ready**: TLS/mTLS support in Admin and data paths

### 🌩️ **Write-Ahead Log + Cloud Persistence**

- **Cloud-Native by Design** - Danube's architecture separates compute from storage
- **Multi-cloud support**: AWS S3, Google Cloud Storage, Azure Blob, MinIO
- **Hot path optimization**: Messages served from in-memory WAL cache
- **Stream per subscription**: WAL + cloud storage from selected offset
- **Asynchronous background uploads** to S3/GCS/Azure object storage

### 🎯 **Intelligent Load Management**

- **Automated rebalancing**: Detects cluster imbalances and redistributes topics automatically
- **Smart topic assignment**: Places new topics on least-loaded brokers using configurable strategies
- **Resource monitoring**: Tracks CPU, memory, throughput, and backlog per broker in real-time
- **Configurable policies**: Conservative, balanced, or aggressive rebalancing based on workload
- **Graceful topic migration**: Moves topics between brokers

## Core Capabilities

### 📨 **Message Delivery**

- **[Topics](https://danube-docs.dev-state.com/architecture/topics/)**: Partitioned and non-partitioned with automatic load balancing
- **[Reliable Dispatch](https://danube-docs.dev-state.com/architecture/dispatch_strategy/)**: At-least-once delivery with configurable storage backends
- **Non-Reliable Dispatch**: High-throughput, low-latency for real-time scenarios

### 🔄 **Subscription Models**

- **[Exclusive](https://danube-docs.dev-state.com/architecture/subscriptions/)**: Single consumer per subscription
- **Shared**: Load-balanced message distribution across consumers
- **Failover**: Automatic consumer failover with ordered delivery

### 📋 **Schema Registry**

- **Centralized schema management**: Single source of truth for message schemas across all topics
- **Schema versioning**: Automatic version tracking with compatibility enforcement
- **Multiple formats**: Bytes, String, Number, JSON Schema, Avro, Protobuf
- **Validation & governance**: Prevent invalid messages and ensure data quality

### 🤖 **AI-Powered Administration**

Danube features **the AI-native messaging platform administration** through the Model Context Protocol (MCP):

- **Natural language cluster management**: Manage your cluster by talking to AI assistants (Claude, Cursor, Windsurf)
- **40 intelligent tools**: Full cluster operations accessible via AI - topics, schemas, brokers, diagnostics, metrics
- **Automated troubleshooting**: AI-guided workflows for consumer lag analysis, health checks, and performance optimization
- **Multiple interfaces**: CLI commands, Web UI, or AI conversation - your choice

**Example**: Ask Claude *"What's the cluster balance?"* or *"Create a partitioned topic for analytics"* and watch it happen.

## Danube Clients

### [Official Clients](https://danube-docs.dev-state.com/client_libraries/clients/)

- **[Rust Client](https://crates.io/crates/danube-client)** - learn more Rust [examples](danube-client/examples/)
- **[Go Client](https://pkg.go.dev/github.com/danrusei/danube-go)** - learn more Go [examples](https://github.com/danube-messaging/danube-go/tree/main/examples)
- **[Java Client](https://central.sonatype.com/namespace/com.danube-messaging)** - learn more Java [examples](https://github.com/danube-messaging/danube-java/tree/main/examples)
- **[Python Client](https://pypi.org/project/danube-client/)** - learn more Python [examples](https://github.com/danube-messaging/danube-py/tree/main/examples)

### Community Contributions

Contributions in **NodeJs**, **C / C++ / C#**, **Ruby**, and other languages are welcome! Join our growing ecosystem.

## Development & Contribution

**Get involved** - Danube is actively developed with new features added regularly.

**[🐛 Report Issues](https://github.com/danube-messaging/danube/issues)** | **[💡 Request Features](https://github.com/danube-messaging/danube/issues/new)** | **[📖 Development Guide](https://danube-docs.dev-state.com/development/dev_environment/)**

### Project Structure

- **[danube-broker](danube-broker/)** - Core messaging platform
- **[danube-persistent-storage](danube-persistent-storage/)** - WAL and cloud storage integration
- **[danube-client](danube-client/)** - Async Rust client library  
- **[danube-cli](danube-cli/)** - Command-line producer/consumer tools
- **[danube-admin](danube-admin/)** - Unified admin tool (CLI + AI/MCP + Web UI)
