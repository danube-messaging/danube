# ![D from Danube](Danube_logo_2.png) Danube

**A lightweight and scalable Cloud-Native Messaging Platform with S3-Compatible Storage**

Danube is an open-source distributed messaging broker platform inspired by Apache Pulsar, designed to be cloud-native and cost-effective. Built with a Write-Ahead Log (WAL) architecture and persistent object storage integration, Danube delivers sub-second dispatch with cloud economics.

[![Documentation](https://img.shields.io/badge/📑-Documentation-blue)](https://danube-docs.dev-state.com/)
[![Docker](https://img.shields.io/badge/🐳-Docker%20Ready-2496ED)](https://github.com/danube-messaging/danube/tree/main/docker)
[![Rust](https://img.shields.io/badge/🦀-Rust-000000)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/📜-Apache%202.0-green)](LICENSE)

## 🚀 Get Started with Danube

**Quick Start with Docker Compose** - Deploy a cluster in seconds:

```bash
# Clone and start the cluster
git clone https://github.com/danube-messaging/danube.git
cd danube/docker
docker-compose up -d
```

This launches a complete Danube cluster with:
- **2 High-Availability Brokers** for topics failover
- **ETCD** for distributed metadata management  
- **MinIO S3-Compatible Storage** for cloud-ready persistence
- **Automatic bucket creation** and configuration

**Test the setup:**
```bash
# Build the CLI
cd ../danube-cli && cargo build --release

# Produce messages with reliable delivery
../target/release/danube-cli produce \
  --service-addr http://localhost:6650 \
  --topic "/default/test-topic" \
  --count 100 --message "Hello Danube!" --reliable

# Consume messages from the topic
../target/release/danube-cli consume \
  --service-addr http://localhost:6650 \
  --topic "/default/test-topic" \
  --subscription "my-sub"
```

📖 **[Complete Docker Setup Guide →](docker/README.md)**

## Architecture

**Cloud-Native by Design** - Danube's architecture separates compute from storage, enabling:

### 🌩️ **Write-Ahead Log + Cloud Persistence**
- **Sub-millisecond producer acknowledgments** via local WAL
- **Asynchronous background uploads** to S3/GCS/Azure object storage
- **Automatic failover** with shared cloud state
- **Infinite retention** without local disk constraints

### ⚡ **Performance & Scalability**
- **Hot path optimization**: Messages served from in-memory WAL cache
- **Horizontal scaling**: Add brokers in seconds
- **Multi-cloud support**: AWS S3, Google Cloud Storage, Azure Blob (soon), MinIO

### 🔄 **Intelligent Data Management**
- **Automatic tiering**: Hot data in WAL, cold data in object storage
- **Background compaction**: Efficient storage utilization
- **Stream per topic**: WAL + cloud storage from selected offset 
- **Cross-region replication**: Built-in disaster recovery


```
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐
│  Producers  │───▶│ Danube WAL   │───▶│ Object Storage  │
└─────────────┘    │ (Sub-ms ACK) │    │ (S3/GCS/Azure)  │
                   └──────────────┘    └─────────────────┘
                          │                      ▲
                          ▼                      │
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐
│  Consumers  │◀───│ Stream Reader│◀───│ Background      │
└─────────────┘    │ (WAL + Cloud)│    │ Uploader        │
                   └──────────────┘    └─────────────────┘
```

## Core Capabilities

### 📨 **Message Delivery**
- **[Topics](https://danube-docs.dev-state.com/architecture/topics/)**: Partitioned and non-partitioned with automatic load balancing
- **[Reliable Dispatch](https://danube-docs.dev-state.com/architecture/dispatch_strategy/)**: At-least-once delivery with configurable storage backends
- **Non-Reliable Dispatch**: High-throughput, low-latency for real-time scenarios
- **Flexible Schemas**: Bytes, String, Int64, JSON with automatic serialization

### 🔄 **Subscription Models**
- **[Exclusive](https://danube-docs.dev-state.com/architecture/subscriptions/)**: Single consumer per subscription
- **Shared**: Load-balanced message distribution across consumers
- **Failover**: Automatic consumer failover with ordered delivery

### 🛠️ **Developer Experience**
- **Multi-language clients**: [Rust](https://crates.io/crates/danube-client), [Go](https://pkg.go.dev/github.com/danrusei/danube-go)
- **[CLI Tools](danube-cli/)**: Message publishing and consumption
- **[Admin CLI](danube-admin-cli/)**: Cluster, namespace, and topic management

![Producers Consumers](https://danube-docs.dev-state.com/architecture/img/producers_consumers.png "Producers Consumers")

## Community & Clients

### Official Clients
- **[Rust Client](https://crates.io/crates/danube-client)** - Full-featured async client with [examples](danube-client/examples/)
- **[Go Client](https://pkg.go.dev/github.com/danrusei/danube-go)** - Production-ready client with [examples](https://github.com/danrusei/danube-go/tree/main/examples)

### Community Contributions
Contributions in **Python**, **Java**, **JavaScript**, and other languages are welcome! Join our growing ecosystem.

## Development & Contribution

**Get involved** - Danube is actively developed with new features added regularly.

**[🐛 Report Issues](https://github.com/danube-messaging/danube/issues)** | **[💡 Request Features](https://github.com/danube-messaging/danube/issues/new)** | **[📖 Development Guide](https://danube-docs.dev-state.com/development/dev_environment/)**

### Project Structure
- **[danube-broker](danube-broker/)** - Core messaging platform
- **[danube-persistent-storage](danube-persistent-storage/)** - WAL and cloud storage integration
- **[danube-client](danube-client/)** - Async Rust client library  
- **[danube-cli](danube-cli/)** - Command-line producer/consumer tools
- **[danube-admin-cli](danube-admin-cli/)** - Cluster management utilities

---
