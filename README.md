# ![D from Danube](Danube_logo_2.png) Danube

**A lightweight and scalable Cloud-Native Messaging Platform with Cloud Object Storage (S3/GCS/Azure)**

Danube is an open-source distributed messaging broker platform inspired by Apache Pulsar, designed to be cloud-native and cost-effective. Built with a Write-Ahead Log (WAL) architecture and persistent object storage integration, Danube delivers sub-second dispatch with cloud economics.

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
curl -O https://raw.githubusercontent.com/danube-messaging/danube/main/docker/docker-compose.yml

curl -O https://raw.githubusercontent.com/danube-messaging/danube/main/docker/danube_broker.yml
```

Start the danube cluster:

```bash
docker-compose up -d
```

This launches a complete Danube cluster with:
- **2 High-Availability Brokers** for topics failover
- **ETCD** for distributed metadata management  
- **MinIO S3-Compatible Storage** for cloud-ready persistence
- **Automatic bucket creation** and configuration

**Test the setup:**

### Produce messages with reliable delivery

```bash
docker exec -it danube-cli danube-cli produce \
  --service-addr http://broker1:6650 \
  --topic "/default/persistent-topic" \
  --count 100 \
  --message "Persistent message" \
  --reliable
```

### Consume messages from the topic

```bash
docker exec -it danube-cli danube-cli consume \
  --service-addr http://broker1:6650 \
  --topic "/default/persistent-topic" \
  --subscription "persistent-sub" \
  --sub-type exclusive
```

📦 Run with Docker (choose one):
- **Minimal stack (brokers + dependencies)**: [docker/README.md](docker/README.md)
- **Stack with Admin UI (UI + gateway + Prometheus)**: [docker/danube_with_ui/README.md](docker/danube_with_ui/README.md)

📖 **[Complete Docker Setup Guide →](docker/README.md)**

## Architecture

### 🏗️ **Cluster & Broker Characteristics**
- **Stateless brokers**: Metadata in ETCD and data in WAL/Object Storage
- **Horizontal scaling**: Add brokers in seconds; partitions rebalance automatically
- **Leader election & HA**: Automatic failover and coordination via ETCD
- **Rolling upgrades**: Restart or replace brokers with minimal disruption
- **Multi-tenancy**: Isolated namespaces with policy controls
- **Security-ready**: TLS/mTLS support in Admin and data paths

**Cloud-Native by Design** - Danube's architecture separates compute from storage, enabling:

### 🌩️ **Write-Ahead Log + Cloud Persistence**
- **Sub-millisecond producer acknowledgments** via local WAL
- **Asynchronous background uploads** to S3/GCS/Azure object storage
- **Automatic failover** with shared cloud state
- **Infinite retention** without local disk constraints

### ⚡ **Performance & Scalability**
- **Hot path optimization**: Messages served from in-memory WAL cache
- **Stream per subscription**: WAL + cloud storage from selected offset 
- **Multi-cloud support**: AWS S3, Google Cloud Storage, Azure Blob, MinIO

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

![Producers Consumers](https://danube-docs.dev-state.com/architecture/img/producers_consumers.png "Producers Consumers")

### 🛠️ **Developer Experience**
- **Multi-language clients**: [Rust](https://crates.io/crates/danube-client), [Go](https://pkg.go.dev/github.com/danrusei/danube-go)
- **[CLI Tools](danube-cli/)**: Message publishing and consumption
- **[Admin CLI](danube-admin-cli/)**: Cluster, namespace, and topic management

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
- **[danube-admin-gateway](danube-admin-gateway/)** - HTTP/JSON BFF for the Admin UI

---
