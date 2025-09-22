# Danube Docker Compose Setup

This directory contains a production-ready Docker Compose setup for the Danube messaging platform, demonstrating cloud-ready deployment with S3-compatible storage (MinIO) and distributed metadata management (ETCD).

## Architecture Overview

The setup includes:
- **2 Danube Brokers**: High-availability message brokers with load balancing
- **ETCD**: Distributed metadata store for cluster coordination
- **MinIO**: S3-compatible object storage for persistent message storage
- **MinIO Client (MC)**: Automatic bucket creation and configuration

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- At least 4GB RAM available for containers
- Ports 2379, 2380, 6650-6651, 9000-9001, 9040-9041, 50051-50052 available

## Quick Start

1. **Clone and navigate to the docker directory**:
   ```bash
   cd danube/docker
   ```

2. **Start the entire cluster**:
   ```bash
   docker-compose up -d
   ```

3. **Verify all services are healthy**:
   ```bash
   docker-compose ps
   ```

4. **Check logs** (optional):
   ```bash
   # View all logs
   docker-compose logs -f
   
   # View specific service logs
   docker-compose logs -f broker1
   docker-compose logs -f broker2
   ```

## Service Endpoints

| Service | Endpoint | Purpose |
|---------|----------|---------|
| Danube Broker 1 | `localhost:6650` | gRPC messaging |
| Danube Broker 2 | `localhost:6651` | gRPC messaging |
| Admin API 1 | `localhost:50051` | Broker administration |
| Admin API 2 | `localhost:50052` | Broker administration |
| Prometheus 1 | `localhost:9040` | Metrics and monitoring |
| Prometheus 2 | `localhost:9041` | Metrics and monitoring |
| MinIO API | `localhost:9000` | S3-compatible storage |
| MinIO Console | `localhost:9001` | Web UI (minioadmin/minioadmin123) |
| ETCD | `localhost:2379` | Metadata store |

## Testing with Danube CLI

### Installation

First, build the Danube CLI from the project root:

```bash
cd ../danube-cli
cargo build --release
```

The binary will be available at `../target/release/danube-cli`.

### Reliable Messaging with S3 Storage

Test the cloud-ready persistent storage capabilities:

Produce with reliable delivery and S3 persistence

```bash
../target/release/danube-cli produce \
  --service-addr http://localhost:6650 \
  --topic "/default/persistent-topic" \
  --count 100 \
  --message "Persistent message" \
  --reliable
```

Consume persistent messages

```bash
../target/release/danube-cli consume \
  --service-addr http://localhost:6650 \
  --topic "/default/persistent-topic" \
  --subscription "persistent-sub" \
  --sub-type exclusive
```

### Non-Reliable Message Flow Testing

#### Basic string messages

Produce basic string messages

```bash
../target/release/danube-cli produce \
  --service-addr http://localhost:6650 \
  --topic "/default/test-topic" \
  --count 10 \
  --message "Hello from Danube Docker!"
```

Consume from shared subscription

```bash
../target/release/danube-cli consume \
  --service-addr http://localhost:6650 \
  --topic "/default/test-topic" \
  --subscription "shared-sub" \
  --consumer "docker-consumer"
```

#### 2. JSON schema messages

Produce JSON messages with schema

```bash
../target/release/danube-cli produce \
  --service-addr http://localhost:6650 \
  --topic "/default/json-topic" \
  --count 5 \
  --schema json \
  --json-schema '{"type":"object","properties":{"message":{"type":"string"},"timestamp":{"type":"number"}}}' \
  --message '{"message":"Hello JSON","timestamp":1640995200}'
```

Consume JSON messages

```bash
../target/release/danube-cli consume \
  --service-addr http://localhost:6651 \
  --topic "/default/json-topic" \
  --subscription "json-sub" \
  --consumer "json-consumer"
```

## Monitoring and Observability

### Prometheus Metrics

Access broker metrics for monitoring:

```bash
# Broker 1 metrics
curl http://localhost:9040/metrics

# Broker 2 metrics  
curl http://localhost:9041/metrics
```

### MinIO Console

1. Open http://localhost:9001 in your browser
2. Login with credentials: `minioadmin` / `minioadmin123`
3. Navigate to "Buckets" to see:
   - `danube-messages`: Persistent message storage
   - `danube-wal`: Write-ahead log storage

### ETCD Inspection

```bash
# List all keys in ETCD
docker exec danube-etcd etcdctl --endpoints=http://127.0.0.1:2379 get --prefix ""

# Watch for changes
docker exec danube-etcd etcdctl --endpoints=http://127.0.0.1:2379 watch --prefix ""
```

## Configuration

### Broker Configuration

The `danube_broker.yml` file is optimized for:
- **S3 Storage**: MinIO integration with automatic credential management
- **High Performance**: Optimized WAL rotation and batch sizes
- **Development**: Relaxed security and unlimited resource policies
- **Monitoring**: Prometheus metrics enabled on all brokers

### Environment Variables

Key environment variables used:
- `AWS_ACCESS_KEY_ID=minioadmin`
- `AWS_SECRET_ACCESS_KEY=minioadmin123`
- `AWS_REGION=us-east-1`
- `RUST_LOG=danube_broker=info,danube_core=info`

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure all required ports are available
2. **Memory issues**: Increase Docker memory allocation if containers fail to start
3. **Storage issues**: Check MinIO bucket creation in logs: `docker-compose logs mc`

### Reset Environment

```bash
# Stop and remove all containers, networks, and volumes
docker-compose down -v

# Remove all Danube-related Docker resources
docker volume prune -f
docker network prune -f

# Restart fresh
docker-compose up -d
```

## Production Considerations

This setup demonstrates Danube's cloud-ready capabilities. For production deployment:

1. **Replace MinIO** with AWS S3, Google Cloud Storage, or Azure Blob Storage
2. **Enable TLS/SSL** authentication in broker configuration
3. **Configure resource limits** and health checks appropriately
4. **Set up monitoring** with Prometheus and Grafana
5. **Implement backup strategies** for ETCD and persistent storage
6. **Use container orchestration** like Kubernetes for scaling

## AWS S3 Migration

To migrate from MinIO to AWS S3, update `danube_broker.yml`:

```yaml
wal_cloud:
  cloud:
    backend: "s3"
    root: "s3://your-production-bucket/danube-cluster"
    region: "us-west-2"
    # Remove endpoint for AWS S3
    # endpoint: "http://minio:9000"  
    # Use IAM roles or environment variables for credentials
```

This Docker Compose setup showcases Danube's architecture with cloud-native storage.
