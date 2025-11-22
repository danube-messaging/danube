# Danube Docker Compose Setup

This directory contains a production-ready Docker Compose setup for the Danube messaging platform, demonstrating cloud-ready deployment with S3-compatible storage (MinIO) and distributed metadata management (ETCD).

## Architecture Overview

The setup includes:
- **2 Danube Brokers**: High-availability message brokers with load balancing
- **ETCD**: Distributed metadata store for cluster coordination
- **MinIO**: S3-compatible object storage for persistent message storage
- **MinIO Client (MC)**: Automatic bucket creation and configuration
- **Prometheus**: Metrics collection and monitoring server
- **Admin Gateway**: HTTP/JSON BFF for the Admin UI
- **Admin UI**: Web UI for cluster management (served via Nginx)

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- At least 4GB RAM available for containers
- Ports 2379, 2380, 6650-6651, 9000-9001, 9040-9041, 50051-50052, 9090, 8080-8081 available

## Quick Start

### Step 1: Setup (Choose One Option)

**Option 1: Download Docker Compose Files (Recommended for running the broker)**

Create a directory and download the required files:

```bash
mkdir danube-docker && cd danube-docker
```

Download the docker-compose file:

```bash
curl -O https://raw.githubusercontent.com/danube-messaging/danube/main/docker/danube_with_ui/docker-compose.yml
```

Download the broker configuration file:

```bash
curl -O https://raw.githubusercontent.com/danube-messaging/danube/main/docker/danube_with_ui/danube_broker.yml
```

Download the Prometheus configuration file:

```bash
curl -O https://raw.githubusercontent.com/danube-messaging/danube/main/docker/danube_with_ui/prometheus.yml
```

**Option 2: Clone Repository (Recommended for development and building from source)**

```bash
git clone https://github.com/danube-messaging/danube.git
cd danube/docker/danube_with_ui
```

### Step 2: Start the Cluster

Start the entire cluster:

```bash
docker-compose up -d
```

### Step 3: Verify all services are healthy

Verify all services are running:

```bash
docker-compose ps
```

Expected output:

```bash
docker-compose ps
NAME                 IMAGE                                               SERVICE          STATUS                             PORTS
danube-admin-ui     ghcr.io/danube-messaging/danube-admin-ui:latest     admin-ui         Up                                0.0.0.0:8081->80/tcp
danube-admin-gateway ghcr.io/danube-messaging/danube-admin-gateway:latest admin-gateway  Up                                0.0.0.0:8080->8080/tcp
danube-prometheus   prom/prometheus:latest                               prometheus       Up                                0.0.0.0:9090->9090/tcp
danube-cli          ghcr.io/danube-messaging/danube-cli:latest           danube-cli       Up
danube-broker1      ghcr.io/danube-messaging/danube-broker:latest        broker1          Up (health: starting)            0.0.0.0:6650->6650/tcp, 0.0.0.0:9040->9040/tcp, 0.0.0.0:50051->50051/tcp
danube-broker2      ghcr.io/danube-messaging/danube-broker:latest        broker2          Up (health: starting)            0.0.0.0:6651->6650/tcp, 0.0.0.0:9041->9040/tcp, 0.0.0.0:50052->50051/tcp
danube-mc           minio/mc:RELEASE.2024-09-16T17-43-14Z                mc               Up
danube-minio        minio/minio:RELEASE.2025-07-23T15-54-02Z             minio            Up (healthy)                     0.0.0.0:9000-9001->9000-9001/tcp
danube-etcd         quay.io/coreos/etcd:v3.5.9                           etcd             Up (healthy)                     0.0.0.0:2379-2380->2379-2380/tcp
```


Open the Admin UI at: http://localhost:8081

**Check logs** (optional):
   ```bash
   # View all logs
   docker-compose logs -f
   
   # View specific service logs
   docker-compose logs -f broker1
   docker-compose logs -f broker2
   docker-compose logs -f admin-gateway
   docker-compose logs -f prometheus
   docker-compose logs -f admin-ui
   ```

## Service Endpoints

| Service | Endpoint | Purpose |
|---------|----------|---------|
| Admin UI | `http://localhost:8081` | Web UI for cluster management |
| Admin Gateway | `http://localhost:8080` | HTTP/JSON API consumed by Admin UI |
| Prometheus Server | `http://localhost:9090` | Metrics and monitoring UI |
| Broker 1 (gRPC) | `localhost:6650` | Messaging service |
| Broker 2 (gRPC) | `localhost:6651` | Messaging service |
| Broker 1 metrics | `http://localhost:9040/metrics` | Prometheus scrape endpoint |
| Broker 2 metrics | `http://localhost:9041/metrics` | Prometheus scrape endpoint |
| MinIO API | `http://localhost:9000` | S3-compatible storage |
| MinIO Console | `http://localhost:9001` | Web UI (minioadmin/minioadmin123) |
| ETCD | `http://localhost:2379` | Metadata store |

## Admin UI and Gateway

- Open the Admin UI at: `http://localhost:8081`
- Admin Gateway runs at: `http://localhost:8080`
- CORS is configured in the compose to allow `http://localhost:8081`
- Prometheus is available at: `http://localhost:9090`

### Version compatibility
If the UI shows errors like `Operation is not implemented or not supported` when listing topics, ensure images are on a recent release (e.g. `v0.5.2`). Consider pinning in `docker-compose.yml`:

```yaml
image: ghcr.io/danube-messaging/danube-broker:v0.5.2
image: ghcr.io/danube-messaging/danube-admin-gateway:v0.5.2
```

Then recreate:

```bash
docker compose pull
docker compose up -d --force-recreate
```

## Testing with Danube CLI

### Using the CLI Container

The Docker Compose setup includes a `danube-cli` container with both `danube-cli` and `danube-admin-cli` tools pre-installed. This eliminates the need to build or install Rust locally.

**No local installation required** - use the containerized CLI tools directly.

### Reliable Messaging with S3 Storage

Test the cloud-ready persistent storage capabilities:

**Produce with reliable delivery and S3 persistence:**

```bash
docker exec -it danube-cli danube-cli produce \
  --service-addr http://broker1:6650 \
  --topic "/default/persistent-topic" \
  --count 1000 \
  --message "Persistent message" \
  --reliable
```

**Consume persistent messages:**

```bash
docker exec -it danube-cli danube-cli consume \
  --service-addr http://broker1:6650 \
  --topic "/default/persistent-topic" \
  --subscription "persistent-sub" \
  --sub-type exclusive
```

### Non-Reliable Message Flow Testing

#### Basic string messages

**Produce basic string messages:**

```bash
docker exec -it danube-cli danube-cli produce \
  --service-addr http://broker1:6650 \
  --topic "/default/test-topic" \
  --count 100 \
  --message "Hello from Danube Docker!"
```

**Consume from shared subscription:**

```bash
docker exec -it danube-cli danube-cli consume \
  --service-addr http://broker1:6650 \
  --topic "/default/test-topic" \
  --subscription "shared-sub" \
  --consumer "docker-consumer"
```

#### JSON schema messages

**Produce JSON messages with schema:**

```bash
docker exec -it danube-cli danube-cli produce \
  --service-addr http://broker1:6650 \
  --topic "/default/json-topic" \
  --count 100 \
  --schema json \
  --json-schema '{"type":"object","properties":{"message":{"type":"string"},"timestamp":{"type":"number"}}}' \
  --message '{"message":"Hello JSON","timestamp":1640995200}'
```

**Consume JSON messages:**

```bash
docker exec -it danube-cli danube-cli consume \
  --service-addr http://broker2:6650 \
  --topic "/default/json-topic" \
  --subscription "json-sub" \
  --consumer "json-consumer"
```

### Admin CLI Operations

**Use danube-admin-cli for cluster management:**

```bash
# List active brokers
docker exec -it danube-cli danube-admin-cli brokers list

# List namespaces in cluster
docker exec -it danube-cli danube-admin-cli brokers namespaces

# List topics in a namespace
docker exec -it danube-cli danube-admin-cli topics list default

# List subscriptions on a topic
docker exec -it danube-cli danube-admin-cli topics subscriptions /default/test-topic
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

# Check broker registrations
docker exec danube-etcd etcdctl --endpoints=http://127.0.0.1:2379 get --prefix "/cluster/register"
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
