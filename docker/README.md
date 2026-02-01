# Danube Docker Setups

Choose the right setup for your use case:

| Setup | Services | Backend | Use Case |
|-------|----------|---------|----------|
| **[quickstart](#quickstart)** | ETCD + 2 Brokers + CLI + Prometheus | Filesystem | Quick testing, learning, MCP development |
| **[with-ui](#with-ui)** | + Admin Server + Web UI | Filesystem | Cluster monitoring, visual exploration |
| **[with-cloud-storage](#with-cloud-storage)** | + MinIO + MC | S3/MinIO | Cloud storage testing, durability testing |
| **[local-development](#local-development)** | All above (builds from source) | Filesystem | Development, testing local changes |

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- At least 2GB RAM (4GB for cloud-storage setup)

---

## Quickstart

**Minimal setup with filesystem backend** - Perfect for quick testing and MCP tools development.

**Services:** ETCD, 2 Brokers, CLI, Prometheus  
**Storage:** Filesystem (no MinIO overhead)  
**Ports:** 2379-2380 (ETCD), 6650-6651 (Brokers), 50051-50052 (Admin), 9040-9041 (Metrics), 9090 (Prometheus)

### Start

```bash
cd quickstart/
docker-compose up -d
```

### Verify

```bash
docker-compose ps
# All services should show "Up" status
```

### Test

```bash
# Produce messages
docker exec danube-cli produce \
  --service-addr http://broker1:6650 \
  --topic "/default/test" \
  --count 100 \
  --message "Hello Danube!"

# Consume messages
docker exec -it danube-cli consume \
  --service-addr http://broker1:6650 \
  --topic "/default/test" \
  --subscription "my-sub"
```

### Stop

```bash
docker-compose down -v  # -v removes volumes for fresh start
```

---

## With UI

**Adds web dashboard and admin server** - Perfect for cluster monitoring and exploration.

**Services:** Quickstart + Admin Server + Web UI  
**Storage:** Filesystem  
**Additional Ports:** 8080 (Admin API), 8081 (Web UI)

### Start

```bash
cd with-ui/
docker-compose up -d
```

### Access UI

Open **http://localhost:8081** in your browser

The UI provides:
- Real-time cluster status and metrics
- Topic and subscription management
- Broker load visualization
- Schema registry browser

### Admin CLI

```bash
# List brokers
docker exec danube-admin brokers list

# Check cluster balance
docker exec danube-admin brokers balance

# List topics
docker exec danube-admin topics list --namespace default
```

### Stop

```bash
docker-compose down -v
```

---

## With Cloud Storage

**S3/MinIO backend for testing cloud-native storage** - Perfect for durability and cloud migration testing.

**Services:** ETCD, MinIO, MC, 2 Brokers, CLI, Prometheus  
**Storage:** MinIO (S3-compatible)  
**Additional Ports:** 9000 (MinIO API), 9001 (MinIO Console)

### Start

```bash
cd with-cloud-storage/
docker-compose up -d
```

### MinIO Console

1. Open **http://localhost:9001**
2. Login: `minioadmin` / `minioadmin123`
3. View buckets: `danube-messages`, `danube-wal`

### Test Reliable Messaging

```bash
# Produce with reliable delivery (persisted to S3)
docker exec danube-cli produce \
  --service-addr http://broker1:6650 \
  --topic "/default/persistent" \
  --count 1000 \
  --message "Persistent message" \
  --reliable

# Consume persistent messages
docker exec -it danube-cli consume \
  --service-addr http://broker1:6650 \
  --topic "/default/persistent" \
  --subscription "persistent-sub" \
  --sub-type exclusive
```

### Stop

```bash
docker-compose down -v
```

---

## Local Development

**Builds from source for contributors** - Perfect for developing broker/admin code.

**Services:** All above (built locally) + UI  
**Storage:** Filesystem  
**Build Time:** ~5-10 minutes first time (cached after)

### Start

```bash
# From repo root
cd docker/local-development/
docker-compose up -d --build
```

### Rebuild After Code Changes

```bash
# Rebuild specific services
docker-compose build broker1 broker2 danube-admin
docker-compose up -d --no-deps broker1 broker2 danube-admin

# Or rebuild everything
docker-compose up -d --build
```

### Stop

```bash
docker-compose down -v
```

---

## Common Operations

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f broker1
docker-compose logs -f danube-admin
```

### Prometheus Metrics

```bash
# Broker metrics
curl http://localhost:9040/metrics  # broker1
curl http://localhost:9041/metrics  # broker2

# Prometheus UI
open http://localhost:9090
```

### ETCD Inspection

```bash
# List all keys
docker exec danube-etcd etcdctl --endpoints=http://127.0.0.1:2379 get --prefix ""

# Watch for changes
docker exec danube-etcd etcdctl --endpoints=http://127.0.0.1:2379 watch --prefix ""
```

### Admin Operations

```bash
# List brokers
docker exec danube-admin brokers list

# List namespaces
docker exec danube-admin brokers namespaces

# Check cluster balance
docker exec danube-admin brokers balance

# Trigger rebalancing
docker exec danube-admin brokers rebalance
```

### JSON Schema Messages

```bash
# Produce with schema
docker exec danube-cli produce \
  --service-addr http://broker1:6650 \
  --topic "/default/json-topic" \
  --count 100 \
  --schema json \
  --json-schema '{"type":"object","properties":{"message":{"type":"string"},"timestamp":{"type":"number"}}}' \
  --message '{"message":"Hello JSON","timestamp":1640995200}'

# Consume
docker exec -it danube-cli consume \
  --service-addr http://broker1:6650 \
  --topic "/default/json-topic" \
  --subscription "json-sub"
```

---

## Configuration

### Broker Configs

- **Quickstart/With-UI/Local-Development:** Uses `../config/danube_broker.yml` (filesystem backend)
- **With-Cloud-Storage:** Uses `../config/danube_broker_cloud.yml` (S3 backend)

Both configs include:
- `assignment_strategy: "balanced"` - Multi-factor broker selection
- `auth: none` - Disabled for development
- `policies: unlimited` - No rate limits for testing

### Prometheus Config

Shared across all setups: `docker/prometheus.yml`

Scrapes both brokers every 5 seconds.

---

## Troubleshooting

### Port Conflicts

Check if ports are already in use:

```bash
# Check specific port
lsof -i :6650

# Kill process using port
kill -9 <PID>
```

### Container Won't Start

```bash
# Check logs
docker-compose logs broker1

# Check Docker resources
docker stats
```

### Reset Everything

```bash
# Stop and remove all Danube containers/volumes
docker-compose down -v
docker volume prune -f
docker network prune -f

# Start fresh
docker-compose up -d
```

### Build Issues (local-development)

```bash
# Clear Docker build cache
docker builder prune -a

# Rebuild from scratch
docker-compose build --no-cache
docker-compose up -d
```

---

## Production Considerations

For production deployment:

1. **Storage:** Replace MinIO with AWS S3, GCS, or Azure Blob
2. **Security:** Enable TLS/mTLS in broker config (`auth: tls`)
3. **Resources:** Configure CPU/memory limits in compose file
4. **Monitoring:** Add Grafana dashboards for Prometheus
5. **Backup:** Implement ETCD backup strategy
6. **Orchestration:** Use Kubernetes for scaling and HA

### AWS S3 Migration

Update `../config/danube_broker.yml`:

```yaml
wal_cloud:
  cloud:
    backend: "s3"
    root: "s3://your-bucket/danube"
    region: "us-west-2"
    # Remove endpoint for AWS (uses default AWS endpoints)
    # Use IAM roles instead of hardcoded keys
```

---

## Structure

```
docker/
├── README.md                    # This file
├── prometheus.yml               # Shared Prometheus config
├── quickstart/
│   └── docker-compose.yml
├── with-ui/
│   └── docker-compose.yml
├── with-cloud-storage/
│   └── docker-compose.yml
└── local-development/
    └── docker-compose.yml
```

Config files referenced from `../config/`:
- `danube_broker.yml` - Filesystem backend (default)
- `danube_broker_cloud.yml` - S3/MinIO backend
