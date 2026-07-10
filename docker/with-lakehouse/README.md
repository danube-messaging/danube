# Danube Lakehouse Pipeline — Docker Compose

**Full end-to-end demo**: IoT devices → MQTT → Edge Broker → Cluster → Iceberg/Parquet → Spark SQL

This setup demonstrates the complete Danube lakehouse data pipeline, from simulated IoT sensors all the way to queryable Iceberg tables in Apache Spark.

## Architecture

```
┌──────────────┐    MQTT     ┌──────────────┐   gRPC     ┌──────────────────┐
│  IoT Sim     │────────────▶│  Edge Broker  │──────────▶│  Cluster (3x)     │
│  (Python)    │   :1883     │  (edge mode)  │           │  broker1/2/3      │
└──────────────┘             └──────────────┘           └────────┬─────────┘
                                                                 │ sealed .dnb1
                                                                 ▼
                              ┌──────────────┐           ┌──────────────────┐
                              │  MinIO (S3)  │◀──────────│  danube-iceberg   │
                              │  :9000/:9001 │  Parquet  │  (sidecar)        │
                              └──────┬───────┘           └──────────────────┘
                                     │                           │
                              ┌──────▼───────┐          catalog commits
                              │  Iceberg REST│◀──────────────────┘
                              │  Catalog     │
                              │  :8181       │
                              └──────┬───────┘
                                     │
                              ┌──────▼───────┐
                              │  Spark SQL   │  ← Query Iceberg tables
                              │  (Jupyter)   │
                              │  :8888       │
                              └──────────────┘
```

## Services

| Service | Container | Ports | Purpose |
|---------|-----------|-------|---------|
| MinIO | `lakehouse-minio` | 9000, 9001 | S3-compatible object storage |
| Broker 1 | `lakehouse-broker1` | 6650, 50051, 9040 | Cluster broker (leader-eligible) |
| Broker 2 | `lakehouse-broker2` | 6651, 50052, 9041 | Cluster broker |
| Broker 3 | `lakehouse-broker3` | 6652, 50053, 9042 | Cluster broker |
| Edge Broker | `lakehouse-edge-broker` | 1883, 6653 | MQTT gateway → cluster replication |
| Admin Init | `lakehouse-admin-init` | — | Registers schemas + namespaces (runs once) |
| danube-iceberg | `lakehouse-danube-iceberg` | — | Converts segments to Parquet/Iceberg |
| Iceberg REST | `lakehouse-iceberg-rest` | 8181 | Iceberg catalog metadata server |
| Spark | `lakehouse-spark` | 8888, 8080 | Jupyter Notebook for SQL queries |
| IoT Simulator | `lakehouse-iot-simulator` | — | Simulates 3 sensor devices over MQTT |
| Prometheus | `lakehouse-prometheus` | 9090 | Metrics collection |

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- At least **4GB RAM** (Spark + broker builds)
- ~10GB disk space (first build downloads + compiles Rust workspace)

## Quick Start

```bash
# From repo root
cd docker/with-lakehouse/

# Build and start everything
docker-compose up -d --build

# Watch the pipeline (initial build takes ~10 minutes)
docker-compose logs -f iot-simulator danube-iceberg
```

> **Note:** The first build compiles the entire Rust workspace. Subsequent builds use Docker layer caching and are much faster.

## How It Works

### 1. IoT Simulator → Edge Broker (MQTT)

The Python simulator creates 3 virtual sensor devices that publish JSON telemetry every 2 seconds to the edge broker's MQTT gateway:

```
MQTT Topic: device/sensor-1/telemetry
Payload: {
  "device_id": "sensor-1",
  "location": "warehouse-A",
  "temperature": 23.45,
  "humidity": 55.2,
  "pressure": 1013.5,
  "battery_pct": 87.3,
  "timestamp": 1720000000
}
```

The edge broker validates each payload against the `telemetry-events` JSON schema registered on the cluster.

### 2. Edge Broker → Cluster (gRPC Replication)

The edge broker's replicator batches MQTT messages and replicates them to the cluster via gRPC. Messages arrive on the cluster topic `/edge1/telemetry`.

### 3. Cluster → MinIO (Segment Export)

Each cluster broker writes messages to its local WAL. When the WAL reaches 1 MiB (configured small for this demo), it rotates and the sealed segment (`.dnb1`) is exported to MinIO at:

```
s3://danube-messages/cluster-data/storage/topics/edge1/telemetry/segments/data-*.dnb1
```

### 4. danube-iceberg → Parquet/Iceberg

The `danube-iceberg` sidecar polls the cluster every 10 seconds for new sealed segments. When it finds them, it:

1. Reads the `.dnb1` segment from MinIO
2. Decodes the binary frames into Arrow RecordBatches
3. Writes Parquet files to the Iceberg warehouse (`s3://warehouse/`)
4. Commits the new data files to the Iceberg REST catalog

### 5. Spark SQL ← Query

Once Parquet files are committed, you can query them using Apache Spark SQL through the Jupyter Notebook.

## Querying Data with Spark

### Open Jupyter

Navigate to **http://localhost:8888** (token: `lakehouse` or check spark container logs for the URL with token).

### Create a New Notebook

In a new Python 3 notebook cell:

```python
# Connect to the Iceberg REST catalog
spark.sql("USE danube_catalog")

# List available tables
spark.sql("SHOW TABLES IN edge1").show()

# Query telemetry data
spark.sql("""
    SELECT device_id, location, temperature, humidity, pressure, timestamp
    FROM edge1.telemetry_data
    ORDER BY timestamp DESC
    LIMIT 20
""").show()

# Aggregations
spark.sql("""
    SELECT 
        device_id,
        COUNT(*) as readings,
        ROUND(AVG(temperature), 2) as avg_temp,
        ROUND(MIN(temperature), 2) as min_temp,
        ROUND(MAX(temperature), 2) as max_temp,
        ROUND(AVG(humidity), 2) as avg_humidity
    FROM edge1.telemetry_data
    GROUP BY device_id
    ORDER BY device_id
""").show()
```

## Verify the Pipeline

### Check IoT Simulator

```bash
docker logs lakehouse-iot-simulator
# Should show: ✅ Connected, publishing telemetry readings
```

### Check Edge Broker

```bash
docker logs lakehouse-edge-broker 2>&1 | grep -i "replicated\|mqtt\|edge"
# Should show: messages being replicated to cluster
```

### Check danube-iceberg

```bash
docker logs lakehouse-danube-iceberg
# Should show: polling for segments, converting to Parquet
```

### MinIO Console

Open **http://localhost:9001** (login: `minioadmin` / `minioadmin123`):

- **danube-messages** bucket → `cluster-data/storage/topics/edge1/telemetry/segments/` → `.dnb1` files
- **warehouse** bucket → Parquet files organized by Iceberg table structure

### Prometheus Metrics

Open **http://localhost:9090** for broker metrics:

```promql
# Messages published to cluster
danube_broker_messages_in_total

# Segment exports
danube_persistent_storage_segment_export_total
```

## Configuration

### Broker Config: `danube_broker_lakehouse.yml`

Based on the cloud storage config with:
- **WAL rotation: 1 MiB** (small for fast demo — production uses 512 MiB)
- **S3 endpoint:** `http://minio:9000`
- **Bootstrap namespaces:** `default` + `edge1`

### Edge Config: `edge.yaml`

- **MQTT listener:** `0.0.0.0:1883`
- **Topic mapping:** `device/+/telemetry` → `/edge1/telemetry` (with JSON schema validation)

### Iceberg Config: `danube_iceberg.yaml`

- **Polling interval:** 10 seconds
- **Flush threshold:** 1 MiB or 30 seconds
- **Catalog:** Iceberg REST at `http://iceberg-rest:8181`

### IoT Simulator

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_DEVICES` | 3 | Number of simulated sensors |
| `PUBLISH_INTERVAL` | 2 | Seconds between publishes |
| `DURATION` | 300 | Runtime in seconds (0 = infinite) |

## Switching to Published Images

When released images are available, replace `build:` blocks with `image:`:

```yaml
# Before (build from source)
broker1:
  build:
    context: ../../
    dockerfile: Dockerfile
    target: broker

# After (published image)
broker1:
  image: ghcr.io/danube-messaging/danube-broker:latest
```

Similarly for `danube-iceberg`:
```yaml
danube-iceberg:
  image: ghcr.io/danube-messaging/danube-iceberg:latest
```

## Troubleshooting

### Build takes too long

The first build compiles the entire Rust workspace (~10 minutes). Subsequent builds use Docker layer caching. Use `--no-cache` only when needed:

```bash
docker-compose build --no-cache broker1
```

### No data in Spark

1. Check IoT simulator is publishing: `docker logs lakehouse-iot-simulator`
2. Check edge broker is replicating: `docker logs lakehouse-edge-broker`
3. Check segments exist in MinIO Console at `danube-messages/cluster-data/storage/topics/`
4. Check danube-iceberg logs: `docker logs lakehouse-danube-iceberg`
5. Wait 2-3 minutes — the pipeline needs time for WAL rotation + export + conversion

### Port conflicts

```bash
# Check which ports are in use
for port in 1883 6650 6651 6652 6653 8080 8181 8888 9000 9001 9040 9041 9042 9090; do
  lsof -i :$port 2>/dev/null && echo "Port $port is in use!"
done
```

### Reset everything

```bash
docker-compose down -v
docker volume prune -f
docker-compose up -d --build
```

## Stop

```bash
docker-compose down -v  # -v removes volumes for a fresh start
```
