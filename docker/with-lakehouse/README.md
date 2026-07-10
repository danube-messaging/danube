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
| IoT Simulator | `lakehouse-iot-simulator` | — | 9 IoT devices across 3 MQTT topics |
| Prometheus | `lakehouse-prometheus` | 9090 | Metrics collection |

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- At least **4GB RAM** (Spark + broker builds)
- ~10GB disk space (first build downloads + compiles Rust workspace)

---

## Quick Start

### 1. Build and Start

```bash
cd docker/with-lakehouse/
docker compose up -d --build
```

> **Note:** The first build compiles the entire Rust workspace (~10 minutes). Subsequent builds use Docker layer caching and are much faster.

### 2. Wait for the Pipeline

The pipeline needs about **5–7 minutes** after startup for data to flow through. You can follow the progress:

```bash
# Watch IoT simulator + iceberg converter
docker compose logs -f iot-simulator danube-iceberg
```

Wait until you see log lines like:

```
danube-iceberg  | wrote iceberg data file path=s3://warehouse/edge1/telemetry_data/data/part-00000-*.parquet rows=1350
danube-iceberg  | flushed to iceberg topic=/edge1/telemetry rows=1350
```

### 3. Verify in MinIO Console

Open **http://localhost:9001** (login: `minioadmin` / `minioadmin123`).

Check these two buckets:

- **`danube-messages`** → `cluster-data/storage/topics/edge1/` — sealed `.dnb1` segment files (the raw WAL segments exported by brokers)
- **`warehouse`** → `edge1/telemetry_data/data/` — Parquet files written by `danube-iceberg`, organized in the standard Iceberg table layout

### 4. Query with Spark SQL (Jupyter)

Open **http://localhost:8888** (token: `lakehouse`, or check `docker logs lakehouse-spark` for the URL with token).

Create a new **Python 3** notebook and run the following cells one at a time:

**Cell 1 — Connect to catalog and list tables:**

```python
# The tabulario/spark-iceberg image pre-configures a catalog named 'demo'
# pointing to the Iceberg REST catalog at http://rest:8181
spark.sql("USE demo")

# List available Iceberg tables
spark.sql("SHOW TABLES IN edge1").show()
```

Expected output:

```
+---------+--------------+-----------+
|namespace|     tableName|isTemporary|
+---------+--------------+-----------+
|    edge1|    alert_data|      false|
|    edge1|   sensor_data|      false|
|    edge1|telemetry_data|      false|
+---------+--------------+-----------+
```

**Cell 2 — Query telemetry readings:**

```python
spark.sql("""
    SELECT device_id, location, temperature, humidity, pressure, timestamp
    FROM edge1.telemetry_data
    ORDER BY timestamp DESC
    LIMIT 20
""").show()
```

**Cell 3 — Aggregation per device:**

```python
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

**Cell 4 — Query machine sensors:**

```python
spark.sql("""
    SELECT device_id, location, rpm, vibration_mm_s, motor_temp_c, power_watts
    FROM edge1.sensor_data
    ORDER BY timestamp DESC
    LIMIT 20
""").show()
```

**Cell 5 — Query alerts:**

```python
spark.sql("""
    SELECT device_id, alert_type, severity, value, message, acknowledged
    FROM edge1.alert_data
    ORDER BY timestamp DESC
    LIMIT 20
""").show()
```

**Cell 6 — Alert severity breakdown:**

```python
spark.sql("""
    SELECT 
        severity,
        alert_type,
        COUNT(*) as total,
        ROUND(AVG(value), 2) as avg_value
    FROM edge1.alert_data
    GROUP BY severity, alert_type
    ORDER BY severity, total DESC
""").show(30)
```

**Cell 7 — Cross-table: devices with both sensor data and alerts:**

```python
spark.sql("""
    SELECT 
        s.device_id,
        COUNT(DISTINCT s.timestamp) as sensor_readings,
        COUNT(DISTINCT a.timestamp) as alert_count
    FROM edge1.sensor_data s
    LEFT JOIN edge1.alert_data a ON s.device_id = a.device_id
    GROUP BY s.device_id
    ORDER BY s.device_id
""").show()
```

**Cell 8 — Iceberg metadata: view table snapshots (time-travel history):**

```python
spark.sql("SELECT * FROM edge1.telemetry_data.snapshots").show(truncate=False)
```

### 5. Check Pipeline Health

```bash
# IoT simulator status (message counts per topic)
docker logs lakehouse-iot-simulator 2>&1 | tail -5

# danube-iceberg activity (Parquet writes)
docker logs lakehouse-danube-iceberg 2>&1 | grep "flushed to" | tail -5

# Edge broker replication
docker logs lakehouse-edge-broker 2>&1 | grep -i "replicated\|mqtt" | tail -5

# Prometheus metrics
# Open http://localhost:9090 and query: danube_broker_messages_in_total
```

### 6. Stop

```bash
docker compose down -v    # -v removes volumes for a fresh start next time
```

---

## How It Works

### IoT Simulator → Edge Broker (MQTT)

The Python simulator (`iot_simulator.py`) creates **9 virtual IoT devices** distributed across 3 topic categories. Each device publishes JSON messages once per second over MQTT:

| Topic Category | MQTT Pattern | Devices | Payload Fields |
|---------------|-------------|---------|----------------|
| **Telemetry** | `device/+/telemetry` | sensor-01, 04, 07 | temperature, humidity, pressure, battery_pct |
| **Sensors** | `sensors/+/vibration` | sensor-02, 05, 08 | rpm, vibration_mm_s, motor_temp_c, power_watts |
| **Alerts** | `device/+/alerts` | sensor-03, 06, 09 | alert_type, severity, value, message, acknowledged |

Each topic category has a **JSON Schema** registered in the Danube cluster (by `admin-init`). The edge broker validates incoming MQTT payloads against these schemas before replicating.

Example telemetry payload:

```json
{
  "device_id": "sensor-01",
  "location": "warehouse-A",
  "temperature": 23.45,
  "humidity": 55.2,
  "pressure": 1013.5,
  "battery_pct": 87.3,
  "timestamp": 1720000000
}
```

### Edge Broker → Cluster (gRPC Replication)

The edge broker's **replicator** batches MQTT messages (50 per batch, 500ms timeout) and replicates them to the 3-node cluster via gRPC. Each MQTT topic pattern maps to a Danube cluster topic:

| MQTT Pattern | Cluster Topic |
|-------------|---------------|
| `device/+/telemetry` | `/edge1/telemetry` |
| `sensors/+/vibration` | `/edge1/sensors` |
| `device/+/alerts` | `/edge1/alerts` |

### Cluster → MinIO (Segment Export)

Each cluster broker writes messages to its local **Write-Ahead Log (WAL)**. When a WAL segment reaches 64 KiB (small for demo; production uses 512 MiB), it is **sealed** and exported to MinIO:

```
s3://danube-messages/cluster-data/storage/topics/edge1/telemetry/segments/data-*.dnb1
```

The `.dnb1` files are Danube's native binary segment format containing message frames with headers and payloads.

### danube-iceberg → Parquet/Iceberg

The `danube-iceberg` sidecar polls the cluster every **10 seconds** for new sealed segments. For each topic, it:

1. **Reads** the `.dnb1` segment from MinIO
2. **Resolves** the JSON Schema from the cluster's schema registry
3. **Decodes** binary frames into typed Arrow RecordBatches (using the schema to create columns like `temperature Float64`, `device_id Utf8`, etc.)
4. **Writes** Parquet files (ZSTD compressed) to the Iceberg warehouse at `s3://warehouse/`
5. **Commits** the new data files to the Iceberg REST catalog via `fast_append`

Each Parquet file includes **per-column statistics** (min/max bounds, null counts) that enable query engines like Spark to perform predicate pushdown and file pruning.

Standard Iceberg table properties are set at creation time:

| Property | Value | Purpose |
|----------|-------|---------|
| `write.metadata.previous-versions-max` | `10` | Limits accumulated metadata files |
| `write.metadata.delete-after-commit.enabled` | `true` | Cleans up old metadata automatically |
| `write.parquet.compression-codec` | `zstd` | Efficient compression |
| `format-version` | `2` | Iceberg V2 (supports row-level deletes) |

### Spark SQL ← Query

The `tabulario/spark-iceberg` container runs Spark with a **Jupyter Notebook** interface and comes pre-configured with an Iceberg catalog named `demo` pointing to the REST catalog. Once Parquet files are committed, any standard SQL query works — including aggregations, joins, and Iceberg-specific features like time-travel (`SELECT * FROM table.snapshots`).

## Configuration

### Broker Config: `danube_broker_lakehouse.yml`

Based on the cloud storage config with:
- **WAL rotation:** 64 KiB (small for fast demo — production uses 512 MiB)
- **S3 endpoint:** `http://minio:9000`
- **Bootstrap namespaces:** `default` + `edge1`

### Edge Config: `edge.yaml`

- **MQTT listener:** `0.0.0.0:1883`
- **3 topic mappings** with JSON schema validation
- **Replicator:** 50 msg batch size, 500ms timeout

### Iceberg Config: `danube_iceberg.yaml`

- **Polling interval:** 10 seconds
- **Flush threshold:** 1 MiB or 30 seconds
- **Catalog:** Iceberg REST at `http://iceberg-rest:8181`
- **3 topic-to-table mappings:** telemetry_data, sensor_data, alert_data

### IoT Simulator

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_DEVICES` | 9 | Number of simulated devices (3 per topic) |
| `PUBLISH_INTERVAL` | 1 | Seconds between publish rounds |
| `DURATION` | 1800 | Runtime in seconds (0 = infinite) |

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
docker compose build --no-cache broker1
```

### No data in Spark

1. Check IoT simulator is publishing: `docker logs lakehouse-iot-simulator`
2. Check edge broker is replicating: `docker logs lakehouse-edge-broker`
3. Check segments exist in MinIO Console at `danube-messages/cluster-data/storage/topics/`
4. Check danube-iceberg logs: `docker logs lakehouse-danube-iceberg`
5. Wait 5–7 minutes — the pipeline needs time for WAL rotation + segment export + Parquet conversion

### Port conflicts

```bash
for port in 1883 6650 6651 6652 6653 8080 8181 8888 9000 9001 9040 9041 9042 9090; do
  lsof -i :$port 2>/dev/null && echo "Port $port is in use!"
done
```

### Reset everything

```bash
docker compose down -v
docker volume prune -f
docker compose up -d --build
```
