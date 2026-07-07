# Danube Iceberg

**Lakehouse connector for Danube messaging — continuously exports streaming data to Apache Iceberg tables.**

Danube Iceberg runs as a **standalone sidecar** alongside your Danube cluster. It discovers sealed WAL segments from object storage, converts them to Apache Parquet files, and optionally registers them with an Iceberg catalog, making your streaming data instantly queryable by DuckDB, Snowflake, Athena, Trino, Spark, and any other engine that speaks Iceberg or Parquet.

## How It Works

```
Danube Cluster                danube-iceberg                 Analytics Engine
(brokers)                     (sidecar binary)               (DuckDB, Trino, Snowflake, ...)
      │                             │                               │
      │  sealed .dnb1 segments      │                               │
      │  on S3 / GCS / Azure        │                               │
      │                             │                               │
      └──────────────────────▶ 1. Discover segments                 │
                               2. Decode WAL frames                 │
                               3. Write Parquet (zstd)              │
                               4. Commit to Iceberg catalog         │
                               5. Checkpoint progress               │
                                        │                           │
                                        │   .parquet + metadata     │
                                        └──────────────────────────▶│
                                                              Query tables
```

1. **Segment discovery**: Polls the Danube broker's gRPC API to find newly sealed `.dnb1` segments in object storage.
2. **WAL decoding**: Reads raw segment files and decodes the WAL frames back into messages.
3. **Parquet conversion**: Converts messages to columnar Apache Parquet format (zstd compressed).
4. **Catalog commit**: Registers the Parquet files with an Iceberg catalog so analytics engines can discover and query them.
5. **Checkpointing**: Tracks progress per topic so it picks up exactly where it left off after restarts.

## Architecture

`danube-iceberg` is designed for **isolation**, it runs as an independent process with its own scaling and failure domain:

- **No broker impact**: Reads directly from object storage, not from broker memory. The broker is never slowed down by analytics workloads.
- **Independent scaling**: Run one instance per cluster, or scale horizontally with topic partitioning.
- **Crash-safe**: Checkpoints persisted to object storage. Restart at any time without data loss or duplication.
- **Schema-less**: Automatically infers Arrow schemas from JSON payloads. No schema registry needed (though it works with one too).

## Quick Start

```bash
# Build
cargo build --release --package danube-iceberg

# Run with a config file
danube-iceberg --config danube-iceberg-config.yaml
```

## Configuration

Configuration is a single YAML file. See [`config-example.yaml`](config-example.yaml) for a complete reference.

```yaml
# Minimal configuration
broker:
  address: "localhost:6650"

storage:
  backend: "s3"
  root: "s3://my-bucket/danube"
  output_prefix: "iceberg"
  options:
    endpoint: "http://minio:9000"
    region: "us-east-1"
    access_key: "minioadmin"
    secret_key: "minioadmin"

# Iceberg catalog (optional — omit for Parquet-only mode)
catalog:
  type: "rest"
  name: "danube_catalog"
  properties:
    uri: "http://nessie:19120/api/v1"
    warehouse: "s3://my-bucket/warehouse"

topics:
  - namespace: "default"
    topic: "sensor-data"
    table_name: "sensor_data"
```

### Supported Catalogs

| Type | Description | Use Case |
|------|-------------|----------|
| `rest` | REST catalog (Nessie, Polaris, Tabular, Unity) | Universal standard — works everywhere |
| `glue` | AWS Glue Data Catalog | AWS-native, zero-infra |
| `s3tables` | AWS S3 Tables | Managed Iceberg on S3 |
| `sql` | SQL-backed (SQLite, PostgreSQL) | Self-hosted, dev/test |
| *(omit)* | Parquet-only mode | Just write Parquet files, no catalog |


### Topics

Only explicitly configured topics are exported. Each topic maps to one Iceberg table:

```yaml
topics:
  - namespace: "default"
    topic: "sensor-data"
    table_name: "sensor_data"       # Iceberg table name
    compaction:                      # Optional per-topic override
      target_parquet_size_mb: 100
      max_flush_interval_seconds: 60
```

## Docker

The `danube-iceberg` binary is available as a Docker image target in the project's multi-stage [Dockerfile](../Dockerfile):

```bash
# Build
docker build --target iceberg -t danube-iceberg .

# Run
docker run \
  -v ./danube-iceberg-config.yaml:/etc/danube-iceberg-config.yaml:ro \
  danube-iceberg \
  --config /etc/danube-iceberg-config.yaml
```

Or add it to your `docker-compose.yml`:

```yaml
services:
  danube-iceberg:
    build:
      context: .
      dockerfile: Dockerfile
      target: iceberg
    volumes:
      - ./danube-iceberg-config.yaml:/etc/danube-iceberg-config.yaml:ro
    environment:
      - RUST_LOG=danube_iceberg=info
    command: ["--config", "/etc/danube-iceberg-config.yaml"]
    depends_on:
      - broker1
```

## Development

```bash
cargo build --package danube-iceberg
cargo test --package danube-iceberg
cargo run --package danube-iceberg -- --config config-example.yaml
```

---

**[Documentation](https://danube-messaging.com/)** · **[Issues](https://github.com/danube-messaging/danube/issues)**
