# Danube Iceberg

**Lakehouse connector for Danube messaging — continuously exports streaming data to Apache Iceberg tables.**

Danube Iceberg runs as a **standalone sidecar** alongside your Danube cluster. It discovers sealed WAL segments from object storage, converts them to Apache Iceberg data files with full column-level statistics, and registers them with an Iceberg catalog — making your streaming data instantly queryable by DuckDB, Snowflake, Athena, Trino, Spark, and any other engine that speaks Iceberg.

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
                               3. Write Iceberg data files          │
                                  (Parquet + column statistics)     │
                               4. Commit to Iceberg catalog         │
                               5. Checkpoint progress               │
                                        │                           │
                                        │   .parquet + metadata     │
                                        └──────────────────────────▶│
                                                              Query tables
```

1. **Segment discovery**: Polls the Danube broker's gRPC API (`StorageService`) to find newly sealed `.dnb1` segments in object storage.
2. **WAL decoding**: Reads raw segment files and decodes the WAL frames back into messages.
3. **Iceberg data file writing**: Converts messages to Iceberg data files using iceberg-rust's writer pipeline (`ParquetWriterBuilder → RollingFileWriter → DataFileWriter`), producing Parquet files with ZSTD compression and **full per-column statistics** (min/max bounds, null counts, value counts, column sizes).
4. **Catalog commit**: Commits data files to the Iceberg catalog via `Transaction::fast_append`, making them discoverable by query engines.
5. **Checkpointing**: Tracks progress per topic in object storage so it picks up exactly where it left off after restarts.

## Architecture

`danube-iceberg` is designed for **isolation** — it runs as an independent process with its own scaling and failure domain:

- **No broker impact**: Reads directly from object storage, not from broker memory. The broker is never slowed down by analytics workloads.
- **Independent scaling**: Run one instance per cluster, or scale horizontally with topic partitioning.
- **Crash-safe**: Checkpoints persisted to object storage. Restart at any time without data loss (at-least-once semantics).
- **Shared connection pool**: All topic workers share a single `DanubeClient` with HTTP/2 multiplexed connections — even 100+ topics use one TCP connection.
- **Schema-aware**: Resolves schemas from the Danube Schema Registry when available. Falls back to an envelope schema (offset, publish_time, producer_name, payload) for schema-less topics.
- **Schema evolution detection**: Detects new columns in incoming data and logs warnings. Full schema evolution will be applied when iceberg-rust adds `Transaction::update_schema()`.

### Writer Pipeline

Unlike writing raw Parquet files manually, `danube-iceberg` uses iceberg-rust's composable writer stack:

```
ParquetWriterBuilder → RollingFileWriterBuilder → DataFileWriterBuilder
    → DataFileWriter.write(batch)
    → DataFileWriter.close() → Vec<DataFile> (with full statistics)
    → Transaction::fast_append().add_data_files(data_files)
```

This produces `DataFile` metadata with per-column statistics that enable query engines to perform **predicate pushdown** and **file pruning** — queries like `WHERE timestamp > '2025-01-01'` skip data files whose max timestamp is before the cutoff.

| Statistic | Description |
|-----------|-------------|
| `column_sizes` | Compressed size per column |
| `value_counts` | Number of values per column |
| `null_value_counts` | Number of nulls per column |
| `lower_bounds` | Minimum value per column |
| `upper_bounds` | Maximum value per column |
| `split_offsets` | Row group byte offsets |

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
cargo test --package danube-iceberg      # 41 tests
cargo run --package danube-iceberg -- --config config-example.yaml
```

---

**[Documentation](https://danube-messaging.com/)** · **[Issues](https://github.com/danube-messaging/danube/issues)**
