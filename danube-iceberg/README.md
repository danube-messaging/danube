# Danube Iceberg

**Embedded Apache Iceberg lakehouse engine for Danube v2 — continuously exports sealed WAL segments to Iceberg tables as Parquet files with column statistics.**

In Danube v2, `danube-iceberg` is a **library crate embedded into `danube-broker` via Cargo feature flags** (`--features lakehouse` or `--features lakehouse-aws`) instead of a standalone binary or YAML-configured sidecar. It runs inside an isolated `LakehouseRuntime` (dedicated `danube-lakehouse` Tokio thread pool) so Arrow/Parquet conversion never interferes with hot-path broker message dispatch, and nodes can be isolated with `--node-role lakehouse` (or run mixed with `--node-role all`).

## How It Works

```
Sealed .dnb1 segments (Object Storage / WAL)
  │
  ▼
1. Segment discovery & checkpoint tracking
2. WAL frame decoding + Schema Registry lookup (JSON Schema, Avro, or envelope fallback)
3. Arrow RecordBatch → ParquetWriterBuilder → RollingFileWriter → DataFileWriter
   (ZSTD Parquet + min/max bounds, value counts, null counts, split offsets)
4. Atomic Iceberg catalog commit (Transaction::fast_append)
  │
  ▼
Queryable in DuckDB, Trino, Spark, Snowflake, Athena
```

## Architecture & Guarantees

- **Workload Isolation (`LakehouseRuntime`):** Runs on a dedicated `danube-lakehouse` Tokio runtime with bounded worker threads so analytics serialization does not cause latency spikes in client message delivery. Configured via `DANUBE_LAKEHOUSE_CONFIG=/path/to/lakehouse.yaml` (starts in idle standby when unset).
- **Node-Role Scheduling (`--node-role`):** Run `danube up --node-role broker` on latency-sensitive ingress nodes and `danube up --node-role lakehouse` on dedicated export nodes (or `--node-role all` by default). Dedicated `lakehouse` nodes register as `drained` (`lakehouse_only`) so `LoadManager` does not assign client topic partitions to them.
- **Schema-Aware Conversion:** Resolves schemas from Danube's embedded Schema Registry (`json_schema`, `avro`) to emit typed Iceberg columns and per-column statistics, falling back to a structured envelope schema for schema-less topics.
- **Catalog Support:**
  - `rest` — Apache Iceberg REST Catalog (Polaris, Nessie, Tabular, Unity, AWS SigV4 REST endpoints with `--features lakehouse-aws`)
  - `sql` — SQL-backed catalog (SQLite/PostgreSQL for local dev and self-hosted setups)
- **Crash-Safe Checkpointing:** Tracks export checkpoints cleanly across restarts.

## Building with Lakehouse Support

```bash
# Build the unified `danube` binary with Iceberg lakehouse support
cargo build --release -p danube-broker --features lakehouse

# Include AWS SigV4 support for AWS Iceberg REST endpoints
cargo build --release -p danube-broker --features lakehouse-aws
```

## Testing

```bash
cargo test --package danube-iceberg --tests
cargo check --workspace --features danube-broker/lakehouse
```

---

**[Documentation](https://danube-messaging.com/)** · **[Issues](https://github.com/danube-messaging/danube/issues)**
