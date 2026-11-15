# 0013: Lakehouse export runs isolated from the hot path

**Status:** Accepted

## Decision

- `danube-iceberg` is a library compiled into the broker only with `--features lakehouse` (or `lakehouse-aws`).
- It reads **sealed segments** (never the live dispatch path), decodes WAL frames, resolves schemas from the registry (0011), writes ZSTD Parquet with column stats, and commits to an Iceberg catalog.
- It runs on a dedicated `danube-lakehouse` Tokio runtime with bounded threads, so Arrow/Parquet work cannot cause latency spikes in message delivery.
- Nodes can be dedicated with `--role lakehouse`; they register as drained and receive no client topics (0010).
- Schema-less topics use a fixed envelope (offset, publish_time, producer_name, routing_key, schema_id, schema_version, payload, attributes_json).

## Why

- Zero-ETL analytics without a separate connector cluster, and without hurting broker latency.

## Known drift

The crate README and `config.rs` comments mention a `sql` catalog, but `src/catalog.rs` only builds `rest`, `glue` and `s3tables` (plus `none`). The runtime config is still a YAML file (`DANUBE_LAKEHOUSE_CONFIG`), an exception to 0006.

## Rules for changes

- Never call into lakehouse code from the dispatcher or the producer path.
- Keep it feature-gated: default builds must not pull Arrow/Parquet/Iceberg.
