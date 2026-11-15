# AGENTS.md: danube-iceberg

Feature-gated lakehouse export: sealed WAL segments → Parquet → Iceberg tables. Compiled into the broker only with `--features lakehouse` / `lakehouse-aws`. Read ADR 0013.

## Entry points

- `src/lib.rs`: `LakehouseRuntime` (dedicated `danube-lakehouse` Tokio runtime, idle standby when unconfigured).
- `src/worker.rs`, `src/segment_reader.rs`, `src/checkpoint.rs`: segment discovery, frame decoding, progress tracking.
- `src/schema_resolver.rs`, `src/iceberg_schema.rs`, `src/schema.rs`: registry schemas → Arrow/Iceberg schemas, envelope fallback for schema-less topics.
- `src/writer.rs`, `src/table_manager.rs`, `src/catalog.rs`, `src/storage.rs`: Parquet writing and catalog commits.
- `src/config.rs`: runtime config (still a YAML file via `DANUBE_LAKEHOUSE_CONFIG`, an exception to ADR 0006).

## Invariants

- Read only sealed segments. Never hook into the dispatcher or producer path.
- All heavy work runs on the lakehouse runtime, not the broker runtime.
- Default (non-feature) builds must not depend on Arrow, Parquet or Iceberg.

## Pitfalls

- `config.rs` docs and the README mention a `sql` catalog, but `src/catalog.rs` only builds `rest`, `glue`, `s3tables` (and `none`). Check `catalog.rs` before documenting.
- `--role lakehouse` nodes register as drained, so they receive no client topics.

## Tests

```bash
cargo test -p danube-iceberg     # end_to_end may need a local catalog / object store
```
