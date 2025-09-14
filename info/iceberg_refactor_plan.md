# Danube Iceberg Storage Refactor Plan (v0.6.0 ecosystem)

This document tracks the staged refactor of `danube-iceberg-storage` to adopt the official Apache Iceberg Rust crates for Catalogs and Storage, and to integrate Iceberg writer/arrow utilities. The goal is to replace custom catalog/REST/Glue clients and `object_store`-based IO with the upstream ecosystem (Glue/REST catalogs, `FileIO` backends), while preserving the WAL-based low-latency write path.

## Goals
- Replace custom Catalog layer with adapters that delegate to Apache Iceberg `Catalog` trait and official implementations.
- Use Iceberg’s `FileIO` for storage (S3, FS, Memory), no direct `object_store`.
- Integrate Iceberg writers for commits and readers for scans; keep WAL for fast acks.
- Support S3 and FS immediately; Memory for tests. GCS can be added similarly.

## Current State Summary (after cleanup)
- We now have thin adapters in `src/catalog/` (`rest_catalog.rs`, `glue_catalog.rs`, and a memory adapter in `catalog.rs`) that:
  - Construct official catalogs via `RestCatalogConfig` / `GlueCatalogConfig` builders.
  - Use `NamespaceIdent`, `TableIdent::from_strs`, `TableCreation` for table ops.
  - Expose `load_table_handle()` returning the official `iceberg::table::Table`.
  - Do not expose or convert to custom metadata types anymore.
- Custom types removed: `TableMetadata`, `TableSchema`, `DataFile`, `PlannedFile`, `Snapshot`.
- `TopicWriter` and `TopicReader` now use `FileIO` directly for Parquet read/write and official `Table` for metadata.
- Writer currently stages Parquet files to the warehouse path; append commits will be added next.
- Reader polls snapshots via `table.metadata().current_snapshot_id()`; scan pipeline to be wired.

## Stages

### Stage 0 — Dependencies switch
- Add:
  - `iceberg-catalog-rest = "0.6.0"`
  - `iceberg-catalog-glue = "0.6.0"`
  - Enable Iceberg storage features in `iceberg`:
    - `features = ["storage-s3", "storage-memory", "storage-fs"]`
- Remove:
  - `aws-config`, `aws-sdk-glue`, `reqwest`, and `object_store` (object_store not used by reader/writer anymore).
- Keep `arrow`/`parquet` during the transition.

### Stage 1 — Catalog refactor to official trait/impls
- Use Apache Iceberg `Catalog` trait and concrete catalogs:
  - `iceberg_catalog_rest::RestCatalog`
  - `iceberg_catalog_glue::GlueCatalog`
  - `iceberg::memory::MemoryCatalog` (for tests via adapter)
- `create_catalog()` builds the appropriate adapter which delegates to the official implementations.
- Use the official APIs for `create_table`, `load_table`, `list_tables`, `list_namespaces`, etc.
- Adapters return the official `Table` handle via `load_table_handle()`.
- Status: COMPLETED

### Stage 2 — Storage refactor to Iceberg FileIO (S3/FS/Memory)
- Build `FileIO` using `iceberg::io::FileIOBuilder` per backend and configuration/env.
- Remove direct `object_store` usage from writer/reader/storage.
- Status: COMPLETED
  - `IcebergStorage` constructs and passes an `Arc<FileIO>` to `TopicWriter` and `TopicReader`.
  - Parquet files are read/written via `FileIO::new_input/new_output`.

### Stage 3 — Writer integration with Iceberg (completed)
- Keep WAL for ack latency.
- Goal: in `topic_writer.rs` flush path
  - Convert messages to Arrow `RecordBatch`.
  - Use Iceberg writer/append to commit data files via the official API.
- Current status: COMPLETED
  - Writer now converts to `RecordBatch` and writes Parquet via `FileIO` under the warehouse path.
  - Append commit via `table.new_append().append(...).commit()` is wired.

### Stage 4 — Reader integration with Iceberg (completed)
- Replace planned scan and manual file iteration with `table.scan()`.
- Track `last_snapshot_id` and only process newly added files (incremental).
- Status: COMPLETED
  - Reader currently detects snapshot changes via `table.metadata().current_snapshot_id()`; the actual scan step is wired.

### Stage 5 — Config and features (pending)
- Ensure configs document REST/Glue + S3/FS setups; add Memory catalog/use for tests.

### Stage 6 — Tests and examples (pending)
 - Unit tests with MemoryCatalog + memory storage.
 - Optional integration tests for S3 (gated by env).

### Stage 7 — Build, e2e, docs (pending)
 - Build and fix compile errors across workspace.
 - Run broker e2e with Iceberg config and validate producer/consumer flows.
 - Update `README.md` and `info/danube_iceberg_storage_overview.md` accordingly.

## File Impact Summary
- Keep: `src/catalog.rs` as a thin factory returning `Arc<dyn iceberg::Catalog>` (no custom metadata/adapters).
- Update: `src/iceberg_storage.rs`, `src/topic_writer.rs`, `src/topic_reader.rs`, `src/config.rs`.
- Remove: any remaining references to custom metadata/commit/scan paths and old wrapper traits.

## Risks and Mitigations
 - API mismatches with Iceberg 0.6.0 → Implement in stages; compile and test at each stage.
 - Credentials configuration for CI → Gate cloud tests with env; default to Memory.

## Acceptance Criteria
 - Catalog operations use official crates via adapters; no custom metadata structs.
 - IO uses Iceberg’s `FileIO`; `object_store` removed from data paths.
 - Writer commits via Iceberg append commit; reader uses Iceberg scan/reader.
 - S3 and FS supported; Memory variants pass unit tests.

## Progress Checklist
 - [x] Stage 0: Dependencies switched — iceberg storage features enabled; added iceberg-catalog-rest/glue; removed object_store/reqwest/aws SDKs
 - [x] Stage 1: Catalog refactor complete — adapters delegate to official catalogs; Memory adapter added; `load_table_handle()` available
 - [x] Stage 2: Storage via Iceberg FileIO — reader/writer use FileIO; object_store removed
 - [x] Stage 3: Writer integrated — append commit wired via official API (Transaction::new(&table) + commit via Catalog)
 - [x] Stage 4: Reader integrated — `table.scan().build().plan_files()` + `ArrowReader.read()` stream
 - [ ] Stage 5: Config updated & documented
 - [ ] Stage 6: Tests (Memory + optional S3)
 - [ ] Stage 7: Build, E2E, Docs updated

## Implementation Notes (current state)
- Catalog factory returns `Arc<dyn iceberg::Catalog>` using `iceberg-catalog-rest`, `iceberg-catalog-glue`, or `MemoryCatalog`.
- Arrow versions aligned to Iceberg 0.6.0 transitive deps: `arrow-array = 55.2.0`, `arrow-schema = 55.2.0`.
- TopicWriter uses Iceberg writer APIs (ParquetWriterBuilder, DataFileWriterBuilder) and commits via `Transaction::new(&table)`.
- TopicReader plans tasks with `TableScanBuilder::build()` + `plan_files().await` and streams Arrow batches with `ArrowReader.read(tasks)`.
- Removed direct `parquet` and `object_store` usage in data paths to avoid version conflicts and rely on Iceberg `FileIO`.

## Next Steps
- Add an integration smoke test with REST + FS warehouse: write to WAL → commit to Iceberg → stream back via reader.
- Add metrics/logging for writer commit latency and reader lag; expose via tracing.
- Update README and `info/danube_iceberg_storage_overview.md` to document new configuration and runtime expectations.

## References (verified)
- Glue Catalog crate (docs.rs): https://docs.rs/iceberg-catalog-glue/latest/iceberg_catalog_glue/struct.GlueCatalog.html
- Glue Catalog config (docs.rs): https://docs.rs/iceberg-catalog-glue/latest/iceberg_catalog_glue/struct.GlueCatalogConfig.html
- REST Catalog crate (docs.rs): https://docs.rs/iceberg-catalog-rest/latest/iceberg_catalog_rest/struct.RestCatalog.html
- REST Catalog config (docs.rs): https://docs.rs/iceberg-catalog-rest/latest/iceberg_catalog_rest/struct.RestCatalogConfig.html
- Catalog trait (docs.rs): https://docs.rs/iceberg/latest/iceberg/trait.Catalog.html
- CatalogBuilder/TableCreation: https://docs.rs/iceberg/latest/iceberg/trait.CatalogBuilder.html
- TableBuilder: https://docs.rs/iceberg/latest/iceberg/table/struct.TableBuilder.html
- Memory Catalog: https://docs.rs/iceberg/latest/iceberg/struct.MemoryCatalog.html
- FileIO/Storage (source): https://github.com/apache/iceberg-rust/blob/main/crates/iceberg/src/io/storage.rs
- Iceberg writer API: https://docs.rs/iceberg/latest/iceberg/writer/index.html
- Iceberg Arrow utilities: https://docs.rs/iceberg/latest/iceberg/arrow/index.html
