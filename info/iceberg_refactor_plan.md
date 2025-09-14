# Danube Iceberg Storage Refactor Plan (v0.6.0 ecosystem)

This document tracks the staged refactor of `danube-iceberg-storage` to adopt the official Apache Iceberg Rust crates for Catalogs and Storage, and to integrate Iceberg writer/arrow utilities. The goal is to replace custom catalog/REST/Glue clients and `object_store`-based IO with the upstream ecosystem (Glue/REST catalogs, `opendal` storage), while preserving the WAL-based low-latency write path.

## Goals
- Replace custom Catalog trait/impls with Apache Iceberg `Catalog` trait and official implementations.
- Switch storage IO from `object_store` to Iceberg’s built-in FileIO via feature flags (no direct `opendal` dependency).
- Integrate Iceberg writers for commits and readers for scans; keep WAL for fast acks.
- Support S3 and GCS as primary backends; Memory variants for tests.

## Current State Summary (pre-refactor)
- Custom catalog layer in `danube-iceberg-storage/src/catalog.rs` with `rest_catalog.rs` and `glue_catalog.rs` (using `reqwest` and `aws-sdk-glue`).
- Direct `object_store` usage in `iceberg_storage.rs`, `topic_writer.rs`, `topic_reader.rs` for S3/local, manual Parquet write/read and custom REST commit.
- Config supports `CatalogConfig::{Rest, Glue}` and `ObjectStoreConfig::{S3, Local}` only.

## Stages

### Stage 0 — Dependencies switch
- Add:
  - `iceberg-catalog-rest = "0.6.0"`
  - `iceberg-catalog-glue = "0.6.0"`
  - Enable Iceberg storage features in `iceberg` (no direct `opendal` dependency):
    - `features = ["storage-s3", "storage-gcs", "storage-memory", "storage-fs"]`
- Remove:
  - `aws-config`, `aws-sdk-glue`, `reqwest`, and `object_store` (object_store may linger temporarily if needed in transition; target removal).
- Keep `arrow`/`parquet` temporarily.

Example Cargo.toml excerpt:

```toml
[dependencies]
iceberg = { version = "0.6.0", features = ["storage-s3", "storage-gcs", "storage-memory", "storage-fs"] }
iceberg-catalog-rest = "0.6.0"
iceberg-catalog-glue = "0.6.0"
# remove: object_store, reqwest, aws-config, aws-sdk-glue
```

### Stage 1 — Catalog refactor to official trait/impls
- Remove `src/catalog.rs`, `src/catalog/rest_catalog.rs`, `src/catalog/glue_catalog.rs`.
- Use Apache Iceberg `Catalog` trait and concrete catalogs:
  - `iceberg_catalog_rest::RestCatalog`
  - `iceberg_catalog_glue::GlueCatalog`
  - `iceberg::catalog::memory::MemoryCatalog` (for tests)
- In `src/iceberg_storage.rs`, construct catalogs from `CatalogConfig` and the configured warehouse.
- Add `CatalogConfig::Memory` to config for tests.
- Use the official `Catalog` trait (do not keep our own). Update call sites accordingly.

Example (conceptual) usage:
```rust
use iceberg::catalog::{Catalog, TableIdent};
use iceberg_catalog_rest::RestCatalog;
use iceberg_catalog_glue::GlueCatalog;
// Memory catalog for tests:
use iceberg::catalog::memory::MemoryCatalog;

// Rest
let rest = RestCatalog::new(rest_uri, Some(warehouse), None).await?;
// Glue (constructor per docs.rs)
let glue = GlueCatalog::new(glue_region, Some(warehouse), None).await?;
// Memory
let file_io = iceberg::io::FileIOBuilder::new("memory").build()?;
let mem = MemoryCatalog::new(file_io, None);
```

### Stage 2 — Storage refactor to Iceberg FileIO (S3/GCS/Memory)
- Do NOT add a direct dependency on `opendal`. Iceberg integrates storage backends behind its `FileIO`.
- Build `FileIO` using `iceberg::io::FileIOBuilder` per backend and configuration/env:
  - S3: `FileIOBuilder::new("s3")` with appropriate props/env.
  - GCS: `FileIOBuilder::new("gcs")` with props like `GCS_SERVICE_PATH`, `GCS_NO_AUTH` for tests.
  - Memory: `FileIOBuilder::new("memory")` for tests.
- Remove direct `object_store` usage from writer/reader/storage. Obtain IO from Table/Catalog context (or pass constructed `FileIO` when constructing catalogs/tables as supported by APIs).

Example (from upstream tests/README patterns):
```rust
use iceberg::io::{FileIO, FileIOBuilder};

// S3
let s3_io = FileIOBuilder::new("s3").build()?; // relies on env like AWS_ACCESS_KEY_ID/SECRET/REGION

// GCS (fake server example)
use iceberg::io::{GCS_NO_AUTH, GCS_SERVICE_PATH};
let gcs_io = FileIOBuilder::new("gcs")
    .with_props(vec![(GCS_SERVICE_PATH, "http://127.0.0.1:4443".to_string()),
                     (GCS_NO_AUTH, "true".to_string())])
    .build()?;

// Memory
let mem_io = FileIOBuilder::new("memory").build()?;
```

### Stage 3 — Writer integration with Iceberg
- Keep WAL for ack latency.
- In `topic_writer.rs`, when flushing a batch:
  - Convert messages to Arrow `RecordBatch`.
  - Use Iceberg writer API (`iceberg::writer`) to produce data files via table IO.
  - Commit with `table.new_append().append(data_file).commit().await?`.
- Remove manual `parquet::ArrowWriter` + upload + custom REST `commit_add_files`.

### Stage 4 — Reader integration with Iceberg
- Replace custom scan planning and `parquet` reading with Iceberg’s `table.scan()` + reader.
- Track `last_snapshot_id` and read only newly added files (incremental approach). Use `seen_files` guard during transition if needed.
- Convert Arrow batches to `StreamMessage` (consider `iceberg::arrow` helpers for schema mapping).

### Stage 5 — Config and features
- Extend `src/config.rs`:
  - Add `CatalogConfig::Memory`.
  - Add `ObjectStoreConfig::{Gcs, Memory}`.
- Document env variables for credentials:
  - S3: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, optional `AWS_ENDPOINT`, `AWS_S3_FORCE_PATH_STYLE`.
  - GCS: `GOOGLE_APPLICATION_CREDENTIALS` (path) or inline JSON key.

### Stage 6 — Tests and examples
- Unit tests with MemoryCatalog + memory storage.
- Optional integration tests for S3/GCS gated by env, following upstream examples:
  - `crates/iceberg/tests/file_io_s3_test.rs`
  - `crates/iceberg/tests/file_io_gcs_test.rs`
- Update `config/danube_broker_iceberg.yml` samples for REST/Glue + S3/GCS via opendal.

### Stage 7 — Build, e2e, docs
- Build and fix compile errors across workspace.
- Run broker e2e with Iceberg config and validate producer/consumer flows.
- Update `README.md` in `danube-iceberg-storage/` and `info/danube_iceberg_storage_overview.md` accordingly.

## File Impact Summary
- Remove: `src/catalog/*`.
- Update: `Cargo.toml`, `src/iceberg_storage.rs`, `src/topic_writer.rs`, `src/topic_reader.rs`, `src/config.rs`.
- Add tests: `danube-iceberg-storage/tests/*`.

## Risks and Mitigations
- API mismatches with Iceberg 0.6.0 → Implement in stages; compile and test at each stage.
- Credentials configuration for CI → Gate cloud tests with env; default to Memory.

## Acceptance Criteria
- Catalog operations use official crates; no custom REST/Glue code remains.
- IO uses Iceberg’s built-in FileIO; `object_store` removed.
- Writer commits via Iceberg append commit; reader uses Iceberg scan/reader.
- S3 and GCS supported; Memory variants pass unit tests.

## Progress Checklist
- [ ] Stage 0: Dependencies switched
- [ ] Stage 1: Catalog refactor complete
- [ ] Stage 2: Storage via Iceberg FileIO
- [ ] Stage 3: Writer integrated
- [ ] Stage 4: Reader integrated
- [ ] Stage 5: Config updated & documented
- [ ] Stage 6: Tests (Memory + optional S3/GCS)
- [ ] Stage 7: Build, E2E, Docs updated

## References (verified)
- Glue Catalog crate (docs.rs): https://docs.rs/iceberg-catalog-glue/latest/iceberg_catalog_glue/struct.GlueCatalog.html
- REST Catalog crate (docs.rs): https://docs.rs/iceberg-catalog-rest/latest/iceberg_catalog_rest/struct.RestCatalog.html
- Memory Catalog (source): https://github.com/apache/iceberg-rust/blob/main/crates/iceberg/src/catalog/memory/catalog.rs
- Catalog trait (source): https://github.com/apache/iceberg-rust/blob/main/crates/iceberg/src/catalog/mod.rs
- Iceberg FileIO/Storage: https://github.com/apache/iceberg-rust/blob/main/crates/iceberg/src/io/storage.rs
- Iceberg README (features for storage backends): https://github.com/apache/iceberg-rust/blob/main/crates/iceberg/README.md
- S3 FileIO test: https://github.com/apache/iceberg-rust/blob/main/crates/iceberg/tests/file_io_s3_test.rs
- GCS FileIO test: https://github.com/apache/iceberg-rust/blob/main/crates/iceberg/tests/file_io_gcs_test.rs
- Iceberg writer API: https://docs.rs/iceberg/latest/iceberg/writer/index.html
- Iceberg Arrow utilities: https://docs.rs/iceberg/latest/iceberg/arrow/index.html
