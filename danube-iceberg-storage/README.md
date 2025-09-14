# Danube Iceberg Storage

Apache Iceberg-based storage backend for the Danube messaging platform.

## Overview

This crate provides a cloud-native storage solution for Danube using Apache Iceberg table format with Write-Ahead Log (WAL) for low-latency producer acknowledgments and asynchronous background processing to Iceberg tables on object storage.

For a deeper architecture overview, see: `info/danube_iceberg_storage_overview.md`.

## Architecture (Updated)

- **Official Iceberg Catalog**: Uses `Arc<dyn iceberg::Catalog>` via `iceberg-catalog-rest`, `iceberg-catalog-glue`, or `MemoryCatalog`.
- **Transactions and Snapshots**: Writer commits batches as new snapshots; Reader scans and streams new data per snapshot.
- **Arrow + Parquet**: Writer converts messages to Arrow `RecordBatch` and writes Parquet via Iceberg writer APIs. Reader streams Arrow record batches via Iceberg Arrow utilities.
- **Dependency alignment**: Arrow crates aligned with Iceberg 0.6.0 transitive deps to avoid type mismatches.

### Write-Ahead Log (WAL)
- Fast, durable writes (local WAL)
- File rotation and CRC32 checksums
- Configurable sync modes (Always/Periodic/None)

### TopicWriter
- Batches messages into Arrow `RecordBatch`
- Loads `Table` via `catalog.load_table(&TableIdent)`
- Builds Parquet data files via `ParquetWriterBuilder` + `DataFileWriterBuilder`
- Commits via Iceberg transactions: `Transaction::new(&table) -> fast_append().add_data_files(...).apply(tx) -> tx.commit(&*catalog).await?`

### TopicReader
- Polls `table.metadata().current_snapshot_id()`
- Plans scan for a snapshot: `table.scan().select_all().snapshot_id(...).build()?.plan_files().await?`
- Streams Arrow batches: `table.reader_builder().build().read(tasks).await?`
- Publishes `StreamMessage` via per-topic broadcast channel

### IcebergStorage
- Implements `danube-core` `PersistentStorage`
- Manages topic writers/readers and per-topic broadcast
- Optional etcd-backed helpers for subscription progress

## Configuration

Example REST + S3/FS configuration (YAML fragment):

```yaml
storage:
  type: iceberg
  iceberg_config:
    catalog:
      type: rest
      uri: "http://localhost:8181"
    # Warehouse points to FS or S3
    warehouse: "file:///var/lib/danube/warehouse" # or s3://my-bucket/prefix
    wal:
      base_path: "/var/lib/danube/wal"
      max_file_size: 104857600
      sync_mode: Always
    writer:
      batch_size: 1000
      flush_interval_ms: 1000
      max_memory_bytes: 67108864
    reader:
      poll_interval_ms: 500
      max_concurrent_reads: 5
      prefetch_size: 3
```

Glue example (the SDK/env provides region/credentials):

```yaml
storage:
  type: iceberg
  iceberg_config:
    catalog:
      type: glue
    warehouse: "s3://danube-warehouse"
    wal:
      base_path: "/var/lib/danube/wal"
      max_file_size: 104857600
      sync_mode: Always
```

## Features

- **Low-latency writes**: Sub-millisecond producer acknowledgments via WAL
- **Cloud-native**: Native integration with S3, MinIO, and other object stores
- **Transactional**: ACID guarantees through Apache Iceberg REST commits
- **Precise reads**: Manifest-based incremental scans via REST scan planning
- **Scalable**: Stateless brokers with shared object storage
- **Analytics-ready**: Data stored in open Parquet format for analytics
- **Schema evolution**: Support for message schema changes over time

## Usage

```rust
use danube_iceberg_storage::{IcebergStorage, IcebergConfig};
use danube_core::message::StreamMessage;

// Create storage instance
let config = IcebergConfig { /* ... */ };
let storage = IcebergStorage::new(config).await?;

// Create topic (starts background reader/writer tasks lazily)
storage.create_topic("my-topic").await?;

// Store messages (fast WAL write)
let messages: Vec<StreamMessage> = vec![/* ... */];
storage.store_messages("my-topic", messages).await?;

// Create a consumer stream (fan-out receiver)
let mut rx = storage.create_message_stream("my-topic", None).await?;
while let Some(msg) = rx.recv().await {
    // process msg
}

// Positions
let write_pos = storage.get_write_position("my-topic").await?;
let committed_pos = storage.get_committed_position("my-topic").await?;

// Graceful shutdown
storage.shutdown().await?;
```

## Development Status

The crate implements the core WAL, writer/reader, and REST catalog flows. Current status:

### Completed Components
- ✅ WriteAheadLog with file rotation and checksums
- ✅ IcebergStorage with PersistentStorage trait
- ✅ TopicWriter for WAL → Arrow/Parquet → REST commit (atomic snapshot)
- ✅ TopicReader for REST scan planning → Parquet → messages (precise incrementals)
- ✅ Comprehensive configuration system
- ✅ Object store integration (S3, MinIO, local)
- ✅ Optional etcd progress helpers (storage-side scaffolding)

### In Progress / Limitations
- ⚠️ Broker wiring for per-subscription etcd progress is pending (helpers available in storage).

### Planned
- 📋 Catalog auth (REST token), AWS credentials/profile; retries/backoff
- 📋 `delete_topic()` to drop table and purge paths
- 📋 Metrics and monitoring
- 📋 Schema evolution support
- 📋 Integration tests (local + MinIO/REST) and performance benchmarks

