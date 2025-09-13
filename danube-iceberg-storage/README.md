# Danube Iceberg Storage

Apache Iceberg-based storage backend for the Danube messaging platform.

## Overview

This crate provides a cloud-native storage solution for Danube using Apache Iceberg table format with Write-Ahead Log (WAL) for low-latency producer acknowledgments and asynchronous background processing to Iceberg tables on object storage.

For a deeper architecture overview, see: `info/danube_iceberg_storage_overview.md`.

## Architecture

The Iceberg storage backend consists of several key components:

### Write-Ahead Log (WAL)
- **Fast, durable writes**: Messages are first written to a local WAL for immediate producer acknowledgment
- **File rotation**: WAL files are rotated based on size limits to prevent unbounded growth
- **Checksums**: CRC32 checksums ensure data integrity
- **Configurable sync modes**: Balance between durability and performance

### TopicWriter
- **Asynchronous processing**: Background task that reads from WAL and writes to Iceberg
- **Batching**: Messages are batched for efficient Parquet file creation
- **Arrow integration**: Messages are converted to Arrow RecordBatch format
- **Object storage**: Parquet files are written to S3/MinIO/local storage
- **REST commit**: Uses Iceberg REST `commit` to atomically add Parquet files as a new snapshot

### TopicReader
- **Snapshot polling**: Continuously polls Iceberg catalog for new snapshots
- **Precise incrementals**: Uses Iceberg REST `scan` planning to enumerate added files between snapshots
- **Streaming**: Converts Parquet data back to StreamMessages for consumers
- **Concurrency & prefetch**: Bounded prefetch queue and semaphore-limited file reads

### IcebergStorage
- **PersistentStorage trait**: Implements `danube-core` PersistentStorage
- **Task management**: Manages TopicWriter and TopicReader tasks per topic
- **Fan-out**: Per-topic broadcast so multiple subscribers receive the same stream independently
- **Subscription progress (scaffold)**: Optional etcd-backed helpers for per-subscription progress
- **Configuration**: Comprehensive configuration for all components

## Configuration

```yaml
storage:
  type: iceberg
  iceberg_config:
    catalog:
      type: rest
      uri: "http://localhost:8181"
    object_store:
      type: s3
      bucket: "danube-data"
      region: "us-west-2"
    wal:
      base_path: "/var/lib/danube/wal"
      max_file_size: 67108864  # 64MB
      sync_mode: periodic
    writer:
      batch_size: 1000
      flush_interval_ms: 5000
      max_memory_bytes: 134217728  # 128MB
    reader:
      poll_interval_ms: 1000
      max_concurrent_reads: 10
      prefetch_size: 5
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
- ⚠️ AWS Glue catalog: basic CRUD only; commit and scan planning are not implemented in Glue. Use REST catalog for ACID commits and precise reads.
- ⚠️ Broker wiring for per-subscription etcd progress is pending (helpers available in storage).

### Planned
- 📋 Catalog auth (REST token), AWS credentials/profile; retries/backoff
- 📋 `delete_topic()` to drop table and purge paths
- 📋 Metrics and monitoring
- 📋 Schema evolution support
- 📋 Integration tests (local + MinIO/REST) and performance benchmarks

## Dependencies

- **iceberg**: Apache Iceberg Rust SDK
- **arrow**: In-memory columnar data representation
- **parquet**: Parquet file format support
- **object_store**: Unified object storage API
- **danube-metadata-store**: Optional etcd-backed metadata helpers (subscription progress)
- **tokio**: Async runtime

## License

Licensed under the Apache License, Version 2.0.
