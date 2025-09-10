# Danube Iceberg Storage

Apache Iceberg-based storage backend for the Danube messaging platform.

## Overview

This crate provides a cloud-native storage solution for Danube using Apache Iceberg table format with Write-Ahead Log (WAL) for low-latency producer acknowledgments and asynchronous background processing to Iceberg tables on object storage.

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

### TopicReader
- **Snapshot polling**: Continuously polls Iceberg catalog for new snapshots
- **Incremental processing**: Only processes new data files since last snapshot
- **Streaming**: Converts Parquet data back to StreamMessages for consumers

### IcebergStorage
- **PersistentStorage trait**: New trait replacing the segment-based StorageBackend
- **Task management**: Manages TopicWriter and TopicReader tasks per topic
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
- **Transactional**: ACID guarantees through Apache Iceberg
- **Scalable**: Stateless brokers with shared object storage
- **Analytics-ready**: Data stored in open Parquet format for analytics
- **Schema evolution**: Support for message schema changes over time

## Usage

```rust
use danube_iceberg_storage::{IcebergStorage, IcebergConfig};

// Create storage instance
let config = IcebergConfig { /* ... */ };
let storage = IcebergStorage::new(config).await?;

// Store messages (fast WAL write)
let messages = vec![/* StreamMessage instances */];
storage.store("my-topic", messages).await?;

// Initialize topic (creates background tasks)
storage.initialize_topic("my-topic").await?;

// Get statistics
let stats = storage.get_topic_stats("my-topic").await?;
println!("Messages: {}, Bytes: {}", stats.message_count, stats.bytes_stored);

// Graceful shutdown
storage.shutdown().await?;
```

## Development Status

This crate is currently under active development. The core WAL and storage infrastructure is complete, but Iceberg catalog integration is still being implemented.

### Completed Components
- ✅ WriteAheadLog with file rotation and checksums
- ✅ IcebergStorage with PersistentStorage trait
- ✅ TopicWriter for WAL to Parquet conversion
- ✅ TopicReader for Parquet to message streaming
- ✅ Comprehensive configuration system
- ✅ Object store integration (S3, MinIO, local)

### In Progress
- 🔄 Iceberg catalog integration (AWS Glue, REST)
- 🔄 Atomic snapshot commits
- 🔄 Integration with danube-reliable-dispatch

### Planned
- 📋 Performance optimizations
- 📋 Comprehensive testing
- 📋 Metrics and monitoring
- 📋 Schema evolution support

## Dependencies

- **iceberg**: Apache Iceberg Rust SDK
- **arrow**: In-memory columnar data representation
- **parquet**: Parquet file format support
- **object_store**: Unified object storage API
- **tokio**: Async runtime

## License

Licensed under the Apache License, Version 2.0.
