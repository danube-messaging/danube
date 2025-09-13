# Danube Iceberg Storage: Architecture Overview

## 1) Role of the crate in Danube messaging

`danube-iceberg-storage` is the persistent storage backend for Danube topics. It provides:

- Durable, scalable storage for messages using Apache Iceberg tables stored in an object store (S3/MinIO/local).
- Low-latency producer acknowledgments via a local Write-Ahead Log (WAL), decoupling ingestion from cloud latency.
- Asynchronous background processing that batches messages into Parquet and commits them atomically as Iceberg snapshots.
- Streaming reads that follow Iceberg snapshots and enumerate exactly which data files were added, enabling precise, incremental consumption.

This crate implements the `PersistentStorage` trait from `danube-core/src/storage.rs` and integrates with the broker to provide topic lifecycle, produce, and consume flows.

Key components and files:
- WAL: `danube-iceberg-storage/src/wal.rs`
- Catalog (REST/Glue): `danube-iceberg-storage/src/catalog.rs`, `catalog/rest_catalog.rs`, `catalog/glue_catalog.rs`
- Topic Writer: `danube-iceberg-storage/src/topic_writer.rs`
- Topic Reader: `danube-iceberg-storage/src/topic_reader.rs`
- Storage facade: `danube-iceberg-storage/src/iceberg_storage.rs`


## 2) Technical architecture and technology choices

Why these technologies and how they interact:

- Apache Iceberg (metadata and table format)
  - Why: Iceberg provides an open table format with ACID commits, snapshots, manifests, and schema evolution. This enables safe concurrent writers, versioned history, and analytics interoperability.
  - How we use it: We integrate with an Iceberg catalog. For production-grade correctness we use the REST Catalog for commits and scan planning. Writers commit Parquet files as new snapshots; readers use scan planning to enumerate added files between snapshots.
  - Where in code: `catalog/` and calls from `topic_writer.rs` and `topic_reader.rs`.

- Apache Arrow (in-memory) and Parquet (on-disk)
  - Why: Arrow is an efficient in-memory columnar format; Parquet is a columnar file format optimized for storage and analytics. Together they provide great compression and fast reads/writes, while keeping data queryable by external engines.
  - How we use them:
    - Writer converts batches of `StreamMessage` into Arrow `RecordBatch` and writes Parquet.
    - Reader opens Parquet, reads Arrow batches, and reconstructs `StreamMessage`.
  - Where in code: Arrow/Parquet conversion logic resides in `topic_writer.rs` and `topic_reader.rs`.

- object_store (unified IO over S3/MinIO/local)
  - Why: A common abstraction to read/write bytes regardless of the backing store (S3, MinIO, local FS). This keeps the code portable and testable.
  - How we use it:
    - Writer uploads newly created Parquet files to `{warehouse}/{topic}/...`.
    - Reader downloads Parquet files identified by Iceberg scan planning to decode into messages.
  - Where in code: Object store is created in `iceberg_storage.rs`, used by `topic_writer.rs` and `topic_reader.rs`.

- Write-Ahead Log (WAL)
  - Why: To acknowledge producers immediately without waiting on object storage and Iceberg commit latency.
  - How we use it: Producers append to the WAL; a background writer drains the WAL, batches messages, and flushes to Parquet + Iceberg commit.
  - Where in code: `wal.rs` and consumed by `topic_writer.rs`.

- Catalog (REST or Glue)
  - Why: The catalog is the source of truth for Iceberg metadata (table location, snapshots, manifests). REST provides the standardized commit and scan APIs we rely on for ACID semantics and precise incrementals.
  - How we use it:
    - Commits: `commit_add_files` via REST to atomically add Parquet files as a new snapshot.
    - Reads: `plan_scan(from_snapshot_id, to_snapshot_id)` via REST to get the exact set of added/deleted files.
  - Where in code: `catalog/rest_catalog.rs` implements these endpoints. `catalog/glue_catalog.rs` provides basic table management; commit/scan are intentionally marked as not implemented.

Interaction summary:
- Writer path: WAL -> Arrow batch -> Parquet file (object_store) -> Iceberg REST commit -> new snapshot.
- Reader path: Iceberg REST scan plan -> object_store Parquet reads -> Arrow batch -> `StreamMessage`.


## 3) Detailed workflows and component roles

Writer workflow (TopicWriter)
- Source: The Writer reads from the WAL (`wal.rs`).
- Batching: Accumulates messages based on `WriterConfig` (batch size, flush interval, memory cap).
- Conversion: Converts messages to Arrow `RecordBatch` and writes a Parquet file to the configured object store.
- Commit: Calls the catalog’s `commit_add_files` to atomically publish the new Parquet file(s) as an Iceberg snapshot.
- Files/paths: `topic_writer.rs` (writer state machine), `catalog/*.rs` for commits, object store configured in `iceberg_storage.rs`.

Reader workflow (TopicReader)
- Polling: Periodically loads table metadata to detect a new `current_snapshot_id`.
- Planning: Uses `plan_scan(from_snapshot_id, to_snapshot_id)` to enumerate precisely the added data files for the new snapshot.
- Reading: Fetches Parquet files from the object store, decodes to Arrow `RecordBatch`, converts rows to `StreamMessage`.
- Delivery: Pushes messages into a per-topic broadcast channel so multiple subscribers can consume independently.
- Concurrency/prefetch: Uses a semaphore to control `max_concurrent_reads` and a bounded prefetch queue (`prefetch_size`).
- Files/paths: `topic_reader.rs` reads and publishes; broadcast wiring lives in `iceberg_storage.rs`.

Multiple subscriptions to the same topic
- Fan-out via broadcast:
  - `iceberg_storage.rs` creates a `tokio::sync::broadcast::Sender<StreamMessage>` per topic.
  - `TopicReader` publishes decoded messages into this broadcast.
  - Each subscription obtains its own `broadcast::Receiver`, which is then bridged to an `mpsc::Receiver<StreamMessage>` for consumer APIs.
- Backpressure & buffering:
  - The reader’s prefetch queue is bounded to contain memory usage.
  - Slow subscribers can lag independently; the broadcast layer isolates them.
- Subscription progress (planned wiring via broker):
  - Storage exposes helpers to persist per-subscription progress in etcd under `/persistent_storage/iceberg/subscriptions/{subscription}/{namespace}/{topic}` (see `iceberg_storage.rs`).
  - The broker can call these helpers on ack/resume to achieve precise restarts. This design avoids modifying existing metadata paths.

End-to-end view
- Producers write to WAL and get fast acks.
- Background writer converts WAL to Parquet and commits a new Iceberg snapshot.
- Reader detects the new snapshot, plans the scan, and streams messages to subscribers.
- Subscribers can be many per topic, each with independent progress tracking.
