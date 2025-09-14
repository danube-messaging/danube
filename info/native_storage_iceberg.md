Danube Technical Documentation: Iceberg-Native Storage Architecture


I. Introduction & Guiding Principles

This document outlines the technical specification for Danube’s Iceberg-native persistence layer. The goal is to provide a cloud-native, highly reliable, and scalable pub/sub platform by leveraging Apache Iceberg and a shared warehouse (filesystem or S3).

This is not a pivot to analytics. We use Iceberg’s transactional table format to achieve ACID commits, safe concurrency, and efficient reads for Danube’s core messaging flows.

The architecture is founded on three key principles:

- Disaggregation of Compute and Storage: Brokers are stateless for persistence; all durable data lives in a shared warehouse. Scale compute and storage independently.
- Low-Latency Writes via Write-Ahead Log (WAL): Producers are acknowledged quickly by appending to a local WAL; background tasks handle durable commits to the warehouse.
- Transactional Durability with Apache Iceberg: Messages are committed atomically as Iceberg snapshots, enabling precise, incremental reads.

II. Core Concepts & Architectural Rationale

The Role of Apache Iceberg

Apache Iceberg is an open table format. It organizes Parquet data and metadata in your warehouse and provides:
- Atomic commits (snapshots)
- Safe concurrency (optimistic concurrency control)
- Schema evolution
- Efficient planning (read only what changed)

The Role of an Iceberg Catalog (REST/Glue/Memory)

- etcd (Operational State): Cluster coordination, topic ownership, optional subscription progress (last processed snapshot/file/offset).
- Iceberg Catalog (Data State): Manages table versions and validates ACID commits. For production, use REST/Glue. For local testing, use a MemoryCatalog.

The Write-Ahead Log (WAL) Pattern

- Store: Append messages to a local WAL (low latency, with CRC32 integrity and configurable fsync).
- Notify: Return an ACK to the producer immediately after a successful WAL append.
- Forward: Background processing (TopicWriter) batches from WAL → Arrow RecordBatch → Parquet → Iceberg commit.

III. Implementation & Crate Structure

Crate: danube-iceberg-storage

Implements `danube-core`’s `PersistentStorage` and wires the following components:

- IcebergStorage
  - Entry point that manages TopicWriter/TopicReader tasks and per-topic broadcast streams.
  - Uses a catalog factory returning `Arc<dyn iceberg::Catalog>` (REST/Glue/Memory).

- WriteAheadLog (wal.rs)
  - Durable local append-only log with file rotation and CRC32.
  - Reader for sequentially draining entries.

- TopicWriter (topic_writer.rs)
  - Batches messages into Arrow `RecordBatch`.
  - Writes Parquet using Iceberg’s writer APIs and commits via `Transaction::new(&table)` → `fast_append()` → `commit(&*catalog)`.

- TopicReader (topic_reader.rs)
  - Polls `table.metadata().current_snapshot_id()` for changes.
  - Plans scan via `table.scan().select_all().snapshot_id(...).build()?.plan_files().await?`.
  - Streams Arrow batches via `table.reader_builder().build().read(tasks).await?` and publishes `StreamMessage` to a per-topic broadcast.

IV. Write and Read Path Logic

The Write Path: Low-Latency Ingestion

- Ingestion: Producers send messages to the broker.
- Reliable Dispatch: Calls `IcebergStorage.store_messages()`.
- WAL Commit: Messages are appended to the local WAL (fsync controlled by config).
- Producer ACK: Returned immediately after WAL append.
- Background Flush: `TopicWriter` builds Arrow batches and writes Parquet using the table’s `FileIO`.
- Iceberg Commit: A transaction fast-appends the new data files and commits a new snapshot atomically.

The Read Path: Streaming from Snapshots

- Subscription: Consumers create a message stream for a topic.
- Snapshot Polling: `TopicReader` periodically fetches `current_snapshot_id()` from table metadata.
- Incremental Scan: On change, the reader builds a scan for that snapshot and calls `plan_files().await` to get the precise set of new data files.
- Data Retrieval & Delivery: Iceberg Arrow reader streams `RecordBatch`es; batches are converted to `StreamMessage` and sent via a per-topic broadcast.
- Cursor Commit (optional): If wired, subscription progress (snapshot/file/offset) is persisted to etcd.

V. Multiple Subscriptions & Fan-out

- Per-topic broadcast: A `tokio::sync::broadcast::Sender<StreamMessage>` is created per topic. `TopicReader` publishes into this channel.
- Independent consumers: Each subscription gets its own receiver bridged to an `mpsc::Receiver<StreamMessage>`; slow subscribers do not block fast ones.
- Optional progress: Storage-side helpers allow persisting per-subscription progress in etcd for precise restarts.

VI. Notes & Best Practices

- Prefer REST Catalog for strongest guarantees and standardized APIs.
- Align Arrow crates with Iceberg 0.6.0 (`arrow-array = 55.2.0`, `arrow-schema = 55.2.0`).
- Tune batching and polling to balance producer latency and consumer freshness.
- Data is stored in Iceberg tables in your warehouse; you can query it with analytics engines.