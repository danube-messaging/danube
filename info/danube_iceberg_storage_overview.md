# Danube Iceberg Storage — How It Works (User Guide)

This guide explains how Danube’s Iceberg-backed storage works from a user’s point of view. It focuses on:

- The Write Path (what happens when producers send messages)
- The Read Path (how consumers receive messages)
- Multiple subscriptions to the same topic (fan-out and progress)

If you want a lower-level, API-centric view, see the code and comments in:
- `danube-iceberg-storage/src/iceberg_storage.rs`
- `danube-iceberg-storage/src/topic_writer.rs`
- `danube-iceberg-storage/src/topic_reader.rs`

## What is danube-iceberg-storage?

`danube-iceberg-storage` is the persistent storage backend for Danube topics. It stores messages in Apache Iceberg tables on your chosen warehouse (local filesystem or cloud object storage like S3). Iceberg provides ACID snapshots, schema evolution, and interoperability with analytics engines.

## Why Apache Iceberg?

- Snapshots give you point-in-time views and safe, atomic commits.
- Manifests and metadata let us read only what changed since the last snapshot.
- Tables live in your warehouse (S3 or FS), so data is accessible to other tools (Spark, Trino, etc.).

---

## The Write Path (Producer ▶ WAL ▶ Iceberg)

High level: we decouple producer latency from cloud storage latency by writing to a local Write-Ahead Log (WAL) first, then asynchronously batch and commit to Iceberg in the background.

Step-by-step when `store_messages()` is called:

1) WAL append (fast acknowledgment)
- Messages are appended to the local WAL (`wal.rs`) with checksums.
- Producers get quick acks without waiting for cloud I/O.

2) Background batching (TopicWriter)
- A background task per topic (`TopicWriter`) reads entries from the WAL.
- Messages are grouped into Arrow `RecordBatch`es according to `WriterConfig` (batch size, flush interval, memory caps).

3) Parquet file creation
- For each batch, the writer uses Iceberg’s Parquet writers to produce one or more Parquet data files under the topic’s table location.
- We rely on the table’s `FileIO` (provided by the Iceberg table) for actual I/O.

4) Atomic commit as a new snapshot
- The writer opens an Iceberg transaction and does a “fast append” of the produced data files.
- On `commit`, the catalog atomically publishes a new snapshot visible to readers.

What this buys you
- Sub-millisecond producer acks (bounded by local storage), with strong durability guarantees set by your WAL sync mode.
- Efficient, columnar Parquet storage in your warehouse for downstream analytics.

---

## The Read Path (Iceberg ▶ Stream)

High level: consumers follow Iceberg snapshots. When a new snapshot appears for a topic’s table, we read exactly the newly added data files and stream those messages.

Step-by-step when you create a message stream:

1) Start the TopicReader
- A background task per topic (`TopicReader`) periodically polls the Iceberg catalog for the table’s `current_snapshot_id`.

2) Detect new snapshots
- If the `current_snapshot_id` changes, it means the writer committed new data files.

3) Plan the scan (precise incrementals)
- We build a table scan for the specific snapshot and ask Iceberg to plan file tasks.
- The plan returns the set of data files that comprise that snapshot (or delta), so we read only what’s needed.

4) Stream Arrow batches
- We use Iceberg’s Arrow reader to stream `RecordBatch`es from the planned files.
- Batches are converted back into `StreamMessage` and published to a per-topic broadcast channel.

What this buys you
- Consumers see new data as soon as the commit is visible.
- Only newly added files are read, minimizing I/O.

---

## Multiple Subscriptions to the Same Topic (Fan-out)

Danube supports multiple subscribers on the same topic. Each subscriber gets the same stream independently.

How this works in practice:

- Per-topic broadcast
  - `IcebergStorage` creates a `tokio::sync::broadcast::Sender<StreamMessage>` per topic.
  - `TopicReader` publishes decoded messages into this broadcast.

- Per-subscriber receiver
  - Each subscriber gets a fresh broadcast receiver which we bridge to an `mpsc::Receiver<StreamMessage>`.
  - Subscribers can run at different speeds without blocking each other.

- Progress tracking (optional)
  - Storage exposes helpers to record per-subscription progress (e.g., last snapshot/file/offset) in an external metadata store (like etcd).
  - This enables precise restarts without changing the on-disk table layout or producer semantics.

---

## Quickstart Configuration (YAML fragment)

REST + filesystem (local dev)

```yaml
storage:
  type: iceberg
  iceberg_config:
    catalog:
      type: rest
      uri: "http://localhost:8181"
    warehouse: "file:///var/lib/danube/warehouse"
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

Glue + S3 (cloud)

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

Notes
- Use REST for the strongest guarantees with standardized commit/scan APIs.
- MemoryCatalog is available for lightweight testing.

---

## Operational Tips

- Batching: tune `writer.batch_size` and `flush_interval_ms` to balance latency and throughput.
- Reader polling: `reader.poll_interval_ms` controls how quickly new snapshots are detected.
- Warehouse path: choose `file://` for local setups or `s3://` for cloud. Iceberg Table `FileIO` abstracts the I/O for you.

---

## FAQ

- Can I query the data outside Danube?
  - Yes. The data lives in Iceberg tables in your warehouse. You can query with Spark, Trino, etc.

- What guarantees do I get on commit?
  - Iceberg commits are atomic. A snapshot either appears fully or not at all.

- What happens if the writer crashes mid-batch?
  - Uncommitted Parquet files are not visible to readers. On restart, the writer resumes WAL draining and attempts the commit again.

- How do multiple consumers read independently?
  - Each subscription has its own broadcast receiver and optional persisted progress state.

---

## References

- Catalog trait: https://docs.rs/iceberg/latest/iceberg/trait.Catalog.html
- REST Catalog crate: https://docs.rs/iceberg-catalog-rest/latest/iceberg_catalog_rest/
- Glue Catalog crate: https://docs.rs/iceberg-catalog-glue/latest/iceberg_catalog_glue/
- Iceberg Arrow utilities: https://docs.rs/iceberg/latest/iceberg/arrow/index.html
- Writer API: https://docs.rs/iceberg/latest/iceberg/writer/index.html
