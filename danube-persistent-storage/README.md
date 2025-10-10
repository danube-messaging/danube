# Danube Persistent Storage (WAL-first)

This crate implements the WAL-first persistent model for the Danube messaging platform. It provides:

- A Write-Ahead Log (WAL) with:
  - In-memory replay cache for hot reads
  - Optional file-backed durability with CRC32 framing
  - Batched fsync with configurable batch size and flush interval
  - File replay to serve offsets not in cache
- A `PersistentStorage` implementation (`WalStorage`) used by the broker’s `TopicStore`
- A `WalStorageFactory` that encapsulates all storage internals and returns per-topic `WalStorage`:
  - Per-topic WAL instances under `<wal_root>/<ns>/<topic>/`
  - Per-topic background `Uploader` writing objects to cloud and descriptors to metadata

## Features

- WAL frame format: `[u64 offset][u32 len][u32 crc][bincode(StreamMessage)]`
- Replay-from-offset combines file replay and in-memory cache, then switches to live tailing
- Configurable knobs via `WalConfig`:
  - `cache_capacity`: in-memory ring buffer size (messages)
  - `fsync_interval_ms`: max interval before flushing the write buffer
  - `max_batch_bytes`: max batched bytes before forcing a flush
  - `dir`, `file_name`: enable file-backed WAL
- `WalStorageFactory`:
  - `new_with_backend(WalConfig, BackendConfig, MetadataStorage, etcd_root) -> WalStorageFactory`
  - `for_topic("/ns/topic") -> WalStorage` (starts per-topic uploader once)

## Usage

### Broker wiring via WalStorageFactory (recommended)

```rust
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{BackendConfig, LocalBackend, WalStorageFactory};
use danube_metadata_store::MetadataStorage;

// Base WAL config: factory will create per-topic WALs under <wal_root>/<ns>/<topic>/
let wal_base_cfg = WalConfig {
    dir: Some(std::path::PathBuf::from("/var/lib/danube/wal")),
    cache_capacity: Some(1024),
    ..Default::default()
};

// Cloud backend for objects
let backend = BackendConfig::Local { backend: LocalBackend::Fs, root: "/tmp/danube-cloud".to_string() };

// Metadata storage for object descriptors (e.g., etcd or in-memory)
let metadata_store: MetadataStorage = /* constructed in broker */;

// Create factory (constructs CloudStore + EtcdMetadata internally)
let factory = WalStorageFactory::new_with_backend(wal_base_cfg, backend, metadata_store.clone(), "/danube");

// Per-topic storage used by TopicStore
let topic_name = "/default/my-topic";
let storage = factory.for_topic(topic_name).await?;
```

### Per-topic append and reader

```rust
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_core::message::{MessageID, StreamMessage};

let msg = StreamMessage {
    request_id: 1,
    msg_id: MessageID {
        producer_id: 1,
        topic_name: topic_name.to_string(),
        broker_addr: "127.0.0.1:6650".into(),
        topic_offset: 0,
    },
    payload: b"hello".to_vec(),
    publish_time: 0,
    producer_name: "p1".into(),
    subscription_name: None,
    attributes: Default::default(),
};

// Append
storage.append_message(topic_name, msg).await?;

// Reader from offset 0 (Cloud→WAL chaining if historical objects exist)
let mut reader = storage.create_reader(topic_name, StartPosition::Offset(0)).await?;
while let Some(item) = reader.next().await.transpose()? {
    // process item.payload
}
```

## Recommended usage (summary)

- Use `WalStorageFactory` at the broker level to provision per-topic `WalStorage` on demand.
- Configure a base `WalConfig` with a root directory (e.g. `/var/lib/danube/wal`); the factory will create
  per-topic directories `<root>/<ns>/<topic>/` automatically.
- Keep the writer path hot: `Wal::append()` is non-blocking and offloads I/O to a background task.
- For observability, start with `RUST_LOG=info` and raise to `wal=debug` when troubleshooting WAL flows.

## WAL architecture (overview)

```
Writer hot path (append)                             Background writer task (I/O)
------------------------------------------------     -----------------------------------------------
Producer -> Wal::append(msg) ->
  - assign offset
  - insert into in-memory Cache (bounded)
  - enqueue LogCommand::Write {offset, bytes}  --->  [Buf]
                                                     | accumulate framed entries until:
                                                     | - batch size reached OR
                                                     | - fsync interval elapsed
                                                     v
                                                write()/flush() -> fsync
                                                     |
                                                     | rotate if size/time thresholds met
                                                     v
                                                wal.<seq>.log (CRC-framed)
                                                     |
                                                     v
                                                write wal.ckpt (bincode) atomically
```

```
Reader path (catch-up + live tail)
----------------------------------
Wal::tail_reader(from) ->
  - snapshot cache (ordered)
  - if from < cache_start:
      read_file_range(<file>, [from, cache_start))
  - append cache items >= max(from, cache_start)
  - inject WAL offsets into MessageID.topic_offset
  - chain with live broadcast for new appends
```

Key details
- Frame format: `[u64 offset][u32 len][u32 crc][bincode(StreamMessage)]`; CRC mismatch is treated as end-of-log.
- Cache is ordered (BTreeMap), so replay stitching requires no extra sorting.
- Rotation policy is optional (size/time); checkpoints record `last_offset`, `file_seq`, and current file path.

## Components

- `Wal`/`WalConfig`: per-topic WAL instances with optional file durability, replay cache, rotation, checkpoints
- `WalStorage`: per-topic `PersistentStorage` implementing append and reader with Cloud→WAL chaining
- `WalStorageFactory`: process-global facade that creates per-topic `WalStorage` and starts per-topic uploaders
- `CloudStore`: backed by `opendal` with `S3`, `Gcs`, `Fs`, `Memory` implementations
- `EtcdMetadata`: writes/reads per-object descriptors using `danube-metadata-store::MetadataStorage`
- `Uploader`: periodic batches from WAL cache to cloud objects and descriptor updates (single-writer per topic)

## Tests

Run unit/integration tests for this crate:

```bash
cargo test -p danube-persistent-storage --tests
```

## Tracing and Notes

- Durability: CRC-protected frames and batched fsync; file replay truncates at first CRC mismatch for safety.
- Tracing targets:
  - `wal_factory`: per-topic WAL created/reused, uploader started, backend summary
  - `wal_storage`: cloud handoff enabled; reader path (Cloud→WAL vs WAL-only)
  - `wal`: WAL file initialized; effective configuration applied
  - `uploader`: uploader started; resume-from-checkpoint
