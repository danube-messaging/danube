# Danube Persistent Storage (WAL-first)

This crate implements the WAL-first persistent model for the Danube messaging platform. It provides:

- A Write-Ahead Log (WAL) with:
  - In-memory replay cache for hot reads
  - Optional file-backed durability with CRC32 framing
  - Batched fsync with configurable batch size and flush interval
  - File replay to serve offsets not in cache
- A `PersistentStorage` implementation (`WalStorage`) that integrates with `danube-reliable-dispatch`
- Phase B scaffolding for background upload to cloud and ETCD manifests

## Features

- WAL frame format: `[u64 offset][u32 len][u32 crc][bincode(StreamMessage)]`
- Replay-from-offset combines file replay and in-memory cache, then switches to live tailing
- Configurable knobs via `WalConfig`:
  - `cache_capacity`: in-memory ring buffer size (messages)
  - `fsync_interval_ms`: max interval before flushing the write buffer
  - `max_batch_bytes`: max batched bytes before forcing a flush
  - `dir`, `file_name`: enable file-backed WAL

## Usage

### Constructing a WAL with durability

```rust
use danube_persistent_storage::wal::{Wal, WalConfig};

let wal = Wal::with_config(WalConfig {
    dir: Some(std::path::PathBuf::from("/tmp/wal")),
    file_name: Some("wal.log".into()),
    cache_capacity: Some(1024),
    fsync_interval_ms: Some(5),
    max_batch_bytes: Some(8 * 1024),
}).await?;
```

### Wiring with ReliableDispatch

Use `WalStorage::from_wal(wal)` and `ReliableDispatch::new_with_persistent(...)`:

```rust
use danube_persistent_storage::WalStorage;
use danube_reliable_dispatch::{ReliableDispatch, TopicCache};
use danube_reliable_dispatch::storage_backend::InMemoryStorage;
use danube_core::dispatch_strategy::{ReliableOptions, RetentionPolicy};

let wal_storage = WalStorage::from_wal(wal);

let storage = std::sync::Arc::new(InMemoryStorage::new());
let topic_cache = TopicCache::new(storage, 100, 10);
let reliable_options = ReliableOptions::new(1, RetentionPolicy::RetainUntilAck, 60);

let dispatch = ReliableDispatch::new_with_persistent(
    "/default/my-topic",
    reliable_options,
    topic_cache,
    wal_storage,
);
```

See the runnable example: `danube-reliable-dispatch/examples/wal_wiring.rs`.

### Reading and Writing

- Producer path (append): `ReliableDispatch::store_message(StreamMessage)`
- Consumer path (tail): `ReliableDispatch::create_stream_latest()`
- Consumer path (replay): `TopicStore::create_reader(StartPosition::Offset(n))`

## Phase B Scaffolding

- `CloudStore` (stubbed): backends for `S3`, `Gcs`, `Fs`, `Memory` with `put_object/get_object`
- `EtcdMetadata`: uses `danube-metadata-store::MetadataStorage` to write/read per-object manifests
- `Uploader`: periodic snapshot from WAL cache and write stub object + descriptor (single-writer assumption)

These will be wired to `opendal` (cloud object storage) and extended with rolling object rotation and ETag checks.

## Tests and Examples

- Run unit/integration tests for this crate:

```bash
cargo test -p danube-persistent-storage --tests
```

- Run the WAL wiring example:

```bash
cargo run -p danube-reliable-dispatch --example wal_wiring
```

## Notes

- The current implementation provides durability with CRC-protected frames and batched fsync, with replay support from file.
- File replay on partial/corrupted frames truncates at the first CRC mismatch as a safety measure.
- Phase B will add object storage uploads and ETCD manifests using the existing metadata store; leader leases are not used (single-writer broker per topic).
