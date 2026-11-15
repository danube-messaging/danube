# AGENTS.md: danube-persistent-storage

Storage engine for reliable topics: local WAL, segment export to durable storage, Valkey warm tier, recovery. Read ADRs 0001, 0003 and 0004 before changing behaviour.

## Entry points

- `src/wal/` (`writer.rs`, `streaming_reader.rs`, `cache.rs`, `deleter.rs`): append path with group commit, readers and local retention.
- `src/wal_storage.rs` (`WalStorage`, hot tier) and `src/tiered_storage.rs` (`TieredStorage`, the `PersistentStorage` impl the broker uses).
- `src/storage_factory/`: per-topic storage creation, `segment_export.rs` (seal and export), `recovery.rs`, `lifecycle.rs`.
- `src/durable_store.rs`, `src/object_store_backend/`: shared FS and OpenDAL object stores (S3, GCS, Azure).
- `src/durable_history_reader.rs`: reads exported history for replay, moves and branching.
- `src/metadata/`: segment descriptors stored in the metadata store (Raft).
- `src/valkey/`: write buffer client, rotation listener (trim after export), stream reader (warm reads), recovery replay.

## Invariants

- The WAL is append-only. Space is reclaimed by deleting whole files and segments, and local files are deleted only after they have been exported (in `shared_fs` / `object_store` modes).
- WAL sync levels: `Async`, `OsCacheFlush` (`event_stream`), `FsyncBatch` (fdatasync, `task_queue` / `audit_log`). Do not weaken a level silently.
- Segments are immutable once sealed and reference-counted (topic branching shares them).
- Recovery order: sealed segments → local WAL → Valkey replay → durable catalog → empty. Offsets must stay continuous across restarts and owner changes.
- Valkey is optional. Every code path must work with it disabled.

## Pitfalls

- Object-store roots need a scheme (`s3://bucket/prefix`). A bare string is treated as a bucket name.
- `local` mode has no shared history, so owner changes cannot preserve data. Tests for moves use `shared_fs`.
- "Delete on ack" retention (ADR 0004) is planned. If you implement it, drive it from subscription cursors and make abandoned subscriptions a policy decision.

## Tests

```bash
cargo test -p danube-persistent-storage --lib
cargo test -p danube-persistent-storage --test <name>   # write_buffer_valkey needs a Valkey instance
```
