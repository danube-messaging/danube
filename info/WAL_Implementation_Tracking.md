# Danube WAL + Cloud Persistence Implementation Tracking Plan

This file tracks the end-to-end implementation progress for the WAL-first storage with cloud persistence via opendal. Update this file as work progresses. The design reference is `info/WAL_Cloud_Persistence_Design.md`.

## Legend
- [ ] Pending
- [~] In progress
- [x] Done

## Phased Implementation Plan

- [x] Phase A: WAL + TopicStore streaming hot path
  - Objectives:
    - Introduce per-topic `WAL` with integrated `WALCache` for sub-ms appends and tail reads.
    - Expose `TopicStore::create_reader(topic, start: StartPosition)` returning an async stream; dispatch consumes `TopicStream` (no segments on hot path).
    - Implement `StartPosition` enum: `Latest` | `Offset(u64)`; `SubscriptionDispatch` selects start, `TopicStore` materializes it.
  - Scope & Tasks:
    - [x] danube-core: Define `PersistentStorage` trait (append/create_reader/ack_checkpoint/flush) and temporary adapter for legacy `StorageBackend`.
    - [x] danube-reliable-dispatch: Introduce `StartPosition` and update callsites to use `create_reader(topic, start)`.
    - [x] danube-persistent-storage: Implement `WAL` (append, fsync batching, rotation, checkpoints, CRC32C) and `WALCache` (ring buffer, eviction).
      - In-memory WAL with live broadcast tailing.
      - File-backed durability with CRC and batched fsync.
      - Replay-from-offset implemented using WalCache + file replay.
      - Rotation by size/time (`rotate_max_bytes`, `rotate_max_seconds`).
      - Checkpoints written to `wal.ckpt` with `{ last_offset, file_seq, file_path }`.
    - [x] danube-reliable-dispatch: Wire `topic_storage.rs` to use WAL for append and tail; `dispatch.rs` consumes `TopicStream` and tracks offsets.
    - [ ] Optional: Make `topic_cache.rs` a thin shim over `WALCache` or deprecate it if redundant.
  - Exit Criteria:
    - [x] Producers publish -> offsets returned from WAL.
    - [x] Consumers read from `TopicStore::create_reader(topic, start)` backed by WAL with stable latency.
      - Latest and replay-from-offset supported (in-memory + file-backed).
    - [x] Segment-based code still compiles but is not on the hot path.

- [x] Phase B: Background Uploader + ETCD manifests (S3/GCS/fs/memory via opendal)
  - Objectives:
    - Persist batched WAL entries to rolling cloud objects using `opendal`.
    - Maintain object manifest and upload session metadata in ETCD (single-writer assumption for now).
    - Adopt key-per-object manifest schema with zero-padded `start_offset` to avoid large ETCD values.
  - Scope & Tasks:
    - [x] danube-persistent-storage: `CloudStore` implemented with opendal Operator (S3, GCS, Fs, Memory) via `BackendConfig { CloudBackend, LocalBackend }` (opendal 0.54).
    - [x] danube-persistent-storage: `Uploader` wired to `CloudStore` (timer/size triggers). Basic object format and ETCD manifest writes working.
      - Passing tests: `uploader_writes_object_and_manifest_memory`, `uploader_writes_object_and_manifest_fs`.
      - TODO: multipart uploads, retries, throttling, checksums.
    - [x] danube-persistent-storage: `EtcdMetadata` strict ETCD-only (no in-memory fallbacks); tests read in-memory store directly.
      - Note: Reuse existing `danube-metadata-store` crate (`MetadataStorage`). Single-writer assumption (serving broker only); no lease/CAS for now.
    - [x] danube-persistent-storage: Uploader checkpoint/resume (`uploader.ckpt`) implemented.
      - On start, resume from `last_committed_offset`; after successful upload, persist checkpoint.
      - Passing test: `uploader_resume_memory_from_checkpoint`.
    - [x] Write WAL `CheckpointEntry` with last committed offset and (future) upload session info; recovery scan/truncate to last valid entry. (Initial uploader checkpoint in place; extended session info deferred to Phase D if needed.)
  - Exit Criteria:
    - [x] Batches uploaded to cloud with rolling objects and ETCD per-object descriptors updated (validated for Memory/Fs backends).
    - [x] Crash/restart resumes uploader from checkpoint.

- [x] Phase C: CloudReader for historical catch-up behind TopicStore + ChainingStream handoff
  - Objectives:
    - Support consumers starting behind the WAL retention window to read historical data from cloud and then switch to WAL tail seamlessly.
    - Implement `ChainingStream` adapter for transparent Cloud→WAL handoff without gaps or duplicates.
  - Scope & Tasks:
    - [x] danube-persistent-storage: `CloudReader` (prefix/range scans of `/objects/` keys ordered by zero-padded `start_offset`; integrity checks using ETag/CRC when available).
    - [x] danube-persistent-storage: `ChainingStream` handoff implemented inside `WalStorage::create_reader()` using watermark H.
    - [x] danube-persistent-storage: Extend `WalStorage` to accept optional `CloudStore` + `EtcdMetadata`; implement `create_reader()` selection logic (Cloud→WAL when needed).
    - [x] danube-reliable-dispatch: Ensure `TopicStore::create_reader` forwards `StartPosition` and returns the PersistentStorage-provided stream (no extra buffering).
    - [ ] Optional: finalize deprecation of `topic_cache.rs` if fully superseded by `WALCache`.
    - [ ] Moved: Metrics (consumer lag, cloud read latency/bytes) tracked under Phase E.
  - Exit Criteria:
    - [x] A consumer starting before WAL retention can fully catch up using cloud objects and then tail from WAL without manual intervention; ordering and at-least-once guarantees preserved.

- [x] Phase D: Remove legacy storage (segments/StorageBackend) and wire wal_cloud end-to-end
   - Objectives:
     - Eliminate segment-based storage and the legacy `StorageBackend` API across the workspace.
     - Standardize on WAL + CloudReader (`WalStorage.with_cloud(...)`) for all reads/writes.
     - Update broker configuration to a single `wal_cloud` mode and wire config → runtime objects.
   - Scope & Tasks:
     - Core API (danube-core):
       - [x] Delete `StorageBackend` trait and `StorageBackendError` from `danube-core/src/storage.rs`.
       - [x] Delete segment model (`Segment`) and legacy storage config types (`DiskConfig`, `RemoteStorageConfig`, `CacheConfig`, `StorageConfig`).
       - [x] Keep only `PersistentStorage` primitives (StartPosition, PersistentStorageError, TopicStream, PersistentStorage).
     - Persistent storage (danube-persistent-storage):
       - [x] Remove `managed_storage.rs` (remote gRPC) and `local_disk.rs` (segment/disk path).
       - [x] Remove `connection.rs` (legacy RPC helper for remote storage).
       - [x] Update `src/lib.rs` to export only WAL + Cloud components (Wal, WalStorage, CloudStore, EtcdMetadata, Uploader, CloudReader).
       - [x] Ensure tests cover WAL, uploader, cloud reader, and handoff only; delete/refactor any legacy tests.
     - Reliable dispatch (danube-reliable-dispatch):
       - [x] Remove `src/storage_backend.rs` and any references to `StorageBackend`/segments.
       - [x] Ensure wiring uses only WAL path: `ReliableDispatch::new(topic, opts, WalStorage)` and `TopicStore::create_reader()` delegates to `PersistentStorage`.
       - [x] Refactor `SubscriptionDispatch` to stream from `TopicStream` (WAL + Cloud) instead of segment logic.
     - Broker config and wiring:
       - [x] Add `wal_cloud` with sections for `wal`, `uploader`, `cloud`, `metadata` in `config/danube_broker.yml`.
       - [x] Wire `WalStorage::from_wal(...).with_cloud(cloud_store, etcd, topic_path)` in broker startup.
       - [x] Update README/docs and provide a local memory/fs sample config.
   - Exit Criteria:
     - [x] Workspace compiles with no references to `StorageBackend`, segments, or legacy Disk/Remote storage.
     - [x] Broker runs in `wal_cloud` mode; end-to-end publish/consume works with local memory/fs backends.
     - [x] Test suites pass in persistent-storage and reliable-dispatch.

- [ ] Phase E: Metrics and Observability
  - Objectives:
    - Validate correctness, performance, and resilience across backends and failure modes.
  - Scope & Tasks:
    - [ ] Add metrics scaffolding for cloud read latency/bytes and consumer lag (now that Phase D is complete).
    - [~] Unit/integration tests: WAL append/read (file replay), rotation/checkpoint (added), CRC; uploader session lifecycle; CloudReader range reads; ChainingStream watermark selection and duplicate defense.
    - [~] Integration: opendal `memory` and `fs` backends (added tests). Feature-flag E2E for S3 and GCS pending.
    - [ ] Failure injection: cloud outages, WAL corruption, unclean shutdown, leader change/rebalance, inactive subscription scenarios (retention-floor behavior).
    - [ ] Benchmarks: publish latency/throughput, upload bandwidth, consumer catch-up time; enforce retention/outage budgets.
  - Exit Criteria:
    - [ ] CI green across unit/integration suites; baseline performance targets met; failure scenarios recover automatically.


## Next PR: Concrete Tasks
- Multipart/retries/throttling/checksums in `Uploader`; align rotation window with WAL files; validate etag/checksum recording.
- Expose S3/GCS cloud settings in broker wal_cloud config (region, endpoint, credentials) mapped into `CloudStore::Cloud` options.
- Adopt Writer API.
- Clean up examples and tests to the WAL-only world:
  - [x] Reliable-dispatch: renamed `process_current_segment()` to `poll_next()` and removed old method.
  - [x] Removed lifecycle task scaffolding and `retention_period` from `TopicStore`.
  - [x] Removed legacy segment-based tests; added WAL replay/tail tests.
  - [x] Update `danube-reliable-dispatch/examples/wal_wiring.rs` to drop `TopicCache`/`StorageConfig` and use only WAL wiring.

### Completed in this iteration
- Broker config now uses a tagged enum `CloudConfig` for `wal_cloud.cloud` with variants: `memory`, `fs`, `s3`, `gcs`.
- Broker startup (`danube-broker/src/main.rs`) maps `CloudConfig` to `BackendConfig::{Local, Cloud}` with options for S3/GCS.
- `config/danube_broker.yml` updated: default `backend: memory` and appended commented examples for `fs`, `s3`, `gcs`.

## Current Status
- [x] Phase D complete and build is clean across broker, reliable-dispatch, and persistent-storage.
- [x] Broker uses `wal_cloud` only; legacy storage removed.
- [x] Reliable-dispatch is WAL-only; streaming via `TopicStream` with `poll_next()` API.
- [x] Persistent-storage exports only WAL/Cloud types; legacy files removed.
- [x] S3/GCS options exposed in broker config and mapped to `CloudStore::Cloud`.

## Decision Log
- [x] Phase A uses WAL with in-memory cache, CRC32C frames, batched fsync, file replay, rotation, and checkpoints.
- [x] Subscription start policy: `SubscriptionDispatch` selects `StartPosition::{Latest, Offset(S)}`; `TopicStore` materializes the requested stream.
- [x] Cloud→WAL handoff via `ChainingStream` (planned for Phase C).
- [x] Added example `danube-reliable-dispatch/examples/wal_wiring.rs` demonstrating durable WAL configuration and wiring.
