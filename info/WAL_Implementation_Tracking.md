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

- [~] Phase B: Background Uploader + ETCD manifests (S3/GCS/fs/memory via opendal)
  - Objectives:
    - Persist batched WAL entries to rolling cloud objects using `opendal`.
    - Maintain object manifest and upload session metadata in ETCD (single-writer assumption for now).
    - Adopt key-per-object manifest schema with zero-padded `start_offset` to avoid large ETCD values.
  - Scope & Tasks:
    - [~] danube-persistent-storage: `CloudStore` scaffold (to be backed by opendal for s3/gcs/fs/memory).
    - [~] danube-persistent-storage: `Uploader` scaffold (timer/size triggers; to add multipart, retries, throttling, checksums).
    - [~] danube-persistent-storage: `EtcdMetadata` scaffold using key-per-object paths: `/storage/topics/{ns}/{topic}/objects/{start_offset_padded}`; optional `/objects/cur` pointer.
      - Note: Reuse existing `danube-metadata-store` crate (`MetadataStorage`). Single-writer assumption (serving broker only); no lease/CAS for now.
    - [ ] Write WAL `CheckpointEntry` with last committed offset and (future) upload session info; recovery scan/truncate to last valid entry.
    - [ ] Config: Add `storage.mode: wal_cloud`, WAL/uploader/cloud settings to `config/danube_broker.yml`.
    - [ ] Metrics: upload.batch_bytes, upload.latency_ms, manifest.txn_latency_ms, session.resume/abort.
  - Exit Criteria:
    - [ ] Batches uploaded to cloud with rolling objects and ETCD per-object descriptors updated.
    - [ ] Crash/restart resumes or rotates MPU safely using WAL checkpoint.

- [ ] Phase C: CloudReader for historical catch-up behind TopicStore + ChainingStream handoff
  - Objectives:
    - Support consumers starting behind the WAL retention window to read historical data from cloud and then switch to WAL tail seamlessly.
    - Implement `ChainingStream` adapter for transparent Cloud→WAL handoff without gaps or duplicates.
  - Scope & Tasks:
    - [ ] danube-persistent-storage: `CloudReader` (prefix/range scans of `/objects/` keys ordered by zero-padded `start_offset`; integrity checks using ETag/CRC when available).
    - [ ] danube-reliable-dispatch: `TopicStore::create_reader` to return `ChainingStream` that computes watermark H and switches to WAL at `H`.
    - [ ] Optional: finalize deprecation of `topic_cache.rs` if fully superseded by `WALCache`.
    - [ ] Metrics: consumer lag, cloud read latency/bytes.
  - Exit Criteria:
    - [ ] A consumer starting before WAL retention can fully catch up using cloud objects and then tail from WAL without manual intervention; ordering and at-least-once guarantees preserved.

- [ ] Phase D: Integration polish, configuration, and legacy removal
  - Objectives:
    - Make the new storage the default path; remove deprecated remote GRPC storage and segment-based code paths.
    - Finalize configuration with WAL retention-floor knobs.
  - Scope & Tasks:
    - [ ] Remove `managed_storage.rs` (remote GRPC) and related configuration.
    - [ ] Deprecate/remove segment APIs and usages in `dispatch.rs` and related modules.
    - [ ] Configuration: ensure presence of WAL retention floor knobs:
      - `wal.retention.min_minutes`, `wal.retention.min_bytes`, `wal.retention.active_subscription_grace_seconds`.
    - [ ] Observability: dashboards/alerts for WAL growth, uploader lag, split-brain guardrails.
    - [ ] Documentation: update samples and runbooks to reflect StartPosition API, key-per-object manifest, and ChainingStream handoff.
  - Exit Criteria:
    - [ ] Broker runs with `wal_cloud` as default; legacy storage removed from production builds.
    - [ ] Documentation updated; samples and README reflect the new architecture.

- [ ] Phase E: Testing, benchmarks, and hardening
  - Objectives:
    - Validate correctness, performance, and resilience across backends and failure modes.
  - Scope & Tasks:
    - [~] Unit tests: WAL append/read (file replay), rotation/checkpoint (added), CRC; uploader session lifecycle; CloudReader range reads; ChainingStream watermark selection and duplicate defense.
    - [ ] Integration: opendal `memory` and `fs` backends; feature-flag E2E for S3 and GCS.
    - [ ] Failure injection: cloud outages, WAL corruption, unclean shutdown, leader change/rebalance, inactive subscription scenarios (retention-floor behavior).
    - [ ] Benchmarks: publish latency/throughput, upload bandwidth, consumer catch-up time; enforce retention/outage budgets.
  - Exit Criteria:
    - [ ] CI green across unit/integration suites; baseline performance targets met; failure scenarios recover automatically.

## Next PR: Concrete Tasks
- Implement opendal-backed `CloudStore` (s3/gcs/fs/memory) and wire `Uploader` to write rolling objects.
- Extend `ObjectDescriptor` (etag, checksums, completed) and align rotation window with WAL files.
- Add WAL `CheckpointEntry` schema for uploader session progress; integrate simple recovery on restart.
- Add broker config surface (`storage.mode: wal_cloud`, wal/uploader/cloud sections) and initial metrics.
- Optional: Add `CloudReader` skeleton and begin ChainingStream integration.

## Decision Log
- [x] Phase A uses WAL with in-memory cache, CRC32C frames, batched fsync, file replay, rotation, and checkpoints.
- [x] Subscription start policy: `SubscriptionDispatch` selects `StartPosition::{Latest, Offset(S)}`; `TopicStore` materializes the requested stream.
- [~] Cloud→WAL handoff via `ChainingStream` (planned for Phase C).
- [x] Added example `danube-reliable-dispatch/examples/wal_wiring.rs` demonstrating durable WAL configuration and wiring.
