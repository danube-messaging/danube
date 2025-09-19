# Danube WAL + Cloud Persistence Implementation Tracking Plan

This file tracks the end-to-end implementation progress for the WAL-first storage with cloud persistence via opendal. Update this file as work progresses. The design reference is `info/WAL_Cloud_Persistence_Design.md`.

## Legend
- [ ] Pending
- [~] In progress
- [x] Done

## Milestones (Phases)
- [ ] Phase A: Introduce WAL alongside existing storage (hot path uses WAL + TopicStore::create_reader stream)
  - Outcome: Sub-second dispatch via local WAL and WALCache; legacy segment code remains compiled but unused by default.
- [ ] Phase B: Cloud uploader via `opendal` (S3/GCS/fs/memory) with ETCD metadata
  - Outcome: Batches from WAL are persisted to cloud; recovery/leader resume verified.
- [ ] Phase C: Historical reads via CloudReader behind TopicStore
  - Outcome: Catch-up consumers read beyond WAL retention seamlessly; TopicCache acts as thin shim over WALCache or is removed.
- [ ] Phase D: Remove remote GRPC storage and finalize configuration/docs
  - Outcome: `managed_storage.rs` removed; segment-based code paths deprecated; new storage is default.

## Immediate Next Steps (Actionable)
- [ ] Refactor existing crate: `danube-persistent-storage` to host the new design and opendal integration:
   - [ ] `Wal`: append, fsync batching, rotation, checkpoints, CRC; async tail reader; integrated `WALCache`
   - [ ] `Uploader`: 10s/size-triggered batching; multipart upload; ETCD txn updates; retries; session resume/abort
   - [ ] `CloudStore`: `opendal` Operator factory for `s3`/`gcs`/`fs`/`memory`
   - [ ] `CloudReader`: ranged reads guided by ETCD manifest
   - [ ] `EtcdMetadata`: schemas, CAS ops, leases/locks for leader-only uploads and ownership
- [ ] Update `danube-core/src/storage.rs` with WAL-capable trait (`PersistentStorage`) and adapter for current `StorageBackend`
- [ ] Wire `danube-reliable-dispatch/src/topic_storage.rs` to use WAL for append/tail read and expose `create_reader` as async Stream
- [ ] Deprecate/remove segment-based code paths in `danube-reliable-dispatch/src/dispatch.rs`; replace with offset-based `TopicStream`
- [ ] Migrate `danube-reliable-dispatch/src/topic_cache.rs` to a thin adapter to `WALCache` (or remove if feasible)
- [ ] Add configuration surface in `config/danube_broker.yml` with `opendal` backends (s3/gcs/fs/memory)
- [ ] Add metrics (WAL latency, upload stats, ETCD latency, consumer lag)
- [ ] Write docs/readme for new storage and migration notes

## Work Breakdown by Crate/Module
- danube-core
  - [ ] Define `PersistentStorage` trait (append/create_reader/ack_checkpoint/flush) and use it as the sole storage interface
  - [ ] Deprecate `StorageBackend` and provide a temporary compatibility adapter for legacy callers
  - [ ] Remove `StorageBackend` usages once migration is complete
- danube-reliable-dispatch
  - [ ] Switch hot path to WAL append
  - [ ] Replace segment-based consumption with offset-based `TopicStream` in `dispatch.rs`
  - [ ] `topic_storage.rs` exposes async Stream via `create_reader`; optional historical fetch via `CloudReader`
  - [ ] `topic_cache.rs` shims to `WALCache` or is removed
- danube-persistent-storage
  - [ ] Refactor to implement (`WAL` + `WALCache` + `Uploader` + `CloudStore` + `CloudReader` + `EtcdMetadata`) within this crate.
  - [ ] Remove `managed_storage.rs`.
- Config and docs
  - [ ] Add new storage config examples and migration notes.

## ETCD Keys and Transactions
- [ ] Implement key layout under `/danube/storage/...` and `/danube/subscriptions/...`
- [ ] Implement CAS operations for manifest advancement and subscription progress
- [ ] Implement lease/lock mechanism for single-writer uploads per topic

## Testing & Benchmarks
- [ ] Unit tests: WAL (append/read/rotate/checkpoint/CRC)
- [ ] Integration tests: `opendal` memory and fs backends
- [ ] E2E tests: S3 and GCS behind feature flags
- [ ] Failure injection: cloud outages, WAL corruption, unclean shutdown, leader change
- [ ] Benchmarks: publish latency, throughput, upload BW, consumer catch-up time

## Risks and Mitigations
- [ ] MPU/session expiration: rotate to new object; track in ETCD; resume on restart
- [ ] WAL growth under outage: enforce retention floors sized to outage budget; pressure metrics/alerts
- [ ] Split-brain uploads: ETCD lease/lock and broker epoch checks
- [ ] Strict single-file-per-topic requirement: consider compose/multipart-copy strategy if needed

## Decision Log
- [ ] Phase 1 uses rolling objects per topic with offset ranges to simplify MPU handling and recovery
- [ ] WAL retention targets: configurable minutes/bytes to absorb outages
