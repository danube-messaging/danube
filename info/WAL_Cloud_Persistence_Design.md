# Danube WAL + Cloud Persistence Design (S3/GCS via opendal)

## Status
- Design proposal for migrating from segment-based storage to a Write-Ahead Log (WAL) with background cloud persistence.
- Targets sub-second dispatch by keeping hot path local and offloading object storage to async background tasks.

## Goals
- Replace segment-based storage with a WAL-first architecture.
- Persist data to cloud object storage using `opendal` (S3 or GCS). Local disk only for single-broker, Memory for tests.
- Maintain consumer progress and object metadata in ETCD (already in use for cluster coordination).
- Keep dispatch path sub-second by serving from in-memory/WAL cache.
- Batch uploads every ~10 seconds (tunable) from WAL to per-topic objects in cloud.
- Preserve backward compatibility during migration where feasible; deprecate remote GRPC storage (`managed_storage.rs`).

## Non-Goals (Phase 1)
- Full Apache Iceberg integration (may come later). This design is inspired by AutoMQ and Iceberg I/O patterns but focuses on WAL + object storage with minimal schema.
- Exactly-once delivery across cluster restarts (Phase 1 aims at at-least-once with idempotency hooks).

---

## Current Architecture (as-is)
- `danube-core/src/storage.rs`: defines `StorageBackend` used by dispatch.
- Implementations:
  - Disk: `danube-persistent-storage/src/local_disk.rs`.
  - Memory: `danube-reliable-dispatch/src/storage_backend.rs` (for tests).
  - Remote GRPC: `danube-persistent-storage/src/managed_storage.rs`.
- Dispatch: `danube-reliable-dispatch/src/dispatch.rs`.
- Topic storage/cache: `danube-reliable-dispatch/src/topic_storage.rs`, `danube-reliable-dispatch/src/topic_cache.rs`.
- Bottleneck: remote storage unsuitable for sub-second stream dispatch.

---

## Target Architecture Overview
Key components:
- WAL (Write-Ahead Log):
  - Durable append-only log per topic on local disk.
  - Provides sub-ms writes and fast reads for hot data.
  - Periodic checkpoints for recovery and upload progress.

- Background Uploader (Flusher):
  - Reads new WAL entries, batches for ~10s or size threshold.
  - Writes batches to a per-topic cloud object via `opendal`.
  - Updates ETCD metadata atomically (upload session state, last committed offset, object ETag/version, etc.).

- Object Storage Abstraction:
  - `opendal` Operator configured for S3 or GCS (or local/memory for dev/test).
  - Inspired by `iceberg-rust` Storage/FileIO patterns.

- ETCD Metadata:
  - Tracks subscription progress (per topic/subscription offset/position).
  - Tracks object metadata (current upload session, committed range, checkpoint offsets, ETag, object version, MPU id).
  - Used for leader changes, crash recovery, and consumers locating data beyond WAL retention.

- Reader Path:
  - Consumers read from in-memory cache/WAL tail for live data.
  - For older data (beyond WAL window), broker can serve from cloud object by range reads or prefetch into cache.

- Backends Selection:
  - `S3` or `GCS` via `opendal` by configuration.
  - `LocalDisk` only if single broker is configured.
  - `Memory` for internal testing.

---

## Data and File Model
- Per-topic object key namespace: `topics/{tenant}/{namespace}/{topic}/data`.
- Phase 1 options:
  1) Single logical object per topic with ongoing multipart upload (MPU) and periodic `CompleteMultipartUpload` for committed parts; on rotation create a new object with a monotonically increasing suffix.
  2) Rolling objects per topic (e.g., N-minute or size-based shards): `.../data-<epoch>-<start_offset>-<end_offset>.parquet` or `.bin`.

Trade-offs:
- True single-object append is not natively supported by S3/GCS; we rely on MPU append behavior. To simplify recovery and avoid MPU longevity issues, Phase 1 will implement rolling objects with clear offset ranges. This also simplifies consumer range reads. The requirement "one file per topic" is approximated by exposing a logical stream per topic, materialized as a small number of rolling objects. Rotation can be on time (10s) or size (e.g., 64–128MB), whichever comes first.

- Object format:
  - Phase 1: compact binary framing with a footer index. Each file contains a sequence of frames: [FrameHeader | Message(s) | CRC].
  - Optional: Snappy/LZ4 compression per frame.
  - Include minimal schema header and magic for self-description.
  - Future: Parquet/Arrow for analytics.

---

## WAL Design
- Per-topic WAL on local disk, directory: `wal/{tenant}/{namespace}/{topic}/`. Files: `wal.log`, with rotation: `wal.log.<seq>`.
- WAL Entry types:
  - MessageEntry { offset, timestamp, key, headers, payload, crc }
  - CheckpointEntry { last_committed_offset, upload_session_id, crc }
- Guarantees:
  - fsync on batch append (configurable) for durability; use O_DIRECT/O_DSYNC where supported.
  - CRC32 on entries; per-file checksum in footer.
- Reader:
  - Supports tailing and replay from offset.
  - Exposes async stream to dispatch.
- Retention:
  - Keep at least X minutes/GB in WAL to absorb cloud delays and enable fast catch-up. Older segments pruned once all subscriptions progress > segment end and cloud upload committed.

References:
- AutoMQ WAL: WriteAheadLog, UploadWriteAheadLogTask.

---

## Background Uploader (Flusher)
- Triggered by timer (default 10s) or size threshold (e.g., 8MB).
- Reads WAL entries since last uploaded offset.
- Batches messages into a rolling object writer for the topic.
- Uses `opendal` Writer with multipart semantics for S3/GCS.
- On successful upload of a batch:
  - Update ETCD with new object manifest or extend end_offset of current object.
  - Write WAL CheckpointEntry with the last committed offset and upload session id.
- On failure:
  - Retry with exponential backoff.
  - If MPU session is corrupted or expired, start a new object and update ETCD accordingly.

References:
- AutoMQ ObjectWriter/S3Storage/S3Stream design for MPU handling.

---

## ETCD Metadata Schema
Key prefixes (all keys are examples; actual paths configurable):
- Topic ownership & leader (existing): `/danube/brokers/...`
- Topic object manifests:
  - `/danube/storage/topics/{tenant}/{ns}/{topic}/objects/cur` -> current rolling object id
  - `/danube/storage/topics/{tenant}/{ns}/{topic}/objects/list` -> list of object descriptors
  - Object descriptor value:
    ```json
    {
      "object_id": "data-1695000000-1000-2000.bin",
      "start_offset": 1000,
      "end_offset": 2000,
      "size": 8388608,
      "etag": "\"abc123\"",
      "created_at": 1695000000,
      "completed": true,
      "upload_id": "<mpu-id-if-active>",
      "backend": "s3|gcs|local|memory"
    }
    ```
- Upload session state:
  - `/danube/storage/topics/{tenant}/{ns}/{topic}/upload/session` -> { object_id, upload_id, last_part, last_committed_offset }
- Subscription progress:
  - `/danube/subscriptions/{tenant}/{ns}/{topic}/{subscription}/progress` -> { offset, timestamp }
- WAL checkpoints metadata (optional if contained in WAL file):
  - `/danube/storage/topics/{tenant}/{ns}/{topic}/wal/last_checkpoint` -> { offset, file_seq, file_pos }

Atomicity:
- Use ETCD transactions (compare-and-swap) to atomically advance object manifest and subscription progress when needed.
- Include broker epoch/lease to prevent split-brain writers.

---

## Dispatch Path Changes
- `danube-reliable-dispatch/src/topic_storage.rs` and `topic_cache.rs` updated to:
  - Append to WAL for new messages.
  - Read from WAL tail for live consumers.
  - For replays beyond WAL retention, issue ranged reads from cloud via `opendal` and feed into cache (optional Phase 1.1).
- `dispatch.rs` remains largely unchanged, but underlying storage becomes WAL-backed. Delivery latency improves since no remote GRPC write is in hot path.

---

## API/Crate Changes
- `danube-core/src/storage.rs`:
  - Introduce new trait `PersistentStorage` or evolve `StorageBackend` to a WAL-capable interface:
    - `appendMessage(topic, msg) -> offset`
    - `createReader(topic, from_offset) -> Stream<Message>`
    - `ackCheckpoint(topic, up_to_offset)` (internal for uploader)
    - `flush(topic)` (optional)
  - Provide a compatibility adapter for current dispatch usage.

- New crate or module: `danube-opendal-storage` (or reuse `danube-persistent-storage` with new design):
  - `Wal` (local disk)
  - `Uploader` (background task)
  - `CloudStore` (opendal operator factory; S3/GCS/local/memory)
  - `EtcdMetadata` (etcd client, schemas)

- Deprecate and remove remote GRPC storage:
  - `danube-persistent-storage/src/managed_storage.rs` to be removed after migration.

- Keep `local_disk.rs` for single-broker mode and as WAL storage device.
- Keep `memory` storage for tests.

References:
- Iceberg FileIO/Storage patterns for S3/GCS abstraction.
- AutoMQ s3stream layering: Writer/Reader/Storage.

---

## Configuration
Example `config/danube_broker.yml` additions:
```yaml
storage:
  mode: wal_cloud            # wal_cloud | local_only | memory
  wal:
    dir: /var/lib/danube/wal
    fsync_interval_ms: 5
    retention:
      min_minutes: 60
      min_bytes: 107374182400   # 100 GiB
  uploader:
    interval_seconds: 10
    max_batch_bytes: 8388608    # 8 MiB
    rotation:
      max_object_bytes: 134217728  # 128 MiB
      max_object_seconds: 300
  cloud:
    backend: s3                 # s3 | gcs | local | memory
    root: s3://my-bucket/danube
    s3:
      region: us-east-1
      endpoint: https://s3.amazonaws.com
      access_key: ${AWS_ACCESS_KEY_ID}
      secret_key: ${AWS_SECRET_ACCESS_KEY}
    gcs:
      bucket: my-bucket
      credential_file: ${GOOGLE_APPLICATION_CREDENTIALS}
  etcd:
    endpoints:
      - http://127.0.0.1:2379
    namespace: /danube
```

`opendal` Operator construction will map from `cloud` config to appropriate scheme and options.

---

## Failure Handling and Recovery
- Broker crash during upload:
  - On restart, consult ETCD upload session; resume or finalize MPU if possible; else start new object and mark previous as aborted.
- WAL corruption:
  - Detect via CRC; truncate to last valid checkpoint.
- Cloud transient failures:
  - Exponential backoff with jitter; keep WAL retention to absorb outages.
- Leader change:
  - Only leader broker for a topic writes uploads. Use ETCD lease/lock to guard. New leader reads latest ETCD state and WAL checkpoints to resume.
- Consumer rebalancing:
  - Subscription offsets in ETCD keep progress consistent across consumers.

---

## Security & IAM
- Use env var or instance role-based credentials for S3/GCS.
- Avoid storing secrets in ETCD; only store object ids, etags, and offsets. Upload IDs may be stored; consider TTL or encryption if needed.

---

## Metrics & Observability
- Expose metrics: WAL append latency, fsync latency, bytes in WAL, upload batch size, upload latency, MPU parts, ETCD tx latency, consumer lag.
- Structured logging with topic, offsets, object ids.

---

## Migration Plan
Phase A: Introduce WAL in parallel with existing segment storage
- Implement WAL writer/reader and use it in dispatch hot path.
- Keep existing storage for fallback (not in hot path).

Phase B: Add Cloud Uploader via `opendal`
- Implement S3 and GCS backends; support local/memory for dev/test.
- Start persisting objects and ETCD metadata. Validate recovery flows.

Phase C: Switch catch-up reads to WAL + cloud objects
- Implement ranged reads from objects for historical data.
- Add prefetch into `topic_cache` when needed.

Phase D: Deprecate and remove remote storage
- Remove `managed_storage.rs` and related config.
- Update docs and samples.

---

## Testing Strategy
- Unit tests for WAL (append, read, rotate, checkpoint, CRC).
- Integration tests with `opendal` memory and local backends.
- E2E tests for S3 and GCS (behind feature flags with credentials).
- Failure injection: cloud outages, WAL corruption, unclean shutdown.
- Benchmark: publish latency, throughput, upload bandwidth, consumer catch-up time.

---

## Open Questions / Future Work
- Exactly-once semantics with idempotent producer/consumer.
- Move to columnar format (Parquet/Arrow) for analytics convergence.
- Compaction of many small objects into larger segments during off-peak.
- Multi-tenant isolation and encryption at rest via KMS.

---

## References
- Iceberg I/O design:
  - `iceberg/src/io/storage.rs`
  - `iceberg/src/io/storage_s3.rs`
  - `iceberg/src/io/storage_gcs.rs`
  - `iceberg/src/io/file_io.rs`
- AutoMQ S3Stream and WAL:
  - `s3stream` module
  - `ObjectReader`, `ObjectWriter`, `S3Storage`, `S3Stream`
  - WAL: `WalFactory`, `WriteAheadLog`, `MemoryWriteAheadLog`, `UploadWriteAheadLogTask`

## Summary of Code Changes (by crate)
- `danube-core`:
  - Evolve `StorageBackend` or add `PersistentStorage` trait with WAL interfaces.
- `danube-reliable-dispatch`:
  - Update `topic_storage.rs` and `topic_cache.rs` to use WAL for append/tail read.
  - Add reader path for historical data via `opendal`.
- `danube-persistent-storage`:
  - Replace with `danube-opendal-storage` (WAL + Uploader + CloudStore + EtcdMetadata) or refactor within this crate.
  - Remove `managed_storage.rs`.
- Config and docs:
  - Add new storage config examples and migration notes.

---

## Implementation Tracking Plan
This section tracks the end-to-end migration with phases, concrete tasks, and status checkboxes. Update as work progresses.

### Legend
- [ ] Pending
- [~] In progress
- [x] Done

### Milestones (Phases)
- [ ] Phase A: Introduce WAL alongside segment storage (hot path uses WAL)
  - Outcome: Sub-second dispatch via local WAL; segment storage remains for fallback only.
- [ ] Phase B: Cloud uploader via `opendal` (S3/GCS/local/memory) with ETCD metadata
  - Outcome: Batches from WAL are persisted to cloud; recovery verified.
- [ ] Phase C: Historical reads from cloud objects (range reads/prefetch)
  - Outcome: Catch-up consumers can read beyond WAL retention seamlessly.
- [ ] Phase D: Remove remote GRPC storage and finalize configuration/docs
  - Outcome: `managed_storage.rs` removed; new storage is default.

### Immediate Next Steps (Actionable)
- [ ] Create new storage module/crate: `danube-opendal-storage` (or refactor `danube-persistent-storage`)
  - [ ] `Wal`: append, fsync batching, rotation, checkpoints, CRC; async reader/tailer
  - [ ] `Uploader`: 10s/size-triggered batching; multipart upload; ETCD txn updates; retries
  - [ ] `CloudStore`: `opendal` Operator factory for S3/GCS/local/memory backends
  - [ ] `EtcdMetadata`: schemas, CAS ops, leases/locks for leader-only upload
- [ ] Update `danube-core/src/storage.rs` with WAL-capable trait (`PersistentStorage`) and adapter for current `StorageBackend`
- [ ] Wire `danube-reliable-dispatch/src/topic_storage.rs` and `topic_cache.rs` to use WAL for append/tail read
- [ ] Add configuration surface in `config/danube_broker.yml` with S3/GCS/local/memory examples
- [ ] Feature flags for cloud backends and integration tests (credentials gated)
- [ ] Add metrics (WAL latency, upload stats, ETCD latency, consumer lag)
- [ ] Write docs/readme for new storage and migration notes

### Work Breakdown by Crate/Module
- danube-core
  - [ ] Define `PersistentStorage` trait (append/create_reader/ack_checkpoint/flush)
  - [ ] Add compatibility adapter for current callers
- danube-reliable-dispatch
  - [ ] Switch hot path to WAL append
  - [ ] Reader uses WAL tail; optional historical fetch via cloud
  - [ ] Minimal changes in `dispatch.rs`; main changes in `topic_storage.rs` and `topic_cache.rs`
- danube-opendal-storage (new) or danube-persistent-storage (refactor)
  - [ ] Implement `Wal`
  - [ ] Implement `Uploader`
  - [ ] Implement `CloudStore` (opendal Operator)
  - [ ] Implement `EtcdMetadata`
  - [ ] Provide constructor mapping from broker config
- danube-persistent-storage
  - [ ] Deprecate/remove `managed_storage.rs` once Phase C is complete

### ETCD Keys and Transactions
- [ ] Implement key layout under `/danube/storage/...` and `/danube/subscriptions/...`
- [ ] Implement CAS operations for manifest advancement and subscription progress
- [ ] Implement lease/lock mechanism for single-writer uploads per topic

### Testing & Benchmarks
- [ ] Unit tests: WAL (append/read/rotate/checkpoint/CRC)
- [ ] Integration tests: `opendal` memory and local backends
- [ ] E2E tests: S3 and GCS behind feature flags
- [ ] Failure injection: cloud outages, WAL corruption, unclean shutdown, leader change
- [ ] Benchmarks: publish latency, throughput, upload BW, consumer catch-up time

### Risks and Mitigations
- [ ] MPU/session expiration: rotate to new object; track in ETCD; resume on restart
- [ ] WAL growth under outage: enforce retention floors sized to outage budget; pressure metrics/alerts
- [ ] Split-brain uploads: ETCD lease/lock and broker epoch checks
- [ ] Strict single-file-per-topic requirement: consider compose/multipart-copy strategy if needed

### Decision Log
- [ ] Phase 1 uses rolling objects per topic with offset ranges to simplify MPU handling and recovery
- [ ] WAL retention targets: configurable minutes/bytes to absorb outages

