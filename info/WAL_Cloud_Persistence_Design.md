# Danube WAL + Cloud Persistence Design (S3/GCS via opendal)

## Status
- Design proposal for migrating from segment-based storage to a Write-Ahead Log (WAL) with background cloud persistence.
- Targets sub-second dispatch by keeping hot path local and offloading object storage to async background tasks.

## Goals
- Replace segment-based storage with a WAL-first architecture.
- Persist data to cloud object storage using `opendal` (S3, GCS and local disk for single-broker). Memory for tests.
- Maintain subscription progress and object metadata in ETCD (ETCD is already in use for cluster coordination).
- Keep dispatch path sub-second by serving from in-memory/WAL cache.
- Batch uploads every ~10 seconds (tunable) from WAL to per-topic objects in cloud.
- Deprecate remote GRPC storage (`managed_storage.rs`).
- At Least Once delivery across broker or subscription restarts.

---
## Danube Topic assignment and subscriptions dispatch

- Topic assignment and subscriptions:
  - Topics are assigned to brokers by the control plane/load manager and are owned exclusively by a single broker at a time (topic-to-broker is 1:1).
  - A topic may have multiple subscriptions (consumer groups). Subscriptions are managed independently and can progress at different offsets.
  - Topics support two modes: reliable and non-reliable.
    - Reliable topics instantiate `ReliableDispatch` in `danube-reliable-dispatch`.
    - `ReliableDispatch::new_subscription_dispatch(...)` creates a `SubscriptionDispatch` per subscription, handling delivery, backpressure, and retries.
    - Non-reliable topics follow a simplified dispatch path without durable progress guarantees.

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
- SubscriptionDispatch (per-subscription):
  - Coordinates delivery, flow control, retries; consumes an async `TopicStream`.
  - Chooses the start position for each subscription:
    - New subscription: start at the latest WAL offset (tail) to receive only recent messages.
    - Existing subscription: resume from the last committed offset recorded in ETCD (At-Least-Once semantics).
  - No direct storage or ETCD writes; emits delivery events to `ProgressUpdater`.
- TopicStore (per-topic facade):
  - Provides append and `create_reader(start: StartPosition) -> TopicStream`.
  - Hides source selection (WAL tail vs CloudReader) and uses hot cache transparently.
- WAL with WALCache (per-topic):
  - Durable append-only log on local disk for sub-ms writes.
  - Integrated in-memory ring buffer (WALCache) for hot reads; batched fsync and checkpoints.
- Background Uploader (Flusher):
  - Batches WAL entries (~10s or size threshold) and uploads via `opendal` to S3/GCS.
  - Writes ETCD object manifest/session state (Option A ownership).
- CloudReader (per-topic):
  - Performs ranged reads of historical objects via `opendal`, guided by ETCD manifest.
  - Feeds data behind `TopicStore` when a reader is behind WAL retention.
- ProgressUpdater (per-topic/group):
  - Rate-limited writer of subscription progress to ETCD (Option A ownership).
  - Time/delta-based flush with CAS; leader-lease guarded.
- Object Storage Abstraction:
  - `opendal` Operator configured for S3 or GCS (or local/memory for dev/test).
- ETCD Metadata:
  - Object manifests/upload session and per-subscription progress; leader changes and recovery.
- Reader Path:
  - `TopicStore::create_reader(topic, start)` returns an async Stream.
  - Yields from WAL tail (via WALCache) when within retention; otherwise uses CloudReader to catch up, then switches to WAL.
- Backends Selection:
  - All cloud/local backends via `opendal` Operator: `s3`, `gcs`, and `fs` (local filesystem).
  - `LocalDisk` is provided through `opendal` `fs`; recommended only for single-broker setups.
  - `Memory` primarily for internal testing: use `opendal` `memory` backend or keep Danube’s in-process memory storage.

---

## API and Interface Decisions

### TopicStore::create_reader
- Returns an async stream of messages with backpressure, abstracting WAL tail vs CloudReader.
- Suggested Rust signature:
  - `fn create_reader(&self, topic: &TopicRef, start: StartPosition) -> Result<TopicStream>` where `StartPosition` is:
    ```rust
    enum StartPosition { Latest, Offset(u64) }
    ```
  - Alternatively, return a concrete `TopicStream` type that implements `Stream<Item = Result<StreamMessage>>`.
- Semantics:
  - Start at `start` (latest or a concrete offset).
  - Yield from WAL tail when within retention; if behind retention, fetch historical ranges via CloudReader using ETCD manifest, then switch to WAL when caught up.
  - Backpressure via bounded internal queues; stream only yields when polled.
- ChainingStream adapter:
  - `TopicStore::create_reader` returns a stateful adapter ("ChainingStream") that first polls from `CloudReader` and, once exhausted up to a computed watermark, seamlessly switches to polling the WAL tailing stream. The handoff is transparent to callers and guarantees in-order, no-duplication delivery across the boundary.

### WAL and WALCache
- Integrate a hot in-memory ring buffer (WALCache) inside WAL implementation to minimize disk reads and simplify caching.
- Write-through on append: push to ring and write WAL file (fsync batched by config).
- Tail reader prefers WALCache; falls back to WAL file when not in cache window.
- Eviction policy bounded by size/time; safe to evict ranges that are uploaded to cloud and advanced by all subscriptions.

### Deprecate Segment-based Storage
- Remove `Segment` usage from `SubscriptionDispatch` (`danube-reliable-dispatch/src/dispatch.rs`).
- Replace with offset tracking and `TopicStream` consumption.
- `TopicStore` drops segment APIs; only exposes `append` and `create_reader`.

### CloudReader
- Performs ranged reads of historical objects via `opendal`, guided by ETCD manifest.
- Streams messages back to `TopicStore` which feeds the unified `TopicStream`.

### ETCD Write Ownership
- `BackgroundUploader` owns object manifest and session updates.
- `ProgressUpdater` owns subscription progress updates with time/delta-based flushing.
- Both guarded by ETCD lease/lock for leader-only writes.

### Suggested Minimal Interfaces
- TopicStore:
  - `fn append(&self, topic: &TopicRef, msg: StreamMessage) -> Result<u64>`
  - `fn create_reader(&self, topic: &TopicRef, start: StartPosition) -> Result<TopicStream>`
- TopicStream:
  - Implements `Stream<Item = Result<StreamMessage>>`
- WAL:
  - `fn append(&self, msg: &StreamMessage) -> Result<u64>`
  - `fn tail_reader(&self, from_offset: u64) -> WalTailReader`
  - Integrated `WALCache` as described
- CloudReader:
  - `fn read_range(&self, start: u64, end_hint: Option<u64>) -> impl Stream<Item = Result<StreamMessage>>`

---

## API/Crate Changes
- `danube-core/src/storage.rs`:
  - Adopt `PersistentStorage` as the sole storage trait. Deprecate `StorageBackend`.
    - Methods:
      - `appendMessage(topic, msg) -> offset`
      - `createReader(topic, from_offset) -> Stream<Message>`
      - `ackCheckpoint(topic, up_to_offset)` (internal for uploader)
      - `flush(topic)` (optional)
    - Provide a temporary compatibility adapter to support legacy callers during migration, then remove `StorageBackend` usages.

- Refactor existing crate: `danube-persistent-storage` to host the new design and opendal integration:
  - `Wal` (local disk)
  - `Uploader` (background task)
  - `CloudStore` (opendal operator factory; S3/GCS/local/memory)
  - `EtcdMetadata` (etcd client, schemas)

---

## Reader and Writer Flows (Multi-Subscription Topics)

### Reader Path (multiple subscriptions, different paces)
- Components:
  - `SubscriptionDispatch` (per-subscription, in `danube-reliable-dispatch/src/dispatch.rs`): coordinates delivery, backpressure, retries; no direct storage or ETCD writes.
  - `TopicStore` (per-topic, in `danube-reliable-dispatch/src/topic_storage.rs`): single facade for append/read; abstracts WAL vs CloudReader and uses `TopicCache`.
  - `TopicCache` (per-topic, in `danube-reliable-dispatch/src/topic_cache.rs`): hot/burst cache; no ETCD.
  - `WAL` (per-topic, in refactored `danube-persistent-storage`): durable append-only log with tailing reader.
  - `CloudReader` (per-topic, in refactored `danube-persistent-storage`): range-reads historical objects via `opendal`, guided by ETCD manifest.
  - `ProgressUpdater` (per-topic/group, in `danube-reliable-dispatch`): rate-limited ETCD writer for subscription progress.

- Flow per subscription:
  1) SubscriptionDispatch determines the start position:
     - New subscription: `StartPosition::Latest` (begin at current WAL tail; no historical replay).
     - Existing subscription: read last flushed progress from ETCD key `/danube/subscriptions/{ns}/{topic}/{sub}/progress` to get start offset `S`, using At-Least-Once semantics (resume at the next undelivered offset).
  2) Call `TopicStore::create_reader(topic, start)`:
     - If `start` is `Latest`: return WAL-backed tailing stream immediately (fast path; no CloudReader).
     - If `start` is `Offset(S)` and S within WAL tail window: return WAL-backed tailing stream (fast path).
     - Else (including when S falls before the retention floor due to inactivity): resolve `S..` by prefix/range scanning ETCD object descriptors under `/danube/storage/topics/{...}/objects/` (key-per-object, ordered by zero-padded `start_offset`), fetch with `CloudReader`, fill `TopicCache`, stream to dispatch, then switch to WAL when caught up via the ChainingStream adapter.
  3) `SubscriptionDispatch` delivers messages, and on delivery events enqueues `ProgressUpdater.record(sub, offset)` (no immediate ETCD write).
  4) `ProgressUpdater` flushes on timer/delta (e.g., every 5–10s or ≥N messages/bytes) using ETCD CAS to update progress.
  5) Slow subscriptions may trigger more `CloudReader` fetches; fast ones stay on WAL tail. Isolation is maintained per subscription.

### Writer Path (per-topic)
- Components:
  - `TopicStore.append` -> `WAL.append` (+ optional `TopicCache` warm).
  - `BackgroundUploader` (per-topic, in refactored `danube-persistent-storage`): batches WAL entries, uploads via `opendal`.

- Flow:
  1) Producer publish -> broker -> `TopicStore.append` -> `WAL.append` returns offset.
  2) Periodically (e.g., 10s) or on size threshold, `BackgroundUploader` reads new WAL entries, batches, uploads rolling objects (S3/GCS multipart).
  3) On success, ETCD updates (CAS):
     - `/danube/storage/topics/{...}/objects/{start_offset_padded}` -> object descriptor JSON {object_id, start_offset, end_offset, etag, completed, ...}
     - `/danube/storage/topics/{...}/upload/session` state transitions
  4) Write WAL checkpoint with last committed offset; prune old WAL once all subscription progress > segment end and objects committed.

### ETCD Write Ownership
- Separation of concerns to avoid overloading ETCD and to isolate failures:
  - `BackgroundUploader`: owns object manifest/session updates only.
  - `ProgressUpdater`: owns subscription progress updates only, with rate limiting.
- Both use ETCD leases/locks tied to topic leadership; only leader writes.
- Flush policies:
  - Progress: time-based (5–10s) and delta-based thresholds; force flush on shutdown/leadership change.
  - Objects: on each successful batch/rotation; resume/abort sessions on recovery as needed.

### Instantiation and Wiring (where components are created)
- In `danube-broker/src/topic.rs::subscribe` -> `danube-broker/src/subscription.rs::create_new_dispatcher` -> `danube-reliable-dispatch/src/lib.rs::new_subscription_dispatch`:
  - Create `SubscriptionDispatch` per subscription.
  - Ensure a per-topic `TopicStore` exists (holding `WAL`, `CloudReader`, `TopicCache`).
  - Ensure a per-topic `ProgressUpdater` exists and wire `SubscriptionDispatch` to send delivered offsets to it.
- On topic leader start:
  - Start `BackgroundUploader` for the topic (single instance per topic on the leader broker) guarded by ETCD lease/lock.

### Configuration Knobs (summary)
- Progress updater: `progress_flush_interval_seconds`, `progress_min_offset_delta`.
- Uploader: `interval_seconds`, `max_batch_bytes`, rotation thresholds.
- WAL: `fsync_interval_ms`, retention floors.
- ETCD: endpoints, namespace, leader lease parameters.

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
      # Retention floor to bound disk growth even if some subscriptions stall.
      # Segments can be pruned only if (1) uploaded+committed to ETCD, (2) all ACTIVE subs advanced past them,
      # and (3) they are older than min_minutes (and overall WAL size above min_bytes when applicable).
      min_minutes: 60
      min_bytes: 107374182400   # 100 GiB
      # A subscription is considered ACTIVE if it has progressed or heartbeated within this grace window.
      # Subscriptions inactive beyond this window do not block pruning and will catch up from cloud on resume.
      active_subscription_grace_seconds: 300
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
```

`opendal` Operator construction will map from `cloud` config to appropriate scheme and options.

- Retention knobs:
  - `wal.retention.min_minutes`, `wal.retention.min_bytes` define the floor that bounds local disk growth even if a subscription is stalled.
  - `wal.retention.active_subscription_grace_seconds` defines the inactivity window for classifying a subscription as active for pruning decisions. Inactive subs will transparently catch up via `CloudReader` when they resume.

---

## Dispatch Path Changes
- `danube-reliable-dispatch/src/topic_storage.rs` updated to:
  - Append to WAL for new messages.
  - Expose `create_reader(start: StartPosition)` returning an async Stream that sources WAL tail vs CloudReader transparently.
- `topic_cache.rs` becomes a thin shim over WALCache (or is deprecated).
- `dispatch.rs` refactored to remove Segment usage; consume `TopicStream` and track offsets.
- Delivery latency improves since no remote GRPC write is in hot path.

---

## Data and File Model
- Per-topic object key namespace: `persistent/{namespace}/{topic}/data`.
- Phase 1 options:
  1) Single logical object per topic with ongoing multipart upload (MPU) and periodic `CompleteMultipartUpload` for committed parts; on rotation create a new object with a monotonically increasing suffix.
  2) Rolling objects per topic (e.g., N-minute or size-based shards): `.../data-<epoch>-<start_offset>-<end_offset>.parquet` or `.bin`.

Trade-offs:
- True single-object append is not natively supported by S3/GCS; we rely on MPU append behavior. To simplify recovery and avoid MPU longevity issues, Phase 1 will implement rolling objects with clear offset ranges. This also simplifies consumer range reads. The requirement "one file per topic" is approximated by exposing a logical stream per topic, materialized as a small number of rolling objects. Rotation can be on time (10s) or size (e.g., 64–128MB), whichever comes first.

- Object format:
  - Phase 1: compact binary framing with a footer index. Each file contains a sequence of frames: [FrameHeader | Message(s) | CRC].
  - Optional: Snappy/LZ4 compression per frame.
  - Include minimal schema header and magic for self-description.
  - Future: Parquet/Arrow for analytics convergence.

---

## WAL Design
- Per-topic WAL on local disk, directory: `danube/{namespace}/{topic}/`. Files: `wal.log`, with rotation: `wal.log.<seq>`.
- WAL Entry types:
  - MessageEntry { offset, timestamp, key, headers, payload, crc }
  - CheckpointEntry { last_committed_offset, upload_session_id, crc }
- Guarantees:
  - fsync on batch append (configurable) for durability; use O_DIRECT/O_DSYNC where supported.
  - CRC32 on entries; per-file checksum in footer.
- Reader:
  - Supports tailing and replay from offset.
  - Exposes async stream to dispatch.
- Retention and pruning:
  - Maintain time/size-based retention floors to bound disk growth: `wal.retention.min_minutes`, `wal.retention.min_bytes`.
  - A WAL segment is eligible for pruning only when ALL of the following hold:
    1) It has been successfully uploaded to cloud and committed in ETCD manifest.
    2) All active subscriptions have progressed beyond the segment end offset (inactive/stale subscriptions do not block pruning).
    3) The segment age exceeds the configured retention floor (min_minutes) and, where applicable, overall WAL size is above `min_bytes` thresholds.
  - Inactive subscriptions that resume after the retention window will transparently read historical data from cloud via `CloudReader`.

- Writer model and ordering:
  - Single-writer per topic; appends are strictly ordered.
  - Offsets are monotonic u64 per topic; returned on append and used across dispatch/uploader/ETCD.

- Entry framing and durability:
  - Entry format: [len | kind | header(ts, key_len, headers_len, offset) | payload | CRC32C].
  - Atomicity via write + (f)data sync in batches; knobs: `wal.fsync_interval_ms`, `wal.max_batch_bytes`.
  - Partial write handling: on recovery, scan/verify CRC; truncate to last valid entry.

- Checkpoints and recovery:
  - CheckpointEntry includes { last_committed_offset, wal_file_seq, file_pos, active_upload {object_id, upload_id, part_no, part_etag?} }.
  - Recovery: validate WAL via CRC, restore checkpoint, reconcile with ETCD upload session, resume/rotate upload as needed.

- Rotation and retention:
  - Rotation triggers: size/time (e.g., `wal.max_file_bytes`, `wal.max_file_seconds`).
  - Prune only files fully covered by uploaded objects AND with all subscription progress > file end offset (from ETCD).
  - Safety margin: configurable lag bytes/time to account for eventual consistency.

- WALCache integration:
  - In-memory ring buffer capacity by bytes/time; tail reader prefers cache and falls back to tail files.
  - Evict ranges only after upload commit and after active subscribers have advanced beyond the range, and in alignment with the retention floor policy above.

- Concurrency and backpressure:
  - Append path non-blocking until cache/queue limits; then apply backpressure or reject per policy.
  - Independent readers; slow subscribers do not block fast ones.

- IO details:
  - Optional direct I/O or aligned buffered writes where supported.
  - Use hardware-accelerated CRC32C when available.

- Metrics:
  - wal.append_latency_ms, wal.fsync_latency_ms, wal.bytes_total, walcache.hit_ratio,
    wal.tail_read_latency_ms, wal.truncate_events.

References:
- AutoMQ WAL: WriteAheadLog, UploadWriteAheadLogTask.

---

## ChainingStream (Cloud → WAL handoff)
- Purpose: Provide a single continuous `Stream<Item = Result<StreamMessage>>` that first sources from `CloudReader` and then from the WAL tail without gaps or duplicates.
- Watermark (handoff point):
  - Compute `H` when the reader is initialized or when CloudReader scan begins:
    - Let `W0` be the minimum offset currently available in WAL (i.e., first non-pruned WAL offset).
    - Let `Oend` be the end_offset of the last COMPLETED cloud object covering the requested range.
    - Set `H = max(W0, Oend + 1)`. CloudReader emits messages in `[S, H-1]`. WAL tail starts at `H`.
  - If there is an active rolling (incomplete) object overlapping `W0`, prefer `H = W0` to avoid racing partial reads; remaining data is read from WAL.
- Operation:
  - Phase 1: Poll `CloudReader` until it returns `None` (i.e., delivered up to `H-1`). The adapter tracks the last delivered offset `L`.
  - Phase 2: Switch to WAL tail with `from_offset = max(H, L+1)`. Since offsets are monotonic and contiguous, no duplication occurs.
  - Concurrency: Producers may continue appending to WAL; the WAL tailing stream naturally carries on from `H`.
- Idempotence and ordering:
  - Offsets are strictly increasing per topic. The adapter enforces `next_offset = last_offset + 1` across the boundary and can drop any duplicate that violates this invariant (defensive check).
- Backpressure and errors:
  - Backpressure is applied uniformly by the adapter; transient CloudReader/WAL errors are surfaced to the caller with retry semantics governed by the higher layer.

---

## Background Uploader (Flusher)
- Triggered by timer (default 10s) or size threshold (e.g., 8MB).
- Reads WAL entries since last uploaded offset.
- Batches messages into a rolling object writer for the topic.
- Uses `opendal` Writer with multipart semantics for S3/GCS.
- On successful upload of a batch:
  - Update ETCD by writing/overwriting the per-object descriptor at key `/.../objects/{start_offset_padded}` to create or advance `end_offset` of the current rolling object.
  - Write WAL CheckpointEntry with the last committed offset and upload session id.
- On failure:
  - Retry with exponential backoff.
  - If MPU session is corrupted or expired, start a new object and update ETCD accordingly.
- Upload triggering and sizing:
  - Dual triggers: time-based (uploader.interval_seconds) and size-based (uploader.max_batch_bytes).
  - Target S3/GCS-friendly part sizes (e.g., 8–64MiB) to balance latency and cost.
- Object naming and rotation:
  - Rolling objects with offset ranges: `data-<epoch>-<start_offset>-<end_offset>.bin`.
  - Rotate on time or size; close object with a final commit and mark `completed=true` in ETCD.
- MPU/session lifecycle (idempotent/resumable):
  - Create or resume MPU on start using ETCD `/upload/session`.
  - Upload parts idempotently; store (part_no, etag, byte_range) locally and/or in session state.
  - On crash/restart: list parts (if supported) and reconcile with session, then complete or abort and rotate.
- Consistency and atomicity:
  - Treat an ETCD manifest update + WAL checkpoint as a logical commit unit.
  - Use ETCD Txn (CAS on the object descriptor at `/.../objects/{start_offset_padded}` and broker epoch) to append/extend object entries.
  - Only after ETCD commit, advance in-memory last_uploaded_offset.
- Backpressure and bandwidth control:
  - Limit concurrent in-flight parts and throttle throughput (configurable) to avoid saturating egress.
  - If uploader lags behind WAL growth beyond thresholds, signal pressure metrics and optionally slow producers.
- Checksums and integrity:
  - Compute per-part checksum (e.g., CRC32C) and validate ETag/CRC where backend supports it.
  - Store ETag in ETCD manifest for later verification by CloudReader.
- Security and encryption (optional):
  - Support server-side encryption (SSE-S3/KMS) or GCS AES256 where configured via opendal.
- Metrics and observability:
  - upload.batch_bytes, upload.latency_ms, upload.inflight_parts, upload.retries,
    manifest.txn_latency_ms, session.resume_events, session.abort_events.

References:
- AutoMQ ObjectWriter/S3Storage/S3Stream design for MPU handling.

---

## ETCD Metadata Schema
Key prefixes (all keys are examples; actual paths configurable):
- Topic object manifests (key-per-object model):
  - Namespace prefix: `/storage/topics/{ns}/{topic}/objects/`
  - Per-object descriptor key: `/storage/topics/{ns}/{topic}/objects/{start_offset_padded}`
    - `start_offset_padded` is a zero-padded decimal representation of `start_offset` such that lexicographic order == numeric order (e.g., width 20: `00000000000000001000`).
  - Object descriptor value (JSON):
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
  - Optional convenience key for current rolling object id: `/storage/topics/{ns}/{topic}/objects/cur` -> `start_offset_padded`
  - Reader logic: CloudReader performs a prefix scan on `/storage/topics/{ns}/{topic}/objects/` with an optional key range to fetch only relevant descriptors for `[from_offset, to_hint]`.
- Upload session state:
  - `/storage/topics/{ns}/{topic}/upload/session` -> { object_id, upload_id, last_part, last_committed_offset }
- Subscription progress:
  - `/subscriptions/{ns}/{topic}/{subscription}/progress` -> { offset, timestamp }
- WAL checkpoints metadata (optional if contained in WAL file):
  - `/storage/topics/{ns}/{topic}/wal/last_checkpoint` -> { offset, file_seq, file_pos }

Atomicity:
- Use ETCD transactions (compare-and-swap) to atomically write or update the per-object descriptor and subscription progress as needed.
- Include broker epoch/lease to prevent split-brain writers.

---

## Metrics & Observability
- Expose metrics: WAL append latency, fsync latency, bytes in WAL, upload batch size, upload latency, MPU parts, ETCD tx latency, consumer lag.
- Structured logging with topic, offsets, object ids.

---

## Migration Plan
Phase A: Introduce WAL + WALCache alongside existing storage (hot path uses WAL)
- Implement WAL writer/reader with integrated WALCache and use it in dispatch hot path via `TopicStore::create_reader` (async Stream).
- Keep legacy segment code compiled but not used by default (fallback only during transition).

Phase B: Cloud Uploader via `opendal` with ETCD manifests
- Implement S3/GCS/fs/memory backends via `opendal` Operator.
- Start persisting rolling objects and writing ETCD manifest/session metadata (Option A ownership).
- Validate recovery: WAL checkpoint + ETCD session resume/rotate on restart/leadership change.

Phase C: Historical reads behind TopicStore via CloudReader
- Implement ranged reads from objects guided by ETCD manifest; feed unified `TopicStream`.
- Make `topic_cache` a thin shim over WALCache or remove if feasible.

Phase D: Remove remote GRPC storage and finalize configuration/docs
- Remove `managed_storage.rs` and related config.
- Deprecate segment-based code paths in dispatch; update samples and docs.

---
