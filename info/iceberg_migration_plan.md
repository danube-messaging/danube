# Danube: Migration Plan to Iceberg-Native Storage (Deprecate Segment Store)

This document defines a staged plan to replace the legacy segment-based storage (implemented in `danube-reliable-dispatch` and backed by `danube-persistent-storage`) with the new Iceberg-native storage implemented in `danube-iceberg-storage`. The end state removes `StorageBackend` and the entire `danube-persistent-storage` crate; `IcebergStorage` becomes the only persistent storage.

References:
- `info/native_storage_iceberg.md`
- `info/danube_iceberg_storage_overview.md`
- `danube-core/src/storage.rs`

Goals:
- Stateless brokers for persistence (compute/storage disaggregation)
- Low-latency producer acks via WAL
- Durable, transactional commits via Apache Iceberg snapshots
- Multi-subscription fan-out with independent progress
- Efficient I/O using WAL batching and a small in-memory cache (moka)

---

Note on PR strategy:
- This is a staged implementation plan, but Stages 0–3 will be delivered within a single PR to surface and fix compile-time errors immediately.
- Stages 4–5 (testing/observability) may be delivered as a follow-up PR depending on scope.

---

## Stage 0 — Subscription Progress and Exactly-Once Semantics 

Objective: Robust progress tracking for multiple subscriptions.

Status: IMPLEMENTED (cursor-based, time-based etcd updates)

- etcd progress (cursor, time-based updates):
  - Persist a per-subscription CURSOR representing last known position: `{ snapshot_id, file_path, row_offset, updated_ms }` under key `/persistent_storage/iceberg/subscriptions/{subscription}/{namespace}/{topic}`.
  - Update cadence is time-based (default: every 10 seconds). We do NOT write per message nor per batch of messages; we overwrite the cursor periodically with the latest observed position.
  - Implement a lightweight in-memory cursor cache for each active subscription; a background task flushes the current cursor for all active subscriptions every N seconds.
  - Accept small replay on resume: if a subscriber was blocked or crashed, it may re-read from the last flushed position (<= N seconds old). This is acceptable by design and avoids excessive etcd writes.
- Idempotency:
  - Ensure `TopicReader` resumes from the persisted cursor on restart.
  - Provide at-least-once delivery by default, with optional deduplication at consumer if needed.
- Resume behavior (planned follow-up):
  - On stream creation, read etcd cursor (if present) and initialize the reader with `last_snapshot_id` to filter scans to newer snapshots.
  - For finer granularity (file + row offset), plan file tasks for that snapshot and skip rows until `row_offset` for the starting file; otherwise start at snapshot boundary.

Code changes:
- `danube-iceberg-storage/`:
  - IMPLEMENTED: timed cursor flusher (`DANUBE_SUB_CURSOR_FLUSH_MS`, default 10s) and `update_subscription_cursor()` API in `IcebergStorage`.
  - IMPLEMENTED: `PersistentStorage::create_subscription_stream(namespace, topic, subscription)` and storage method that seeds from etcd cursor (snapshot backfill) and then attaches to live broadcast.
- `danube-broker/` and `danube-reliable-dispatch/`:
  - TODO: call storage `update_subscription_cursor()` at logical delivery checkpoints (post-ack to subscriber) with the latest observed snapshot/file/offset.

Deliverables:
- Implemented: cursor-based progress with background flusher.
- Pending: TopicReader seeding from cursor, dispatch wiring, and config/docs updates.

---

## Stage 1 — Immediate Removal of Legacy Segment Store (single PR)

- Delete legacy types and errors in `danube-core/src/storage.rs`:
  - Remove `StorageBackend` trait and `StorageBackendError` enum.
  - Remove `Segment` struct and any helpers.
- Simplify `StorageConfig` to only include `Iceberg { iceberg_config }` , keep `InMemory` for tests only. Remove `Local` and `Remote` variants.
- Remove the `danube-persistent-storage/` crate from the workspace and root `Cargo.toml`.
- Update broker configuration handling: error if a non-Iceberg storage type is configured.
- Allow compile failures to surface any remaining references across the workspace and fix them as part of this PR.
- Update READMEs and config docs to state Iceberg is the only persistent storage backend.


## Stage 2 — Integrate Iceberg in Reliable Dispatch (in same PR)

Objective: Replace segment writes/reads with `PersistentStorage` APIs from `danube-iceberg-storage`.

Code changes:
- `danube-reliable-dispatch/`:
  - Replace Topic store abstractions that manipulate `Segment` with a storage façade over `PersistentStorage`.
  - Route all write paths through:
    - `PersistentStorage::store_messages(topic, Vec<StreamMessage>)`
  - Replace read paths to create subscription streams via:
    - `PersistentStorage::create_subscription_stream(namespace, topic, subscription)`
  - Map subscription types to stream semantics:
    - Exclusive: single receiver from broadcast; ack/progress stored per-subscription key in etcd.
    - Shared: multiple receivers; fair dispatch at the subscription layer (existing dispatcher logic). Each consumer tracks its own progress if needed.
    - Failover: one active, others standby; leverage the same broadcast receiver per subscription group, with leader-election elsewhere as today.
  - Remove direct usage of `Segment`, `get_segment/put_segment` calls and local segment caches within dispatch code.
- `danube-broker/`:
  - Wire broker startup to create `IcebergStorage` from config (already added) and pass it into reliable dispatch constructors.
  - Ensure graceful shutdown calls `PersistentStorage::shutdown()`.

Behavioral details:
- On publish: dispatch calls `store_messages()`; this performs WAL append and immediately ACKs producers.
- Reader fan-out: `IcebergStorage` provides a per-topic broadcast; bridge to per-subscription receivers.
- Start positions: pluggable via etcd-stored subscription cursor (snapshot/file/offset or logical offset). Provide helper functions in `danube-iceberg-storage` to serialize/deserialize cursors.

Deliverables:
- In this stage redirecting writes and reads to `PersistentStorage`.
- Integration tests ensuring end-to-end produce/consume with Iceberg for Exclusive/Shared/Failover.

---

## Stage 3 — WAL Batching and Caching with moka (in same PR where feasible)

Objective: Optimize I/O using WAL batching and a small in-memory cache.

- WAL batching (already supported):
  - Tune `writer_batch_size`, `writer_flush_interval_ms`, `writer_max_memory_bytes` in `IcebergStorage` based on workload.
  - Add backpressure: if WAL backlog grows beyond threshold, slow down producers via dispatcher signaling.
- Two-tier cache with `moka`:
  - Hot WAL cache: keep last N seconds of messages already appended to WAL but not yet committed, keyed by `(topic, wal_offset_range)`.
  - Recent snapshot cache: decoded Arrow `RecordBatch`es from the most recent committed snapshot files, keyed by `(topic, snapshot_id, file_path, row_group)`.
  - Size/TTL controlled via config; cache is best-effort to reduce reads of fresh data and repeated Parquet scans during bursts.
- Fast-path reads:
  - When a consumer starts or catches up, serve from WAL cache first (low latency), then from recent snapshot cache, then fall back to Iceberg scan.

Code changes:
- `danube-iceberg-storage/`:
  - Implement optional cache module using `moka::sync::Cache` with metrics.
  - Integrate cache checks in `TopicReader` and stream path (without breaking correctness).

Deliverables:
- Stage 2: adding cache configuration and integration.
- Benchmarks showing improved p50/p99 consumer latencies under bursty workloads.

---

## Stage 4 — Testing, CI, and Benchmarks (follow-up MR if needed)

- Integration tests:
  - Use REST catalog with local filesystem for CI (per `info/danube_iceberg_storage_overview.md` guidance), and memory object store for speed.
  - Cover multi-subscription fan-out and restart scenarios.
- Compatibility:
  - Ensure client-facing semantics (Exclusive/Shared/Failover) are preserved.
- Performance:
  - Benchmarks for producer latency vs batch size/flush interval.
  - Consumer throughput and end-to-end latency with and without caches.
- Flaky handling:
  - Add retries and robust error mapping for catalog/object store errors in `PersistentStorageError`.

---

## Stage 5 — Observability and Operations (follow-up MR if needed)

- Metrics:
  - WAL length, write/commit throughput, snapshot lag, cache hit rates, reader poll intervals.
- Logging/tracing:
  - Structured `tracing` spans for write/commit and read/scan paths.
- Config docs:
  - Update `config/danube_broker_iceberg.yml` example and user guide in `info/`.

Deliverables:
- PR enhancing metrics and docs.

---

## Implementation Tracking

- [x] Stage 0: Subscription progress (cursor, time-based etcd)
  - [x] Add timed etcd cursor recording (default 10s) and `update_subscription_cursor()`
  - [x] Implement `create_subscription_stream(namespace, topic, subscription)` (trait + storage) with etcd-seeded backfill and live broadcast
  - [x] Accept replay semantics and document behavior
  - [ ] Wire dispatcher to update cursors at delivery checkpoints
  - [ ] Document `DANUBE_SUB_CURSOR_FLUSH_MS` in config/examples
- [ ] Stage 1: Remove legacy storage
  - [ ] Delete `StorageBackend`, `StorageBackendError`, `Segment` from `danube-core/src/storage.rs`
  - [ ] Simplify `StorageConfig` to `Iceberg { iceberg_config }` (keep `InMemory` for tests); remove `Local`/`Remote`
  - [ ] Remove `danube-persistent-storage/` from workspace and root `Cargo.toml`
  - [ ] Broker config: error on non-Iceberg storage type
  - [ ] Docs updated to reflect Iceberg-only persistence
- [ ] Stage 2: Integrate Iceberg in reliable dispatch
  - [ ] Writes go through `PersistentStorage::store_messages()`
  - [ ] Reads use `PersistentStorage::create_subscription_stream()`
  - [ ] Fan-out preserved via per-topic broadcast; per-subscription receivers
  - [ ] Exclusive/Shared/Failover semantics validated
- [ ] Stage 3: WAL batching + moka caching
  - [ ] Add optional caches (WAL hot cache + recent snapshot cache)
  - [ ] Configurable sizes/TTL and metrics
  - [ ] Benchmarks for p50/p99 with/without cache
- [ ] Stage 4: Testing/CI (follow-up PR)
  - [ ] REST + filesystem CI, memory object store for data
  - [ ] Multi-subscription + restart scenarios
- [ ] Stage 5: Observability (follow-up PR)
  - [ ] Metrics: WAL depth, commit throughput, snapshot lag, cache hits, reader polls
  - [ ] Tracing spans for write/commit and read/scan paths

---

## Finalization

- Ensure documentation, examples, and configs reflect Iceberg-only persistence.
- Remove any residual mentions of segments in code comments and docs.

Deliverables:
- Doc cleanups and final polish.

---

## Notes from Current Implementation

- Follow the design in `info/native_storage_iceberg.md` and `info/danube_iceberg_storage_overview.md`.
- For local CI/tests, prefer REST catalog with local filesystem metadata and memory object store for data. This bypasses the `MemoryCatalog` update limitation while keeping tests fast.
- Align Arrow/Iceberg crate versions as documented and keep WAL sync and batching configurable.
- Topic/table mapping:
  - Current implementation uses a fixed Iceberg namespace `"danube"` and creates one table per topic: `TableIdent::from_strs(["danube", topic])` in `TopicWriter::ensure_table_exists()` and `TopicReader`.
  - We can make namespace configurable in `IcebergConfig` as a follow-up enhancement.
- Resume flow via etcd + Catalog:
  - On resume, we read etcd progress, then use the Iceberg `Catalog` trait (`load_table`, `scan`, `plan_files`) to create a stream starting from the recorded snapshot (and file/offset when supported). This assumption is correct and is how Stage 3 will be wired.

---

## FAQ

Q: How does topic-to-namespace mapping work?
A: Currently, we use a fixed Iceberg namespace `"danube"` and create one table per topic. This can be made configurable in `IcebergConfig` as a follow-up enhancement.

Q: How does resume flow work using etcd + Catalog?
A: On resume, we read etcd progress, then use the Iceberg `Catalog` trait to create a stream starting from the recorded snapshot (and file/offset when supported).

---

--------------------------------------------------------

--------------------------------------------------------
