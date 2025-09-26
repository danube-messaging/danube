# WAL Performance and Structure Improvement Plan

This document tracks the implementation plan to optimize `danube-persistent-storage` WAL performance and structure for Danube’s traffic pattern (multi-topic, per-topic WAL with tailing readers per subscription).

Scope covers `danube-persistent-storage/src/wal.rs`, with integration touch points in `wal_storage.rs` and `wal_factory.rs`.


## Assessment Summary

- Hot path `Wal::append()` performs disk I/O and acquires multiple `Mutex` locks in async context:
  - Locks: `file`, `write_buf`, `current_file_bytes`, `last_flush`, `cache` (twice).
  - Holds lock across `await` on `file.write_all()` and `file.flush()`, risking latency spikes and contention.
- Cache is a `VecDeque` FIFO, not optimized for random/range lookups; `tail_reader()` dedupes via `HashSet` and scans the whole file then iterates cache.
- Rotation/checkpoint logic is coupled into hot path state and requires locking.
- Serialization:
  - WAL entries use `bincode` (good), but checkpoints use `serde_json` (slower, larger).
- Structure: monolithic `wal.rs` mixing API, I/O, cache, and checkpoint responsibilities.


## Goals

- Minimize producer latency and contention by removing I/O from the hot path.
- Provide ordered, efficient replay for readers from an offset with minimal dedup and minimal file reads.
- Improve maintainability by structuring WAL into focused modules.
- Preserve current public APIs of `Wal`, `WalStorage`, `WalFactory`.


## Design Overview

1) Decouple appends from I/O via a single-writer background task:
- Introduce `tokio::sync::mpsc` channel. Hot path only serializes and enqueues commands.
- Dedicated I/O task owns: `File`/`BufWriter`, `write_buf`, rotation counters, `last_flush`, and performs flush/rotate/checkpoint.
- `Wal::append()` becomes: compute next offset (atomic), serialize via `bincode`, push to cache, send `LogCommand::Write { offset, bytes }`, return.

2) Replace FIFO cache with ordered map:
- Use `BTreeMap<u64, StreamMessage>` for ordered range iteration and predictable eviction of oldest offsets.
- Capacity-bound by count; on insert, evict from the smallest key upward until within capacity.

3) Reader optimization:
- `tail_reader()` performs stitched replay without full-file scan/dedup:
  - Determine `cache_start = cache.keys().next().unwrap_or(u64::MAX)`.
  - If `from < cache_start` and WAL file exists: read file range `[from, cache_start)` only.
  - Then serve `[max(from, cache_start), ..]` directly from `cache.range(..)`.
  - Chain with live broadcast for tailing.

4) Checkpoints and buffering:
- Switch `WalCheckpoint` and `UploaderCheckpoint` to `bincode`.
- Wrap file in `tokio::io::BufWriter` inside the I/O task; flush on size/time thresholds.

5) Module structure:
- `wal/` directory with:
  - `mod.rs` (public API: `Wal`, `WalConfig`, `UploaderCheckpoint`)
  - `storage.rs` (I/O task loop, `LogCommand`, rotation/checkpoint writing)
  - `cache.rs` (ordered cache with capacity eviction)
  - `errors.rs` (if module-specific errors emerge)


## Detailed Implementation Steps

### Phase 1: Infrastructure and API preservation — STATUS: COMPLETED ✅
- Add `LogCommand` enum:
  - `Write { offset: u64, msg: StreamMessage, bytes: Vec<u8> }`
  - `Flush`
  - `Rotate`
- Add `mpsc::Sender<LogCommand>` to `WalInner`; spawn I/O task with `Receiver` in `Wal::with_config()`.
- Move file/buffer/rotation/checkpoint state into I/O task struct. Remove their `Mutex` wrappers from `WalInner`.
- Introduce `BufWriter<File>` inside I/O task; policy: flush when either `max_batch_bytes` reached or `fsync_interval_ms` elapsed.
- Keep `broadcast::Sender` as-is for live tailing.

### Phase 2: Cache refactor — STATUS: COMPLETED ✅
- Replace `VecDeque<(u64, StreamMessage)>` with `BTreeMap<u64, StreamMessage>` in `WalInner`.
- Update `append()` to insert into map and evict oldest while `len > capacity`.
- Expose helper for reading a range from cache: `cache.range(from..)` when needed.

### Phase 3: Reader stitching — STATUS: COMPLETED ✅
- Implement `read_file_range(path, from, to_exclusive)` in I/O/read module.
- Update `tail_reader(from)`:
  - Determine `wal_path` once.
  - Compute `cache_start` and conditionally call `read_file_range` for `[from, cache_start)`.
  - Append items from `cache.range(max(from, cache_start)..)`.
  - Eliminate `HashSet` duplicate filtering and post-sort; ordering is intrinsic.
  - Maintain watermark and chain with live broadcast as today.

### Phase 4: Checkpointing improvements — STATUS: COMPLETED ✅
- Change `WalCheckpoint` and `UploaderCheckpoint` to `bincode` serialization.
- Ensure atomic file replace (write tmp + rename) when writing checkpoints from I/O task.
- Keep compatibility: no external tooling depends on JSON.

### Phase 4.1: Writer task ownership & shutdown — STATUS: COMPLETED ✅
- Move writer-specific state out of `WalInner` and into `run_writer()` as an owned `WriterState` (no mutexes for writer path):
  - `BufWriter<File>`, `write_buf`, `last_flush`, `current_file_bytes`, rotation counters.
- Pass `LogCommand::Write { offset, bytes }` (or message) and let the writer manage buffering/flush/rotation.
- Add graceful shutdown:
  - Extend `LogCommand` with `Shutdown(oneshot::Sender<()>)`.
  - On `Drop` of `Wal`, send `Shutdown` and await ack, performing final flush.
- Benefits: clearer ownership model, zero locking in writer hot path, durable shutdown.

### Phase 5: Code structure refactor — STATUS: COMPLETED ✅
- Created `danube-persistent-storage/src/wal/` and moved components accordingly.
- Kept `danube-persistent-storage/src/wal.rs` as the module root (no `mod.rs`).
- Added submodules: `writer.rs`, `reader.rs`, `cache.rs`, `checkpoints.rs`.
- `wal.rs` declares `mod writer; mod reader; mod cache; mod checkpoints;`, wires writer::run, and re-exports `UploaderCheckpoint`.
- Public API unchanged for callers: continue to use `Wal`; external import path `crate::wal::{Wal, UploaderCheckpoint}` works.

### Phase 6: Integration and tests — STATUS: PENDING
- Verify `WalStorage` and `WalFactory` need no signature changes.
- Add tests/benchmarks:
  - Append throughput under contention (N tasks appending concurrently).
  - Tail latency measurement (time from append to reader receive).
  - Reader catch-up from offset far behind cache (file+cache stitch correctness).
  - Rotation + checkpoint correctness with I/O task.


## I/O Task Sketch

Pseudocode of the new background writer task:

```rust
enum LogCommand {
    Write { offset: u64, bytes: Vec<u8> },
    Flush,
    Rotate,
}

struct WriterState {
    file: Option<tokio::fs::File>,
    buf: tokio::io::BufWriter<tokio::fs::File>,
    last_flush: Instant,
    bytes_in_file: u64,
    file_started: Instant,
    file_seq: u64,
    wal_path: Option<PathBuf>,
    checkpoint_path: Option<PathBuf>,
    fsync_interval_ms: u64,
    max_batch_bytes: usize,
    rotate_max_bytes: Option<u64>,
    rotate_max_seconds: Option<u64>,
}

impl WriterState {
    async fn process(&mut self, cmd: LogCommand) -> Result<(), PersistentStorageError> { /* ... */ }
    async fn maybe_flush_and_checkpoint(&mut self, last_offset: u64) -> Result<(), PersistentStorageError> { /* ... */ }
    async fn maybe_rotate(&mut self) -> Result<(), PersistentStorageError> { /* ... */ }
}

async fn run_writer(mut state: WriterState, mut rx: mpsc::Receiver<LogCommand>) {
    while let Some(cmd) = rx.recv().await {
        if let Err(e) = state.process(cmd).await { /* log + continue */ }
    }
}
```

`Wal::append()` will prepare frame `[offset|len|crc|bytes]` and send `Write { offset, bytes }`.


## Performance Considerations

- Hot path no longer awaits on disk I/O or contends on multiple `Mutex`es.
- `BTreeMap` provides O(log n) insert and efficient range iteration; overall superior for reader catch-up vs scanning vectors.
- `BufWriter` reduces syscalls; periodic flush preserves durability target.
- Broadcast path remains unchanged; publish happens after cache insert to maintain order for live readers.


## API Compatibility and Behavior Notes

- `Wal::append()` semantics unchanged (returns assigned offset immediately after enqueue and cache update).
- `Wal::tail_reader(from)` semantics unchanged; now more efficient when `from` < cache start.
- `WalStorage::create_reader()` behavior unchanged, including Cloud→WAL chaining logic.
- Checkpoint file format changes to binary; no external dependency expected.


## Risks and Mitigations

- I/O task crash: Enclose loop with error logging; on fatal errors, attempt to reopen file and continue. Consider a kill-switch metric.
- Channel backpressure: Configure bounded channel size; if full, either apply backpressure on producers (await send) or drop with error (configurable). Initial plan: await send.
- Ordering: Single writer task ensures strict append ordering on disk.
- Shutdown: Future enhancement to add graceful shutdown signal to flush+checkpoint; current drop semantics acceptable for now.


## Metrics to Add

- `wal.append_enq_latency_us` (hot path)
- `wal.writer_flush_latency_ms`, `wal.writer_flush_bytes`
- `wal.writer_rotate_events`
- `wal.cache.size`, `wal.cache.evictions`
- `wal.reader.stitch_file_bytes`, `wal.reader.stitch_cache_items`


## Acceptance Criteria

- 3–10x reduction in P50/P99 `append_message` latency under concurrent producers.
- Reader catch-up from behind the cache does not re-read overlapped ranges from file; no duplicates and correct ordering.
- Rotation and checkpoint continue to function; uploader checkpoints still written.
- All existing tests pass; new perf tests show expected improvement.


## Work Breakdown Checklist (live)

- [x] Implement `LogCommand` and I/O task; wire `mpsc` in `Wal::with_config()`.
- [x] Remove hot-path file/buffer locks; migrate state to I/O task.
- [x] Replace cache with `BTreeMap` + eviction; unify cache locking in `append()`.
- [x] Implement `read_file_range(path, from, to_exclusive)` and update `tail_reader()` stitching.
- [x] Switch checkpoints to `bincode` and atomic write (tmp+rename) in I/O task.
- [x] Wrap WAL file in `BufWriter`; enforce flush policy.
- [x] Move writer state into `WriterState` owned by `run_writer()`; remove writer-related mutex fields from `WalInner`.
- [x] Implement graceful shutdown: `LogCommand::Shutdown(oneshot)`; flush and ack before exit.
- [x] Log `broadcast` send errors in `append()` to detect slow/lagging consumers.
- [x] Restructure files into `wal/` modules; keep public API stable.
- [ ] Update/extend tests; add simple benchmarks.
- [ ] Verify integration with `WalStorage` and `WalFactory`.


## References

- `danube-persistent-storage/src/wal.rs`
- `danube-persistent-storage/src/wal_storage.rs`
- `danube-persistent-storage/src/wal_factory.rs`
- `info/WAL_Cloud_Persistence_Design.md`
