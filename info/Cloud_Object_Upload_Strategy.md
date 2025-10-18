# Cloud Object Upload Strategy (Danube WAL -> Cloud)

## Context
- **Module**: `danube-persistent-storage/`
- **Current behavior**: `uploader_stream::stream_frames_to_cloud()` uploads at most one WAL file per cycle and finalizes a cloud object (e.g., `data-<start>-<end>.dnb1`). With short `interval_seconds` and low traffic, this yields many small objects.
- **Constraints**:
  - S3/MinIO objects are immutable after finalize. No true append to finalized objects.
  - OpenDAL `writer_with()` abstracts Multipart Upload (MPU) and finalizes on `close()`.
  - We want larger objects for efficiency while maintaining timely persistence.

## Goals
- **Fewer, larger objects** to optimize cloud read/write costs and listing overhead.
- **Bounded time-to-cloud** for durability (can’t wait indefinitely).
- **No manual MPU**: rely on OpenDAL `writer_with()`.
- **Crash safety**: WAL ensures durability; uploader is best-effort and idempotent.

---

## Option B: Multi-file Aggregation per Cycle (Quick Win)

### Summary
Aggregate frames across multiple WAL files within a single uploader cycle, up to a configurable size/message cap, then finalize a single object. This removes the current "one-file-per-cycle" limitation.

### Changes
- **Config** (`docker/danube_broker.yml` → `UploaderNode` in `service_configuration.rs` → `UploaderBaseConfig`/`UploaderConfig`):
  - `max_object_bytes: Option<u64>` (e.g., 1 GiB)
  - `max_object_messages: Option<u64>` (optional)
- **Streamer** (`danube-persistent-storage/src/uploader_stream.rs`):
  - Remove early `break` after the first file that yields frames.
  - Maintain `state.total_bytes_uploaded` and optionally a `msgs_written` counter.
  - Continue reading subsequent WAL files in order while thresholds are not met.
  - Finalize pending object into `data-<start>-<end>.dnb1` when a threshold is reached or no more complete frames are available.

### Algorithm (high-level)
1. Build ordered list of WAL files from `WalCheckpoint` (rotated + active).
2. Initialize in-memory `carry` buffer and `UploadState` (includes `first_offset`, `last_offset`, `total_bytes_uploaded`, `index_entries`).
3. For each file `seq >= start_seq`:
   - Read in 10 MiB chunks (as today) → extend `carry`.
   - Compute `safe_len = scan_safe_frame_boundary_with_crc(carry)`.
   - If `safe_len > 0`:
     - Lazily open writer via `cloud.open_streaming_writer()` on first frame, choose key `storage/topics/{topic}/objects/data-<start>-pending.dnb1`.
     - Build/extend sparse index within `safe_len`.
     - Write `carry[..safe_len]` to the writer; drain from `carry`; update counters.
     - If `total_bytes_uploaded >= max_object_bytes` (or `msgs >= max_object_messages`): stop scanning further files.
4. If nothing was uploaded: return `None`.
5. Otherwise, close writer and finalize pending → final key `data-<start>-<end>.dnb1` (via `copy_object`/fallback).
6. Return `(object_id, start_offset, end_offset, next_seq, next_pos, meta, index)` back to `uploader.rs`, which commits the descriptor and checkpoint.

### OpenDAL Usage
- Continue to use `writer_with(chunk, concurrent)`:
  - Chunk: 8–16 MiB
  - Concurrent: 4–8
- Optional `WriteOptions.user_metadata` can store lightweight hints (topic, start offset), but authoritative metadata remains in ETCD `ObjectDescriptor`.

### Pros/Cons
- **Pros**: Simple; much larger objects when backlog exists; no persistent writer state.
- **Cons**: Object size depends on available backlog within one cycle; still limited if traffic is low and `interval_seconds` is short.

### Config Example
```yaml
wal_cloud:
  uploader:
    enabled: true
    interval_seconds: 300
    root_prefix: "/danube-data"
    max_object_bytes: 1073741824  # 1 GiB
    # max_object_messages: 1000000  # optional
```

### Observability
- Add counters/gauges:
  - `uploader_bytes_per_object`
  - `uploader_msgs_per_object`
  - `uploader_objects_created_total`
  - `uploader_failures_total`

---

## Option A: Long-Lived Writer Across Cycles (Rolling MPU)

### Summary
Keep a single `CloudWriter` (OpenDAL writer) open per topic across multiple uploader ticks and stream frames into it until size/message/time thresholds are met; then close to finalize. On crash/restart, re-upload from WAL rather than resuming the same MPU (OpenDAL doesn’t expose resumption of an in-flight MPU).

### Changes
- **Config thresholds** (in addition to Option B):
  - `max_open_seconds: Option<u64>` – close object after this time window is reached.
  - `max_idle_seconds: Option<u64>` – close if no new data for N seconds.
- **Uploader state** (`danube-persistent-storage/src/uploader.rs`):
  - Maintain optional in-memory `CloudWriter` and `UploadState` across ticks.
  - Extend `UploaderCheckpoint` to persist logical progress for in-progress object:
    - `active_start_offset: Option<u64>`
    - `active_bytes_written: u64`
    - `active_msgs_written: u64`
    - `index_entries_in_progress: Vec<(u64 offset, u64 byte_pos)>`
    - `opened_at_unix: u64`
    - `last_activity_unix: u64`
  - On restart: abandon any previous in-progress pending object (not finalized), rebuild from WAL.

### Algorithm (high-level)
1. On uploader start or tick:
   - If no active writer: start new pending object at next frame boundary encountered; initialize state with `start_offset`.
   - Else: continue appending frames from WAL starting at `(seq,pos)` recorded in checkpoint.
2. Write chunks, update `active_bytes_written`, `active_msgs_written`, `index_entries_in_progress`.
3. If any threshold reached (`max_object_bytes`, `max_object_messages`, `max_open_seconds`, `max_idle_seconds`):
   - Close writer; finalize pending → final key `data-<start>-<end>.dnb1`.
   - Commit descriptor + checkpoint.
   - Clear active writer state.
4. Otherwise: persist checkpoint with updated `(next_seq, next_pos)` and in-progress metrics, keep writer open.

### OpenDAL Usage
- Stick with `writer_with(chunk, concurrent)`; let OpenDAL manage MPU.
- We don’t rely on resuming an existing MPU after process crashes; WAL replay guarantees idempotent rebuild.
- Optional: set `user_metadata` on finalization; not useful for open-writer resumption.

### Pros/Cons
- **Pros**: Predictable large object sizes even at steady/low throughput; bounded time-to-cloud.
- **Cons**: Requires careful state handling and timers; on crash we may re-upload some data (still correct).

---

## Backward Compatibility & Migration
- Existing small objects remain valid; readers (`cloud_reader.rs`) already handle ordered sequences by descriptors.
- New config fields are optional with sensible defaults (no size cap = current behavior).
- Rollout plan:
  1. Implement Option B (multi-file per cycle + `max_object_bytes`).
  2. Add metrics/dashboards.
  3. Observe object sizing; tune `interval_seconds` and `max_object_bytes`.
  4. Implement Option A long-lived writer and thresholds when needed.

## Testing
- Unit tests for `uploader_stream.rs` to validate:
  - Stops at `max_object_bytes` boundary and finalizes.
  - Index building spans across multiple files.
  - Correct `(next_seq, next_pos)` after large object.
- Integration tests:
  - Low throughput: ensure fewer, larger objects as `interval_seconds` increases.
  - Crash during in-progress object: verify that on restart uploader rebuilds a correct object from WAL.

## Security & Permissions
- Continue to rely on configured S3 credentials and endpoint.
- Ensure `virtual_host_style` is set appropriately for MinIO.

## Observability
- Add structured logs around start/end of object creation, thresholds reached, finalize outcome, and errors.
- Expose Prometheus metrics listed above.

## Example Pseudocode Patch Points
- `uploader_stream.rs`:
  - Remove early break; loop over files until thresholds.
  - Track `total_bytes_uploaded` and message count.
- `uploader.rs`:
  - Extend `UploaderConfig` with size/message/time thresholds.
  - Pass into `stream_frames_to_cloud(...)`.
- `service_configuration.rs`:
  - Extend `UploaderNode` to parse optional thresholds from YAML.

## Defaults (proposed)
- `interval_seconds`: 300
- `max_object_bytes`: 1_073_741_824 (1 GiB)
- `max_object_messages`: None
- `max_open_seconds` (for Option A): 600

