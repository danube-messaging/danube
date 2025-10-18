# Cloud Object Upload Strategy (Danube WAL -> Cloud)

## Context
- **Module**: `danube-persistent-storage/`
- **Implemented behavior**: `uploader_stream::stream_frames_to_cloud()` aggregates frames across multiple WAL files within a single cycle and produces one cloud object per tick (e.g., `data-<start>-<end>.dnb1`). Upload stops when the optional `max_object_mb` cap is reached or frames are exhausted for the cycle.
- **Constraints**:
  - Cloud objects are immutable after finalize. No true append to finalized objects.
  - OpenDAL `writer_with()` abstracts Multipart Upload (MPU) and finalizes on `close()`.
  - We want larger objects for efficiency while maintaining timely persistence.

## Goals
- **Fewer, larger objects** to optimize cloud read/write costs and listing overhead.
- **Bounded time-to-cloud** for durability (can’t wait indefinitely).
- **No manual MPU**: rely on OpenDAL `writer_with()`.
- **Crash safety**: WAL ensures durability; uploader is best-effort and idempotent.

---

## Chosen Approach: Option A-lite (Option B++)

### Summary
Stream sequentially across multiple WAL files within a single uploader tick into one `CloudWriter`, producing a single large object per tick, while keeping memory bounded by fixed chunk sizes. The writer is not kept open across ticks; on crash, the tick restarts and rebuilds the object from WAL. This is an extension of Option B (multi-file per cycle) with a strong emphasis on sequential streaming and bounded memory.

### Changes
- **Config** (extend `UploaderConfig`):
  - `max_object_mb: Option<u64>`
- **Streamer** (`danube-persistent-storage/src/cloud/uploader_stream.rs`):
  - Remove the early stop after the first WAL file that yields frames.
  - Continue to subsequent WAL files in the same tick while size threshold isn’t reached.
  - Track `total_bytes_uploaded` (in bytes) for comparison against `max_object_mb`.
- **Uploader** (`danube-persistent-storage/src/cloud/uploader.rs`):
  - Pass thresholds into `stream_frames_to_cloud(...)` from `run_once()`.
  - Optionally perform defensive cleanup of any leftover `*-pending.dnb1` objects at start.

### Algorithm (single tick)
1. Determine `(start_seq, start_pos)` from `UploaderCheckpoint` and WAL watermark from `WalCheckpoint`.
2. Open one pending writer: `storage/topics/{topic}/objects/data-<start>-pending.dnb1` on first complete frame seen.
3. For each WAL file in order `seq >= start_seq`:
   - Read in fixed-size chunks (e.g., 50 MiB) → append to carry buffer.
   - Compute `safe_len = scan_safe_frame_boundary_with_crc(carry)`.
   - If `safe_len > 0`:
     - Extend sparse index over `carry[..safe_len]`.
     - Write `carry[..safe_len]` to `CloudWriter`; drain those bytes; update counters.
     - If `total_bytes_uploaded >= max_object_mb * 1024 * 1024`: stop.
4. If nothing was uploaded: return `None`.
5. Else: close writer, finalize pending → final key `data-<start>-<end>.dnb1`, write descriptor to ETCD, update checkpoints.

### Scaling and Memory
- **Bounded per-topic memory**: limited to read chunk size + carry buffer + OpenDAL internal buffers (typically ~10–20 MiB).
- **Many topics**: limit concurrent uploaders and/or jitter `interval_seconds` to avoid synchronized spikes.

### Config Example
```yaml
uploader:
  interval_seconds: 1200       # 20 minutes per your example
  max_object_mb: 1024          # 1 GiB bound
```

### Tests
- Unit tests in `cloud/uploader_test.rs` should cover:
  - Aggregation across multiple WAL files in a single tick.
  - Stop at `max_object_mb` boundary and finalize.
  - Correct `(next_seq, next_pos)` and sparse index spanning files.

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

