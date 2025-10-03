# WAL Tail Reader Optimization Plan

## Status
- **Design Proposal** for optimizing memory-intensive tail_reader implementation
- Targets scalability for 100+ concurrent subscriptions per broker (tens of topics, multiple subs each)
- Applies streaming patterns from uploader_stream.rs to reader path

## Goals
- Reduce memory consumption per subscription from O(cache_size + file_size) to O(chunk_buffer)
- Fix rotated file reading to support full historical replay from any offset
- Eliminate redundant cache snapshot cloning across multiple subscriptions
- Enable 100+ concurrent subscriptions without OOM risk
- Maintain frame format and ordering guarantees

---

## Current Problems

### Problem 1: Over-Broad Cache Snapshot (wal.rs:332)
**Location:** `danube-persistent-storage/src/wal.rs`

**Current code:**
```rust
pub async fn tail_reader(&self, from_offset: u64) -> Result<TopicStream, PersistentStorageError> {
    let wal_path_opt = self.inner.wal_path.lock().await.clone();
    let cache_snapshot: Vec<(u64, StreamMessage)> = {
        let cache = self.inner.cache.lock().await;
        cache.range_from(0)  // ❌ Gets ALL cached items from offset 0
            .map(|(off, msg)| (off, msg.clone()))
            .collect()
    };
    // ...
}
```

**Issue:**
- Retrieves and clones ALL cached messages from offset 0, ignoring `from_offset` parameter
- With default cache capacity of 1024 messages, every reader clones up to 1024 messages
- Cache items before `from_offset` are immediately discarded in `build_tail_stream`

**Impact:**
- 10 subscriptions/topic × 10 topics = 100 readers
- Each clones 1024 messages = ~102,400 unnecessary clones per read cycle
- Estimated waste: 100-500 MB memory depending on message size

### Problem 2: Full File Loading (wal/reader.rs:19-65)
**Location:** `danube-persistent-storage/src/wal/reader.rs`

**Current code:**
```rust
pub(crate) async fn read_file_range(
    path: &PathBuf,
    from_offset: u64,
    to_exclusive: u64,
) -> Result<Vec<(u64, StreamMessage)>, PersistentStorageError> {
    let mut f = tokio::fs::File::open(path).await?;
    let mut out = Vec::new();  // ❌ Accumulates entire file range
    // ... loop reads and parses all frames into out ...
    Ok(out)  // Returns entire vector
}
```

**Issue:**
- Loads entire file range `[from_offset, to_exclusive)` into memory as a `Vec`
- For slow/lagging subscriptions reading from offset 0 with gigabytes of historical data
- No chunking, no streaming—everything buffered before returning

**Impact:**
- Slow subscription catching up from 1 GB behind = 1 GB memory per subscription
- 10 slow subscriptions = 10 GB memory usage
- Broker OOM risk with moderate subscription lag

### Problem 3: Missing Rotated File Support (wal/reader.rs:91-96)
**Location:** `danube-persistent-storage/src/wal/reader.rs`

**Current code:**
```rust
async fn build_tail_stream(...) -> Result<TopicStream, PersistentStorageError> {
    // ...
    if let Some(path) = &wal_path_opt {
        if from_offset < cache_start {
            let mut file_part = read_file_range(path, from_offset, cache_start).await?;
            // ❌ Only reads from single active file path
        }
    }
    // ...
}
```

**Issue:**
- Only reads from `wal_path_opt` which points to the current active WAL file
- Rotated files (`wal.1.log`, `wal.2.log`, etc.) are tracked in `WalCheckpoint.rotated_files` but never accessed
- Historical data before current file's start offset is unreachable

**Impact:**
- **Data loss**: Subscriptions starting from old offsets miss data in rotated files
- **Breaks At-Least-Once semantics**: Cannot replay from arbitrary offset
- Violates HLD design (WAL_Cloud_Persistence_Design.md) requirement for historical replay

---

## Target Architecture

### Streaming File Reader (New Module)

Create `danube-persistent-storage/src/wal/streaming_reader.rs` following the pattern from `uploader_stream.rs`.

**Key design principles:**
1. **Chunked disk I/O** - Read files in 4-10 MiB chunks (configurable)
2. **Carry buffer** - Handle incomplete frames across chunk boundaries
3. **Rotated file support** - Read from ordered list of (seq, path) tuples
4. **CRC validation** - Per-frame validation during streaming
5. **Incremental streaming** - Yield messages as parsed, not after reading everything

**Architecture:**
```
┌─────────────────────────────────────────────────────────┐
│  WalStorage::create_reader(from_offset)                 │
└────────────────┬────────────────────────────────────────┘
                 │
                 v
┌─────────────────────────────────────────────────────────┐
│  Wal::tail_reader(from_offset)                          │
│  - Snapshot cache.range_from(from_offset)  ✅ Fixed     │
│  - Get WalCheckpoint for rotated files                  │
└────────────────┬────────────────────────────────────────┘
                 │
                 v
┌─────────────────────────────────────────────────────────┐
│  streaming_reader::build_tail_stream()                  │
│  1. stream_from_wal_files(checkpoint, from_offset)      │
│  2. chain(cache_snapshot)                               │
│  3. chain(broadcast live stream)                        │
└────────────────┬────────────────────────────────────────┘
                 │
                 v
┌─────────────────────────────────────────────────────────┐
│  stream_from_wal_files()                                │
│  - Build ordered file list from checkpoint              │
│  - Find starting file for from_offset                   │
│  - Stream chunks with carry buffer                      │
│  - Yield (offset, StreamMessage) incrementally          │
└─────────────────────────────────────────────────────────┘
```

### Streaming Parse State Machine

**Reuse pattern from uploader_stream.rs:**
```rust
struct StreamingState {
    carry: Vec<u8>,           // Incomplete frame bytes
    chunk_buf: Vec<u8>,       // File read buffer (reusable)
    next_file_idx: usize,     // Current file index
}

async fn stream_from_wal_files(
    files: Vec<(u64, PathBuf)>,
    from_offset: u64,
) -> impl Stream<Item = Result<(u64, StreamMessage)>> {
    // Pseudocode:
    // 1. Skip files entirely before from_offset (using checkpoint metadata)
    // 2. For each relevant file:
    //    - Open file, seek to position if needed
    //    - Read chunks (4-10 MiB)
    //    - Append to carry buffer
    //    - Scan for complete frames (CRC validated)
    //    - Yield frames with offset >= from_offset
    //    - Retain incomplete bytes in carry
    // 3. Continue to next file until all processed
}
```

---

## Implementation Plan

### Phase 1: Fix Cache Snapshot (Immediate - 1 hour)

**File:** `danube-persistent-storage/src/wal.rs`

**Change line 332:**
```rust
// Before:
cache.range_from(0)

// After:
cache.range_from(from_offset)
```

**Benefits:**
- Immediate 10-100× reduction in unnecessary clones
- Zero risk, single-line fix
- Maintains backward compatibility

**Testing:**
- Unit test: verify cache snapshot only contains items >= from_offset
- Integration test: run existing tests to ensure no regression

### Phase 2: Streaming Reader Module (1-2 weeks)

**New file:** `danube-persistent-storage/src/wal/streaming_reader.rs`

**Core function signatures:**
```rust
/// Stream frames from ordered WAL files starting at from_offset.
pub(crate) async fn stream_from_wal_files(
    checkpoint: &WalCheckpoint,
    from_offset: u64,
    chunk_size: usize,
) -> Result<impl Stream<Item = Result<(u64, StreamMessage)>>, PersistentStorageError>;

/// Internal state for streaming parse across chunks.
struct StreamState {
    carry: Vec<u8>,
    chunk_buf: Vec<u8>,
    files: Vec<(u64, PathBuf)>,
    current_file_idx: usize,
    current_file: Option<tokio::fs::File>,
}

/// Parse complete frames from carry buffer, yielding parsed messages.
/// Returns number of bytes consumed.
fn parse_frames_from_carry(
    carry: &[u8],
    from_offset: u64,
) -> Result<(Vec<(u64, StreamMessage)>, usize)>;
```

**Algorithm (stream_from_wal_files):**
1. Build ordered file list: `checkpoint.rotated_files + (checkpoint.file_seq, checkpoint.file_path)`
2. Sort by sequence number
3. Determine starting file (binary search or linear scan based on offset ranges from checkpoint)
4. For each file from start:
   - Open file, seek if needed
   - Loop: read chunk → append to carry → parse frames → yield → drain parsed bytes
   - Move to next file
5. Return stream that yields incrementally

**Frame parsing (reuse CRC validation):**
```rust
use crate::frames::FRAME_HEADER_SIZE;
use crc32fast;

fn parse_frames_from_carry(
    carry: &[u8],
    from_offset: u64,
) -> Result<(Vec<(u64, StreamMessage)>, usize)> {
    let mut idx = 0;
    let mut out = Vec::new();
    
    while idx + FRAME_HEADER_SIZE <= carry.len() {
        let off = u64::from_le_bytes(carry[idx..idx+8].try_into().unwrap());
        let len = u32::from_le_bytes(carry[idx+8..idx+12].try_into().unwrap()) as usize;
        let expected_crc = u32::from_le_bytes(carry[idx+12..idx+16].try_into().unwrap());
        
        let frame_end = idx + FRAME_HEADER_SIZE + len;
        if frame_end > carry.len() {
            break; // Incomplete frame
        }
        
        let payload = &carry[idx+FRAME_HEADER_SIZE..frame_end];
        let actual_crc = crc32fast::hash(payload);
        if actual_crc != expected_crc {
            return Err(PersistentStorageError::Io(
                format!("CRC mismatch at offset {}", off)
            ));
        }
        
        if off >= from_offset {
            let msg: StreamMessage = bincode::deserialize(payload)
                .map_err(|e| PersistentStorageError::Io(format!("deserialize: {}", e)))?;
            out.push((off, msg));
        }
        
        idx = frame_end;
    }
    
    Ok((out, idx))
}
```

**Memory characteristics:**
- Chunk buffer: 4-10 MiB (reusable)
- Carry buffer: < 2× chunk_size worst case (one incomplete frame)
- Total per reader: ~20-30 MiB bounded

### Phase 3: Integrate with build_tail_stream (1 week)

**File:** `danube-persistent-storage/src/wal/reader.rs`

**Refactor build_tail_stream:**
```rust
pub(crate) async fn build_tail_stream(
    checkpoint_opt: Option<WalCheckpoint>,
    cache_snapshot: Vec<(u64, StreamMessage)>,
    from_offset: u64,
    rx: broadcast::Receiver<(u64, StreamMessage)>,
) -> Result<TopicStream, PersistentStorageError> {
    let cache_start = cache_snapshot.first().map(|(o, _)| *o).unwrap_or(u64::MAX);
    
    // Phase 1: Stream from WAL files if needed (NEW)
    let file_stream = if let Some(checkpoint) = checkpoint_opt {
        if from_offset < cache_start {
            Some(streaming_reader::stream_from_wal_files(
                &checkpoint,
                from_offset,
                4 * 1024 * 1024, // 4 MiB chunks
            ).await?)
        } else {
            None
        }
    } else {
        None
    };
    
    // Phase 2: Cache replay
    let cache_stream = tokio_stream::iter(
        cache_snapshot.into_iter()
            .filter(|(off, _)| *off >= from_offset)
            .map(|(off, mut msg)| {
                msg.msg_id.segment_offset = off;
                Ok(msg)
            })
    );
    
    // Phase 3: Live tail from broadcast
    let start_from_live = // ... (existing logic)
    let live_stream = BroadcastStream::new(rx).filter_map(/* ... */);
    
    // Chain all three phases
    let combined = match file_stream {
        Some(fs) => Box::pin(fs.chain(cache_stream).chain(live_stream)) as TopicStream,
        None => Box::pin(cache_stream.chain(live_stream)) as TopicStream,
    };
    
    Ok(combined)
}
```

**Key changes:**
- Accept `Option<WalCheckpoint>` instead of `Option<PathBuf>` to access rotated files
- Use streaming_reader for file phase
- Chain three phases: files → cache → live

### Phase 4: Update Wal::tail_reader (1 day)

**File:** `danube-persistent-storage/src/wal.rs`

**Changes:**
```rust
pub async fn tail_reader(
    &self,
    from_offset: u64,
) -> Result<TopicStream, PersistentStorageError> {
    // 1. Get checkpoint for rotated files (NEW, async, no blocking)
    let checkpoint_opt = self.current_wal_checkpoint().await;
    
    // 2. Snapshot cache from from_offset (FIXED)
    let cache_snapshot: Vec<(u64, StreamMessage)> = {
        let cache = self.inner.cache.lock().await;
        cache.range_from(from_offset)  // ✅ Fixed
            .map(|(off, msg)| (off, msg.clone()))
            .collect()
    };
    
    // 3. Get broadcast receiver
    let rx = self.inner.tx.subscribe();
    
    // 4. Build streaming tail (UPDATED)
    reader::build_tail_stream(checkpoint_opt, cache_snapshot, from_offset, rx).await
}
```

Note: Avoid `block_in_place` or any synchronous blocking in async contexts to prevent runtime thread starvation; use the existing async helper `current_wal_checkpoint().await` for scalability.

### Phase 5: Testing and Validation (1 week)

**Unit tests:**
1. `test_streaming_reader_single_file` - Stream from one file, verify all frames
2. `test_streaming_reader_rotated_files` - Multiple rotated files, verify continuity
3. `test_streaming_reader_partial_frame` - Incomplete frame at chunk boundary
4. `test_streaming_reader_crc_mismatch` - Corrupted frame detection
5. `test_cache_snapshot_filtering` - Verify range_from(from_offset) correctness

**Integration tests:**
1. `test_slow_subscription_replay` - Subscription 1 GB behind, verify memory bounded
2. `test_multiple_subscriptions_different_offsets` - 10 subs at different offsets
3. `test_rotated_file_catchup` - Start from offset in old rotated file
4. `test_live_tail_after_catchup` - Verify seamless transition to live stream

**Performance benchmarks:**
```rust
#[tokio::test]
async fn bench_memory_usage_100_subscriptions() {
    // Create 100 subscriptions at random offsets
    // Measure peak memory usage
    // Assert < 5 GB (vs current ~50-100 GB)
}
```

---

## Configuration

**New knobs in `WalConfig`:**
```rust
pub struct WalConfig {
    // ... existing fields ...
    
    /// Chunk size for streaming file reads (bytes).
    /// Default: 4 MiB
    pub stream_chunk_size: Option<usize>,
}
```

**Broker config (danube_broker.yml):**
```yaml
storage:
  wal:
    stream_chunk_size: 4194304  # 4 MiB
```

---

## Performance Expectations

### Memory Usage (per subscription)

| Scenario | Before | After | Improvement |
|----------|--------|-------|-------------|
| Active (cache hit) | ~10 MB | ~1 MB | 10× |
| Lagging 1 GB | ~1 GB | ~20 MB | 50× |
| 100 subscriptions | 50-100 GB | 1-2 GB | 50-100× |

### Latency

| Operation | Before | After | Change |
|-----------|--------|-------|--------|
| Cache hit read | <1 ms | <1 ms | No change |
| File replay (1 GB) | 500-1000 ms | 600-1200 ms | +20% acceptable |
| Rotated file access | ❌ Broken | ✅ Works | Fixed |

### Scalability Limits

**Before:**
- Max subscriptions per broker: ~20-30 (OOM risk)
- Max lag tolerance: ~100 MB per subscription

**After:**
- Max subscriptions per broker: 100-200 (CPU bound)
- Max lag tolerance: Multiple GB per subscription

---

## Additional Considerations

### Rotated Files Enumeration via WalCheckpoint

- Use `WalCheckpoint` to enumerate local WAL files precisely, not just the active `wal_path`.
- Build the ordered list as `rotated_files + (file_seq, file_path)` and sort by `file_seq` (same as `uploader_stream.rs::build_ordered_wal_files`).
- Readers should open one file at a time and close it when advancing to the next to minimize open FDs and memory usage.

### Backpressure and Cancellation

- The streaming reader must yield incrementally using `tokio_stream` so slow consumers naturally apply backpressure to disk reads.
- If the consumer drops the stream, the reader stops file I/O quickly (cooperative cancellation by breaking the read loop when the stream is no longer polled).
- Keep per-reader memory bounded by a reusable chunk buffer (4–10 MiB) plus a carry buffer for partial frames.

### File Descriptor Limits and Fairness

- Ensure at most one file is open per reader; close before moving to the next file to avoid exhausting OS FD limits when many readers exist.
- Let Tokio’s scheduler and the streaming interface provide fairness across readers; avoid long blocking operations within the parsing loop.

### Start Position Semantics (Latest vs Offset)

- `StartPosition::Latest` should be used for true tailing consumers; it maps to `current_offset().saturating_sub(1)` (next appended message will be the first delivered).
- Reliable subscriptions that must read all available data should prefer `StartPosition::Offset(0)` or an explicit earliest offset.
- Tests and examples should be verified not to use `Latest` when historical replay is required.

### Optional Enhancement: Local Sparse Index per WAL File

- Future optimization: write a lightweight sidecar index per WAL file (e.g., every N messages record `(offset -> byte_pos)`).
- This can accelerate seeking into very large local WALs for `from_offset` without scanning the entire file.
- This is not required for correctness and can be added incrementally after the streaming reader lands.

## Migration and Compatibility

**Backward compatibility:**
- No breaking changes to APIs or data formats
- Existing WAL files readable without migration
- Gradual rollout supported

**Rollout strategy:**
1. Deploy Phase 1 fix (cache snapshot) - immediate win
2. Deploy streaming reader in canary brokers
3. Monitor memory metrics and latency
4. Full rollout once validated

---

## Next Steps

1. Implement config knob for streaming chunk size
   - Add `stream_chunk_size: Option<usize>` to `WalConfig`
   - Thread into `streaming_reader::stream_from_wal_files`

2. Tests
   - Unit: single-file streaming, rotated files, partial frame at boundary, CRC mismatch
   - Integration: lagging subscription (GB-scale) memory bounded; mixed offsets across many readers; file→cache→live handoff continuity

3. Observability
   - Add gauges/counters/histograms proposed in Metrics and Observability
   - Log handoff decisions (Files→Cache→Live vs Cache→Live)

4. Optional enhancement (follow-up)
   - Local sparse index sidecar per WAL file to accelerate seeks for very large files

5. Broker tuning guidance
   - Document recommended `stream_chunk_size` and expected per-reader memory bounds
   - Document consumer StartPosition guidance (use `Offset(0)` for full catch-up; `Latest` for pure tail)

---

## Test Plan

### Unit tests (src/wal/reader_test.rs)

- Streaming reader single file
  - Write frames 0..N in one file; `stream_from_wal_files(ckpt, 0)` yields all in order.
- Streaming reader rotated files
  - Create 2+ WAL files via rotation; checkpoint lists rotated files + active; stream from offset in first file; verify continuity across file boundaries.
- Partial frame at chunk boundary
  - Craft file so a frame spans chunk boundary; ensure carry buffer handles it; all messages delivered.
- CRC mismatch handling
  - Corrupt a frame payload CRC; verify reader stops before corrupt frame without panicking.
- Tail builder cache-only and file→cache→live
  - `build_tail_stream(None, cache>=from, from)` yields cache→live only.
  - `build_tail_stream(Some(ckpt), cache, from<cache_start)` yields files→cache→live.

Timeouts:
- Wrap awaits for next items with `tokio::time::timeout(Duration::from_secs(5), ...)` where a stream could otherwise hang, and assert on timeout to fail fast.

### Integration tests (tests/)

- Local disk handoff focused (chaining_stream_handoff.rs)
  - Focus on Files→Cache→Live with local disk only; multiple scenarios:
    - Start far behind (spanning multiple files), catch up to cache, then live.
    - Start inside cache window, no file phase, cache→live only.
  - Use `.take(N)` or `timeout(...)` to avoid indefinite tailing.

- Cloud→WAL handoff (factory_cloud_wal_handoff.rs)
  - Ensure reader chains Cloud [historical objects] → WAL tail correctly.
  - Add variant starting after cloud object end to skip cloud phase.
  - Use timeouts for each awaited item to avoid hangs.

Metrics/log assertions (optional):
- Verify presence of `wal_reader` decision logs (Files→Cache→Live vs Cache→Live) in test logs when enabled.

## References

- **Streaming pattern:** `danube-persistent-storage/src/uploader_stream.rs`
- **Frame format:** `danube-persistent-storage/src/frames.rs`
- **HLD:** `info/WAL_Cloud_Persistence_Design.md` (sections on WAL reader and retention)
- **Checkpoint structure:** `danube-persistent-storage/src/checkpoint.rs`

---

## Metrics and Observability

**New metrics:**
```rust
// Memory usage per reader
wal_reader.memory_bytes{type="file_stream|cache|live"}: gauge

// Streaming performance
wal_reader.chunk_read_latency_ms: histogram
wal_reader.frames_parsed_total: counter
wal_reader.crc_errors_total: counter

// File access patterns
wal_reader.rotated_files_accessed: counter
wal_reader.active_readers: gauge
```

**Logging:**
```rust
tracing::info!(
    target = "wal_reader",
    from_offset = from_offset,
    files_count = files.len(),
    chunk_size = chunk_size,
    "starting streaming WAL reader"
);
```
