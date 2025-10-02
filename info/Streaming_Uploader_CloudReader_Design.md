# Streaming Uploader and CloudReader Design

## Status
- Implemented in codebase (uploader + cloud reader) with OpenDAL streaming APIs
- Eliminates memory bloat by streaming data instead of buffering entire files/objects in memory

### Implemented Changes (Summary)
- Uploader streams WAL frames and writes a pending object, then finalizes to `data-<start>-<end>.dnb1` via server-side copy with read+write fallback; pending key is deleted. See `danube-persistent-storage/src/uploader.rs` and `src/cloud_store.rs` (`copy_object`, `delete_object`).
- Uploader builds a sparse offset index every 1000 messages (`INDEX_EVERY_MSGS`) and persists it in `ObjectDescriptor.offset_index`.
- CloudReader performs descriptor discovery using `get_object_descriptors()` and filters overlapping ranges; it uses the sparse index (when present) to seek to a starting byte within the object.
- `CloudRangeReader` uses `stat()` to determine content length and bounds range reads, preventing out-of-bounds on small objects.
  
Note: The uploader now only streams complete frames. If no complete frame is available in a cycle, it performs no upload in that cycle. The previous buffered fallback (`read_frames_by_position` + `commit_upload`) has been removed to keep behavior simple and aligned with mid/long-term cloud storage semantics.

## Goals
- Replace buffer-based upload with streaming Writer API for constant memory usage
- Replace bulk object reads with ranged Reader API to fetch only needed offsets
- Maintain frame format compatibility (no breaking changes to on-disk/cloud data)
- Improve memory efficiency for large objects (multi-GB WAL files)
- Support batch/chunked processing for CloudReader to avoid OOM errors

---

## Current Problems

### Problem 1: Uploader Memory Bloat
**Location:** `danube-persistent-storage/src/uploader.rs`

**Current flow:**
```rust
// read_frames_by_position() reads entire file content between checkpoints
let (bytes, start_offset, end_offset, next_seq, next_pos) = 
    self.read_frames_by_position(&wal_ckpt, start_seq, start_pos).await?;
// bytes is a Vec<u8> containing ALL frames

// commit_upload() writes entire buffer at once
self.cloud.put_object_meta(&object_path, &bytes).await?;
```

**Issues:**
- For a topic with 1 GB of accumulated WAL data, `bytes` buffer consumes 1 GB of memory
- With 100 topics, this scales to 100 GB memory usage during upload cycles
- No backpressure or chunk control; everything loaded upfront

### Problem 2: CloudReader Memory Exhaustion
**Location:** `danube-persistent-storage/src/cloud_reader.rs`

**Current flow:**
```rust
// read_range() fetches entire object into memory
let bytes = self.cloud.get_object(&key).await?;
// bytes contains entire multi-GB object

let mut msgs = parse_raw_frames(&bytes)?;
// Filter offsets we actually need
msgs.retain(|(off, _)| *off >= start && end_inclusive.map(|e| *off <= e).unwrap_or(true));
```

**Issues:**
- Fetches entire object even if consumer only needs offsets 1000-2000 (first 1% of file)
- For a 500 MB object, loads 500 MB into memory to extract maybe 5 MB of needed frames
- No support for resuming from partial reads if memory pressure occurs
- Multiple slow consumers reading from cloud can exhaust broker memory

---

## OpenDAL Streaming APIs (as used)

### Reader API
```rust
// Create whole-object reader, but our wrapper issues ranged reads with bounds
let reader = op.reader(&key).await?;

// Read bounded ranges via Reader::read(start..end)
let chunk = reader.read(start..end).await?;
```

### Writer API
```rust
// Create streaming writer with chunk/concurrency control
let mut writer = op.writer_with("path")
    .chunk(8 * 1024 * 1024)      // 8 MiB chunk size
    .concurrent(4)                // 4 concurrent parts
    .await?;

// Stream data incrementally
writer.write(chunk1).await?;
writer.write(chunk2).await?;
writer.close().await?;  // Completes multipart upload
```

---

## Target Architecture

### Streaming Uploader

**New flow (implemented):**
```rust
// 1) Stream frames from WAL with carry buffer and frame-boundary alignment
let writer = cloud.open_streaming_writer(path, 8*1024*1024, 4).await?;
//    - Maintain `carry` Vec<u8>
//    - Compute `safe_len` = largest prefix with whole frames
//    - On first complete frame, record `first_offset`, create pending object name
//    - Update `last_offset` as we extend; write only frame-aligned prefixes
//    - Build sparse index entries every 1000 msgs: (offset, object_byte_pos)

// 2) Close writer to finalize multipart upload
let meta = writer.close().await?;

// 3) Finalize: copy pending -> final name (data-<start>-<end>.dnb1) and delete pending
cloud.copy_object(pending_key, final_key).await.or_else(|_| async {
    // Fallback for backends without server-side copy
    let bytes = cloud.get_object(pending_key).await?;
    cloud.put_object_meta(final_key, &bytes).await?;
    Ok(())
}).await?;
let _ = cloud.delete_object(pending_key).await;

// 4) Write ETCD descriptor with offset_index (if non-empty) and update checkpoints
```

**Benefits:**
- Constant memory: only `chunk_size` (8 MiB) buffer needed
- Progressive upload: chunks upload in background while reading continues
- Backpressure: writer automatically throttles if network is slow

### Streaming CloudReader

**New flow (implemented):**
```rust
// 1) Query ETCD for all object descriptors and filter by overlap with [start, end]
let mut descs = etcd.get_object_descriptors(topic_path).await?;
descs.retain(|d| d.end_offset >= start && end.map(|e| d.start_offset <= e).unwrap_or(true));
descs.sort_by_key(|d| d.start_offset);

// 2) For each object, compute a starting byte using sparse index if present
let start_byte = match &desc.offset_index {
    Some(index) if !index.is_empty() => binary_search_for_byte(index, start),
    _ => 0,
};

// 3) Open CloudRangeReader at start_byte and read bounded chunks using stat() size
let mut reader = cloud.open_ranged_reader(&object_key, start_byte).await?;
loop {
    let chunk = reader.read_chunk(4 * 1024 * 1024).await?; // 4 MiB
    if chunk.is_empty() { break; }
    parse_frames_from_carry(&mut carry, &mut parsed)?; // safe at chunk boundaries
    // Filter on offsets and emit messages lazily
}
```

**Benefits:**
- Ranged reads: fetch only needed byte ranges from cloud
- Streaming parse: process frames incrementally without full object buffer
- Memory bounded: at most `chunk_size` + parsing overhead in memory

---

## Stateful Streaming Parsing (Uploader and CloudReader)

To handle frames that can be split across chunk boundaries, both the uploader and cloud reader must implement a small state machine that carries leftover bytes between chunk reads.

### Frame Layout Recap
- `[u64 offset][u32 len][u32 crc][len bytes payload]` (little-endian)
- Header size is fixed at 16 bytes.

### Parser State (shared concept)
```rust
struct ParserState {
    // Accumulates incomplete header/payload bytes across chunk boundaries
    carry: Vec<u8>,
    // When reading a payload, track how many bytes we still need
    need_payload: Option<usize>,
    // Offset of current frame (valid once header complete)
    cur_off: Option<u64>,
}

impl ParserState {
    fn new() -> Self { Self { carry: Vec::new(), need_payload: None, cur_off: None } }
}
```

### Parsing Algorithm (per chunk)
1. Append incoming `chunk` to `carry` (without copying if possible; can use a `BytesMut`).
2. While true:
   - If `need_payload` is `None`:
     - If `carry.len() < 16`, break (need more header bytes).
     - Parse header: `offset`, `len`, `crc`; set `cur_off = Some(offset)`, `need_payload = Some(len)`.
   - Else (need payload):
     - If `carry.len() < 16 + len`, break (partial payload).
     - Validate CRC over payload; if mismatch, stop parsing (treat as end-of-log in uploader; error in reader unless configured to be lenient).
     - Extract payload and emit message `(offset, payload)`; advance by `16 + len` and reset `need_payload=None`, `cur_off=None`.
3. Retain any remaining (incomplete) bytes in `carry` for the next chunk.

### Uploader-specific Notes
- The uploader only needs `first_offset` and `last_offset` for the batch plus a guarantee to not cut a frame: stop parsing at first incomplete header/payload and stop uploading further bytes from that file in this cycle.
- Checkpointing uses the file stream position after the last fully parsed frame, ensuring resumability without duplication.

### CloudReader-specific Notes
- The reader yields fully parsed messages immediately as they are decoded.
- If starting mid-object with a sparse index, the first chunk may land mid-frame; the carry-buffer parser safely advances to the next full header.
- On the final chunk, an incomplete frame tail is ignored (no error).

### Correctness Invariants
- Never emit a message unless header and full payload are available and CRC matches.
- Never advance checkpoints beyond the last fully parsed frame.
- CRC mismatch is treated as end-of-log for uploader (defensive) and as an error (or soft-stop) for reader depending on policy.

### Complexity and Memory
- Time: O(total bytes), single pass.
- Memory: O(header + max payload in carry), practically bounded by a small multiple of header size because payload is emitted/forwarded immediately.

## Implementation Notes

### Uploader finalize and metadata
- Pending name pattern: `data-<start>-pending.dnb1`
- Final name pattern: `data-<start>-<end>.dnb1`
- Finalize via `CloudStore::copy_object()` and delete pending; fallback to read+write if server-side copy is unavailable.
- `ObjectDescriptor` includes `offset_index: Option<Vec<(u64, u64)>>`.

### CloudReader discovery and bounded reads
- Descriptors fetched via `get_object_descriptors()` and filtered for overlap.
- Sparse index binary-searched to find `byte_pos` ≤ start; begin reading there.
- `CloudRangeReader` calls `stat()` to bound `read(start..end)` calls to content length.

### Legacy fallback path
Removed. The uploader does not buffer frames for cloud upload; it only streams complete frames and skips the cycle otherwise.

---

## Implementation Plan (original)

### Phase 1: Streaming Uploader (Week 1)

**Changes to `cloud_store.rs`:**
```rust
impl CloudStore {
    /// Create a streaming writer to cloud storage.
    pub async fn writer(&self, path: &str, chunk_size: usize) -> Result<CloudWriter> {
        let key = self.join(path);
        let writer = self.op.writer_with(&key)
            .chunk(chunk_size)
            .concurrent(4)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("cloud writer: {}", e)))?;
        
        Ok(CloudWriter {
            inner: writer,
            bytes_written: 0,
        })
    }
}

pub struct CloudWriter {
    inner: opendal::Writer,
    bytes_written: u64,
}

impl CloudWriter {
    pub async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.inner.write(opendal::Buffer::from(buf.to_vec())).await?;
        self.bytes_written += buf.len() as u64;
        Ok(())
    }
    
    pub async fn close(self) -> Result<opendal::Metadata> {
        self.inner.close().await.map_err(|e| PersistentStorageError::Other(e.to_string()))
    }
}
```

**Changes to `uploader.rs`:**
```rust
async fn stream_frames_to_cloud(
    &self,
    wal_ckpt: &WalCheckpoint,
    start_seq: u64,
    start_pos: u64,
    object_path: &str,
) -> Result<(u64, u64, u64, u64), PersistentStorageError> {
    // 1. Create streaming writer
    let mut writer = self.cloud.writer(object_path, 8 * 1024 * 1024).await?;
    
    // 2. Iterate WAL files and stream frames
    let mut files = wal_ckpt.rotated_files.clone();
    files.push((wal_ckpt.file_seq, PathBuf::from(&wal_ckpt.file_path)));
    files.sort_by(|a, b| a.0.cmp(&b.0));
    
    let mut first_offset = 0u64;
    let mut last_offset = 0u64;
    let mut started = false;
    let mut next_seq = start_seq;
    let mut next_pos = start_pos;
    
    let chunk_size = 1024 * 1024; // 1 MiB read buffer
    let mut chunk_buf = vec![0u8; chunk_size];
    
    for (seq, path) in files.into_iter().filter(|(s, _)| *s >= start_seq) {
        if path.as_os_str().is_empty() { continue; }
        
        let mut file = tokio::fs::File::open(&path).await?;
        
        // Seek to start position for first file
        if seq == start_seq && start_pos > 0 {
            file.seek(SeekFrom::Start(start_pos)).await?;
        }
        
        // Stream file content to cloud in chunks
        loop {
            let n = file.read(&mut chunk_buf).await?;
            if n == 0 { break; }
            
            // Write chunk to cloud
            writer.write(&chunk_buf[..n]).await?;
            
            // Track first/last offsets by parsing frame headers
            let (first, last) = extract_offset_range(&chunk_buf[..n])?;
            if !started && first > 0 {
                first_offset = first;
                started = true;
            }
            if last > 0 {
                last_offset = last;
            }
        }
        
        // Track position for checkpoint
        next_seq = seq;
        next_pos = file.stream_position().await?;
        
        // Process only one file per cycle for bounded latency
        if started { break; }
    }
    
    // 3. Finalize upload
    writer.close().await?;
    
    Ok((first_offset, last_offset, next_seq, next_pos))
}

/// Extract first and last offsets from a chunk by scanning frame headers.
fn extract_offset_range(chunk: &[u8]) -> Result<(u64, u64)> {
    let mut idx = 0;
    let mut first = 0u64;
    let mut last = 0u64;
    
    while idx + 16 <= chunk.len() {
        let off = u64::from_le_bytes(chunk[idx..idx+8].try_into().unwrap());
        let len = u32::from_le_bytes(chunk[idx+8..idx+12].try_into().unwrap()) as usize;
        
        if first == 0 { first = off; }
        last = off;
        
        idx += 16 + len;
        if idx > chunk.len() { break; }
    }
    
    Ok((first, last))
}
```

**Testing:**
- Unit test: stream 100 MB of frames from WAL to memory backend
- Integration test: verify object metadata and content correctness
- Benchmark: measure memory usage with 1 GB upload (should remain constant)

### Phase 2: Streaming CloudReader (Week 2)

**Changes to `cloud_store.rs`:**
```rust
impl CloudStore {
    /// Create a ranged reader from cloud storage.
    pub async fn reader_range(
        &self,
        path: &str,
        byte_range: Option<(u64, u64)>,
    ) -> Result<CloudReader> {
        let key = self.join(path);
        
        let reader = if let Some((start, end)) = byte_range {
            self.op.reader_with(&key)
                .range(start..end)
                .await
                .map_err(|e| PersistentStorageError::Other(format!("cloud reader: {}", e)))?
        } else {
            self.op.reader(&key).await
                .map_err(|e| PersistentStorageError::Other(format!("cloud reader: {}", e)))?
        };
        
        Ok(CloudReader {
            inner: reader,
            offset: 0,
        })
    }
}

pub struct CloudReader {
    inner: opendal::Reader,
    offset: usize,
}

impl CloudReader {
    pub async fn read_chunk(&mut self, chunk_size: usize) -> Result<Vec<u8>> {
        let end = self.offset + chunk_size;
        let buf = self.inner.read(self.offset..end).await
            .map_err(|e| PersistentStorageError::Other(e.to_string()))?;
        self.offset += buf.len();
        Ok(buf.to_vec())
    }
}
```

**Changes to `cloud_reader.rs`:**
```rust
impl CloudReader {
    /// Read messages in range [start, end_inclusive] using streaming and ranged reads.
    pub async fn read_range_streaming(
        &self,
        start: u64,
        end_inclusive: Option<u64>,
    ) -> Result<TopicStream, PersistentStorageError> {
        let from_padded = format!("{:020}", start);
        let descriptors = self.etcd
            .get_object_descriptors_range(self.topic_path(), &from_padded, None)
            .await?;
        
        // Filter descriptors overlapping our range
        let filtered: Vec<ObjectDescriptor> = descriptors
            .into_iter()
            .filter(|d| d.end_offset >= start)
            .filter(|d| match end_inclusive {
                Some(end) => d.start_offset <= end,
                None => true,
            })
            .collect();
        
        // Create async stream that lazily fetches and parses objects
        let cloud = self.cloud.clone();
        let topic_path = self.topic_path.clone();
        let chunk_size = 4 * 1024 * 1024; // 4 MiB chunks
        
        let stream = futures::stream::iter(filtered)
            .then(move |desc| {
                let cloud = cloud.clone();
                let topic_path = topic_path.clone();
                async move {
                    // Fetch object in chunks and parse incrementally
                    let key = format!("storage/topics/{}/objects/{}", topic_path, desc.object_id);
                    
                    let mut reader = cloud.reader_range(&key, None).await?;
                    let mut all_frames = Vec::new();
                    
                    loop {
                        let chunk = reader.read_chunk(chunk_size).await?;
                        if chunk.is_empty() { break; }
                        
                        // Parse frames from chunk
                        let mut frames = parse_raw_frames_partial(&chunk)?;
                        all_frames.append(&mut frames);
                    }
                    
                    Ok::<_, PersistentStorageError>(all_frames)
                }
            })
            .try_flatten()
            .try_filter(move |(off, _)| {
                futures::future::ready(
                    *off >= start && end_inclusive.map(|e| *off <= e).unwrap_or(true)
                )
            })
            .map_ok(|(_, msg)| msg);
        
        Ok(Box::pin(stream))
    }
}

/// Parse raw frames from a partial chunk. Handles incomplete frames at end gracefully.
fn parse_raw_frames_partial(bytes: &[u8]) -> Result<Vec<(u64, StreamMessage)>, PersistentStorageError> {
    // Same as parse_raw_frames but returns partial results
    let mut idx = 0usize;
    let mut out = Vec::new();
    
    while idx + 16 <= bytes.len() {
        let off = u64::from_le_bytes(bytes[idx..idx+8].try_into().unwrap());
        idx += 8;
        let len = u32::from_le_bytes(bytes[idx..idx+4].try_into().unwrap()) as usize;
        idx += 4;
        let _crc = u32::from_le_bytes(bytes[idx..idx+4].try_into().unwrap());
        idx += 4;
        
        if idx + len > bytes.len() {
            // Incomplete frame at end; stop without error
            break;
        }
        
        let rec = &bytes[idx..idx+len];
        idx += len;
        
        let msg: StreamMessage = bincode::deserialize(rec)
            .map_err(|e| PersistentStorageError::Other(format!("bincode: {}", e)))?;
        out.push((off, msg));
    }
    
    Ok(out)
}
```

**Advanced optimization (Phase 2b): Sparse Byte-Offset Index**

Implement a sparse index recorded in `ObjectDescriptor` to enable precise ranged reads without scanning from the start of the object.

```rust
pub struct ObjectDescriptor {
    // ... existing fields ...

    /// Optional sparse index mapping message offsets to byte positions within the object.
    /// Example (by message count): every K messages => (offset_i, byte_pos_i)
    /// Example (by bytes): every ~B bytes => nearest (offset, byte_pos)
    pub offset_index: Option<Vec<(u64, u64)>>,
}
```

#### Index Construction (in Uploader)
- While streaming frames, track object-relative `byte_pos` as chunks are written.
- Append an index entry according to a policy:
  - By message interval: every `K` messages (e.g., K=1_000)
  - By byte interval: approximately every `B` bytes (e.g., B=4 MiB)
- Persist the index inside the object descriptor (ETCD) or a sidecar key.

#### Read-time Usage (in CloudReader)
1. Binary search the sparse index for the greatest entry with `offset_i <= start`.
2. Start a ranged read at `byte_pos_i`.
3. Parse forward until reaching `start` (skipping frames before `start`), then stream messages.

#### Scalability Discussion
- With thousands of messages per object, a sparse index remains tiny:
  - Example: 10,000 msgs/object, index every 1,000 msgs => 10 entries.
  - Each entry is 16 bytes (u64,u64) + overhead; 10 entries ≈ 160–256 bytes total.
  - Even with 1,000,000 msgs/object, index every 10,000 msgs => 100 entries (~1.6 KiB).
- Benefits scale well: faster seeks, less egress, minimal metadata size.
- Choose `K`/`B` as configuration knobs to balance precision vs metadata size.

#### Backward Compatibility
- If `offset_index` is absent, fall back to start-of-object scan (current behavior) with streaming parser.

**Testing:**
- Unit test: read only offsets 1000-2000 from 10k-message object, verify memory bounded
- Integration test: verify message correctness with Cloud→WAL chaining
- Benchmark: measure memory with 1 GB object (should use only chunk_size)

### Phase 3: Configuration and Observability (Week 3)

**Configuration knobs:**
```yaml
storage:
  uploader:
    stream_chunk_size: 8388608  # 8 MiB (chunk size for streaming uploads)
    concurrent_parts: 4         # concurrent multipart upload parts
  
  cloud_reader:
    fetch_chunk_size: 4194304   # 4 MiB (chunk size for streaming downloads)
    enable_ranged_reads: true   # use byte-range requests when possible
```

**Metrics:**
- `uploader.stream_chunk_bytes`: histogram of chunk sizes written
- `uploader.memory_usage_bytes`: gauge of current buffer size
- `cloud_reader.range_requests_total`: counter of ranged reads vs full reads
- `cloud_reader.bytes_fetched` vs `cloud_reader.bytes_used`: efficiency ratio

**Logging:**
```rust
tracing::info!(
    target = "uploader",
    object_id = %object_id,
    bytes_written = writer.bytes_written,
    chunks = num_chunks,
    duration_ms = elapsed.as_millis(),
    "streamed WAL frames to cloud"
);
```

---

## Migration and Compatibility

**No breaking changes:**
- Frame format unchanged (still `[u64 offset][u32 len][u32 crc][bytes]`)
- Object naming unchanged
- ETCD metadata unchanged
- Existing objects readable by new CloudReader

**Rollout strategy:**
1. Deploy streaming uploader
2. Monitor metrics and logs in staging
3. Gradual rollout to all topics

---

## Performance Expectations

### Memory Usage

| Component | Before | After | Improvement |
|-----------|--------|-------|-------------|
| Uploader (1 GB file) | 1 GB | 8 MiB | **125x reduction** |
| CloudReader (500 MB object) | 500 MB | 4 MiB | **125x reduction** |
| 100 topics uploading | 100 GB | 800 MiB | **128x reduction** |

### Latency
- **Uploader:** Slight increase due to streaming overhead (~5-10%), acceptable for background task
- **CloudReader:** Ranged reads reduce latency for partial fetches (50-90% faster for small ranges)

### Cost
- **Uploader:** No change (same total bytes uploaded)
- **CloudReader:** Reduced egress costs by fetching only needed byte ranges (10-50% savings)

---

## References

- OpenDAL Writer API: https://docs.rs/opendal/latest/opendal/struct.Operator.html#method.writer_with
- OpenDAL Reader API: https://docs.rs/opendal/latest/opendal/struct.Operator.html#method.reader_with
- S3 Multipart Upload: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- S3 Range GET: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
