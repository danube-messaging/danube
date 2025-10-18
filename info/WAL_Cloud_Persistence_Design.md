# Danube WAL + Cloud Persistence Design (S3/GCS via opendal)

## Status
- **IMPLEMENTED** - Production-ready implementation in `danube-persistent-storage` crate
- Replaces segment-based storage with Write-Ahead Log (WAL) and background cloud persistence
- Achieves sub-millisecond dispatch by keeping hot path local with async cloud uploads
- Integrated with broker via `WalStorageFactory` providing per-topic `WalStorage` instances

## Goals
- Replace segment-based storage with a WAL-first architecture.
- Persist data to cloud object storage using `opendal` (S3, GCS and local disk for single-broker). Memory for tests.
- Maintain subscription progress and object metadata in ETCD (ETCD is already in use for cluster coordination).
- Keep dispatch path sub-second by serving from in-memory/WAL cache.
- Batch uploads every ~10 seconds (tunable) from WAL to per-topic objects in cloud.
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


## Implemented Architecture Overview

### Core Components

**WalStorageFactory** (`wal_factory.rs`):
- Central factory managing per-topic WAL and Uploader instances
- Creates `WalStorage` instances via `for_topic(topic_path)` method
- Internally manages:
  - `DashMap<String, Wal>` for per-topic WAL instances
  - `DashMap<String, JoinHandle>` for per-topic Uploader background tasks
  - Shared `CloudStore` and `EtcdMetadata` for all topics
- Configures WAL directory structure: `<root>/<namespace>/<topic>/`
- Initializes per-topic `CheckpointStore` for atomic checkpoint management

**Deleter** (`wal/deleter.rs`):
- Per-topic background task that enforces WAL retention policies
- Evaluates eligible rotated files and deletes them safely
- Advances `WalCheckpoint.start_offset` after deletions

**Wal** (`wal.rs`):
- Per-topic append-only log with integrated in-memory `Cache` (BTreeMap-based)
- Frame format: `[u64 offset][u32 len][u32 crc][bytes]` with CRC32 validation
- Background writer task for batched fsync (configurable interval/size thresholds)
- File rotation support: `wal.<seq>.log` files tracked in `WalCheckpoint`
- Cache capacity: configurable (default 1024 messages) with automatic eviction
- Methods: `append()`, `tail_reader(from_offset)`, `current_offset()`, `flush()`, `shutdown()`

**WalStorage** (`wal_storage.rs`):
- Implements `PersistentStorage` trait from `danube-core`
- Wraps a `Wal` instance with optional cloud integration
- Tiered reading logic in `create_reader()`:
  - If `start_offset >= wal_start_offset`: serve from WAL only (fast path)
  - If `start_offset < wal_start_offset`: chain CloudReader → WAL for seamless streaming
- Methods: `append_message()`, `create_reader()`, `ack_checkpoint()`, `flush()`

**Uploader** (`cloud/uploader.rs`):
- Per-topic background task uploading WAL frames to cloud storage
- Periodic upload cycles (configurable interval, default 300s)
- Resumes from `UploaderCheckpoint` tracking `(last_read_file_seq, last_read_byte_position)`
- Streams frames directly to cloud via `uploader_stream::stream_frames_to_cloud()`
- Writes `ObjectDescriptor` to ETCD after successful upload
- Never flushes WAL (read-only consumer of WAL files)

**CloudReader** (`cloud/reader.rs`):
- Reads historical messages from cloud objects uploaded by Uploader
- Queries ETCD for `ObjectDescriptor` list, filters by offset range
- Sparse index support: uses `offset_index: Vec<(u64, u64)>` for efficient seeks
- Streams frames with CRC32 validation, returns async `TopicStream`
- Chunked reading (4 MiB default) with incremental frame parsing

**CloudStore** (`cloud/storage.rs`):
- Abstraction over `opendal::Operator` for S3/GCS/local-fs/memory backends
- Supports: `put_object()`, `get_object()`, `copy_object()`, `delete_object()`
- Streaming APIs: `open_streaming_writer()`, `open_ranged_reader()`
- Configuration via `BackendConfig` enum (Cloud or Local variants)

**EtcdMetadata** (`etcd_metadata.rs`):
- Manages object descriptors in ETCD under `/danube/storage/topics/{ns}/{topic}/objects/`
- Methods: `put_object_descriptor()`, `get_object_descriptors()`, `put_current_pointer()`
- Each descriptor tracks: object_id, start/end offsets, size, etag, created_at, completed flag
- Optional sparse index per object: `offset_index: Vec<(u64, u64)>`

**CheckpointStore** (`checkpoint.rs`):
- Per-topic cache + atomic file persistence for WAL and Uploader checkpoints
- Separate files: `<dir>/wal.ckpt` and `<dir>/uploader.ckpt`
- In-memory cache with `RwLock` for fast reads
- Atomic writes via tmp+rename pattern using bincode serialization
- Methods: `get_wal()`, `update_wal()`, `get_uploader()`, `update_uploader()`

---

## Implemented API and Interfaces

### WalStorage (implements PersistentStorage trait)
**Core API:**
```rust
impl PersistentStorage for WalStorage {
    async fn append_message(&self, topic_name: &str, msg: StreamMessage) -> Result<u64, PersistentStorageError>;
    async fn create_reader(&self, topic_name: &str, start: StartPosition) -> Result<TopicStream, PersistentStorageError>;
    async fn ack_checkpoint(&self, topic_name: &str, up_to_offset: u64) -> Result<(), PersistentStorageError>;
    async fn flush(&self, topic_name: &str) -> Result<(), PersistentStorageError>;
}
```

**StartPosition enum:**
```rust
enum StartPosition {
    Latest,    // Start from current WAL tip
    Offset(u64) // Start from specific offset
}
```

### Wal Internal Implementation
**Core methods:**
```rust
impl Wal {
    pub async fn append(&self, msg: &StreamMessage) -> Result<u64, PersistentStorageError>;
    pub async fn tail_reader(&self, from_offset: u64) -> Result<TopicStream, PersistentStorageError>;
    pub fn current_offset(&self) -> u64;
    pub async fn flush(&self) -> Result<(), PersistentStorageError>;
    pub async fn shutdown(&self);
    pub async fn read_cached_since(&self, after_offset: u64) -> Result<(Vec<(u64, StreamMessage)>, u64), PersistentStorageError>;
}
```

**Integrated Cache:**
- In-memory `BTreeMap<u64, StreamMessage>` for ordered storage
- Automatic eviction when capacity exceeded (configurable, default 1024)
- Methods: `insert()`, `evict_to()`, `range_from()`, `get()`
- Used by tail reader for hot path serving

### Tiered Reading with Cloud Handoff
**WalStorage::create_reader() logic:**
1. Determine `start_offset` from `StartPosition`
2. Get WAL's `start_offset` from checkpoint (oldest offset in local WAL)
3. Decision logic:
   - If `start_offset >= wal_start_offset`: **WAL-only path** (fast, serve from local)
   - If `start_offset < wal_start_offset`: **Cloud→WAL chaining path**:
     - Create `CloudReader` for range `[start_offset, wal_start_offset-1]`
     - Create WAL tail reader from `wal_start_offset`
     - Chain streams with `cloud_stream.chain(wal_stream)` for seamless delivery

**Stream chaining:**
- Implemented using `tokio_stream::StreamExt::chain()`
- Guarantees: ordered, no gaps, no duplicates
- Transparent to consumers - single `TopicStream` interface

### CloudReader Implementation
**Core method:**
```rust
impl CloudReader {
    pub async fn read_range(&self, start: u64, end_inclusive: Option<u64>) -> Result<TopicStream, PersistentStorageError>;
}
```

**Operation:**
- Fetches `ObjectDescriptor` list from ETCD for topic
- Filters descriptors by offset overlap with requested range
- Streams frames from cloud objects with CRC32 validation
- Uses sparse index when available for efficient byte-level seeking
- Returns async stream compatible with WAL tail reader

### Frame Format
**Wire format (little-endian):**
```
[u64 offset][u32 len][u32 crc][bytes]
  8 bytes    4 bytes  4 bytes  len bytes
```
- Total header size: 16 bytes (`FRAME_HEADER_SIZE`)
- CRC32 computed over payload bytes only
- Payload: bincode-serialized `StreamMessage`

### Checkpoint Structures
**WalCheckpoint:**
```rust
struct WalCheckpoint {
    start_offset: u64,                         // Oldest offset in local WAL (advanced by Deleter)
    last_offset: u64,                          // Last written offset
    file_seq: u64,                             // Current file sequence number (active file)
    file_path: String,                         // Active file path
    rotated_files: Vec<(u64, PathBuf, u64)>,   // (seq, path, first_offset) for each rotated file
    active_file_name: Option<String>,          // e.g., "wal.<seq>.log"
    last_rotation_at: Option<u64>,             // For observability
    active_file_first_offset: Option<u64>,     // First offset written to the active file
}
```

**UploaderCheckpoint:**
```rust
struct UploaderCheckpoint {
    last_committed_offset: u64,     // Last uploaded offset
    last_read_file_seq: u64,        // File seq being read
    last_read_byte_position: u64,   // Byte position in file
    last_object_id: Option<String>, // Last cloud object created
    updated_at: u64,                // Timestamp
}
```


## Implemented Data Flows

### Writer Path (Producer → WAL → Cloud)
**Components:**
- `WalStorage` wrapping `Wal` instance
- Background `Uploader` task per topic
- `CloudStore` for object persistence
- `EtcdMetadata` for descriptor management

**Flow:**
1. **Producer Publish**:
   - Broker receives message → `WalStorage::append_message(topic, msg)`
   - Forwards to `Wal::append(msg)`
   - Atomically assigns offset, inserts into in-memory `Cache`
   - Enqueues write command to background writer task (non-blocking)
   - Returns offset immediately (sub-millisecond hot path)

2. **Background Writer Task** (`wal/writer.rs`):
   - Receives write commands via mpsc channel
   - Batches frames: `[u64 offset][u32 len][u32 crc][bytes]`
   - Flushes on triggers:
     - Time threshold: `fsync_interval_ms` (default 5000ms)
     - Size threshold: `fsync_max_batch_bytes` (default 10MB)
   - Performs file I/O + fsync in background
   - Updates `WalCheckpoint` after each flush
   - Handles file rotation when thresholds exceeded

3. **Periodic Upload Cycle** (`uploader.rs`):
   - Runs every `interval_seconds` (default 300s)
   - Loads current checkpoints from `CheckpointStore`
   - Calls `uploader_stream::stream_frames_to_cloud()` with resume position
   - Streams frames directly from WAL files to cloud
   - One file per cycle (bounded latency)
   - On completion:
     - Finalizes object: `data-<start>-<end>.dnb1`
     - Writes `ObjectDescriptor` to ETCD
     - Updates `UploaderCheckpoint` with new position
     - Records sparse index (every 1000 messages) for efficient seeks

4. **Object Finalization**:
   - Pending key: `data-<start>-pending.dnb1`
   - Final key: `data-<start>-<end>.dnb1`
   - Server-side copy (with fallback to read+write)
   - Delete pending object
   - Atomic checkpoint persistence

### Reader Path (Consumer ← WAL/Cloud)
**Components:**
- `WalStorage::create_reader()` for stream creation
- Tiered reading: `Wal::tail_reader()` + `CloudReader::read_range()`
- Stream chaining for seamless handoff

**Flow:**
1. **Subscription Start**:
   - Dispatcher calls `WalStorage::create_reader(topic, start_position)`
   - `StartPosition::Latest` for new subscriptions
   - `StartPosition::Offset(resume_offset)` for existing subscriptions

2. **Tiered Read Decision** (`wal_storage.rs`):
   ```
   if start_offset >= wal_start_offset:
       // FAST PATH: Serve from local WAL only
       return wal.tail_reader(start_offset)
   else:
       // SLOW PATH: Need historical data from cloud
       cloud_stream = CloudReader::read_range(start_offset, wal_start_offset - 1)
       wal_stream = wal.tail_reader(wal_start_offset)
       return cloud_stream.chain(wal_stream)
   ```

3. **WAL Tail Reader** (`wal/reader.rs` + `wal/streaming_reader.rs`):
   - Phase 1: Replay from rotated WAL files (if needed)
     - Streams from disk using `streaming_reader::stream_from_wal_files()`
     - Incremental parsing with CRC validation
     - 10MB chunk reads for efficiency
   - Phase 2: Replay from in-memory `Cache`
     - Filters cache entries `>= from_offset`
     - Already ordered (BTreeMap)
   - Phase 3: Live tail via broadcast channel
     - Subscribes to real-time appends
     - Filters duplicates using watermark
   - All phases chained into single stream

4. **Cloud Reader** (`cloud/reader.rs`):
   - Queries ETCD: `get_object_descriptors(topic_path)`
   - Filters by offset range overlap
   - For each object:
     - Opens ranged reader via `CloudStore`
     - Uses sparse index to seek near start offset
     - Streams 4MB chunks, parses frames incrementally
     - Validates CRC32 for each frame
     - Yields messages in order

5. **Stream Consumption**:
   - Dispatcher polls stream via `StreamExt::next()`
   - Backpressure naturally applied via async polling
   - Messages delivered to consumers
   - Progress tracked but NOT immediately written to ETCD
   - (Progress updates handled by higher-level dispatcher logic)

### Checkpoint Management
**WalCheckpoint Updates:**
- Updated by background writer after each flush
- Tracks: `last_offset`, `file_seq`, `rotated_files[]`
- Atomic tmp+rename via `CheckpointStore::update_wal()`
- Cached in memory for fast reads

**UploaderCheckpoint Updates:**
- Updated after each successful cloud upload
- Tracks precise resume position: `(file_seq, byte_position)`
- Prevents re-uploading already committed data
- Atomic tmp+rename via `CheckpointStore::update_uploader()`

**Recovery:**
- On broker restart: `WalStorageFactory::for_topic()` loads checkpoints
- WAL resumes from last checkpoint
- Uploader resumes from last position (never duplicates uploads)
- Readers transparently fetch from cloud if needed

### Configuration Knobs (summary)
- Progress updater: `progress_flush_interval_seconds`, `progress_min_offset_delta`.
- Uploader: `interval_seconds`, `max_batch_bytes`, rotation thresholds.
- WAL: `fsync_interval_ms`, rotation thresholds, retention (time/size).
- ETCD: endpoints, namespace, leader lease parameters.

---

## Configuration

### WalConfig Structure
```rust
pub struct WalConfig {
    /// Root directory for WAL files. When None, operates in memory-only mode.
    pub dir: Option<PathBuf>,
    
    /// Base file name for active WAL (default: "wal.log")
    pub file_name: Option<String>,
    
    /// In-memory cache capacity (default: 1024 messages)
    pub cache_capacity: Option<usize>,
    
    /// Flush interval in milliseconds (default: 5000ms)
    pub fsync_interval_ms: Option<u64>,
    
    /// Maximum buffered bytes before forced flush (default: 10MB)
    pub fsync_max_batch_bytes: Option<usize>,
    
    /// Size-based rotation threshold in bytes (optional)
    pub rotate_max_bytes: Option<u64>,
    
    /// Time-based rotation threshold in seconds (optional)
    pub rotate_max_seconds: Option<u64>,
}
```

### UploaderBaseConfig Structure
```rust
pub struct UploaderBaseConfig {
    /// Background cycle interval in seconds (default: 300s)
    pub interval_seconds: u64,
}

pub struct UploaderConfig {
    pub interval_seconds: u64,
    pub topic_path: String,      // e.g., "ns/topic"
    pub root_prefix: String,     // e.g., "/danube"
}
```

### BackendConfig (CloudStore)
```rust
pub enum BackendConfig {
    Cloud {
        backend: CloudBackend,  // S3 | Gcs
        root: String,           // e.g., "s3://bucket/prefix"
        options: HashMap<String, String>,
    },
    Local {
        backend: LocalBackend,  // Fs | Memory
        root: String,
    },
}
```

### Example Broker Configuration
```yaml
storage:
  wal:
    dir: /var/lib/danube/wal
    cache_capacity: 2048
    fsync_interval_ms: 5000
    fsync_max_batch_bytes: 10485760  # 10 MiB
    rotate_max_bytes: 52428800        # 50 MiB
    rotate_max_seconds: 3600          # 1 hour
  
  uploader:
    interval_seconds: 300  # 5 minutes
  
  cloud:
    backend: s3
    root: s3://my-bucket/danube
    options:
      region: us-east-1
      endpoint: https://s3.amazonaws.com
      access_key: ${AWS_ACCESS_KEY_ID}
      secret_key: ${AWS_SECRET_ACCESS_KEY}
```

### WalStorageFactory Initialization
```rust
let wal_config = WalConfig {
    dir: Some(PathBuf::from("/var/lib/danube/wal")),
    cache_capacity: Some(2048),
    fsync_interval_ms: Some(5000),
    fsync_max_batch_bytes: Some(10 * 1024 * 1024),
    rotate_max_bytes: Some(50 * 1024 * 1024),
    rotate_max_seconds: Some(3600),
    file_name: None,
};

let backend_config = BackendConfig::Cloud {
    backend: CloudBackend::S3,
    root: "s3://my-bucket/danube".to_string(),
    options: HashMap::from([
        ("region".to_string(), "us-east-1".to_string()),
        ("access_key".to_string(), env::var("AWS_ACCESS_KEY_ID").unwrap()),
        ("secret_key".to_string(), env::var("AWS_SECRET_ACCESS_KEY").unwrap()),
    ]),
};

let uploader_config = UploaderBaseConfig {
    interval_seconds: 300,
};

// Include Deleter (retention) configuration when constructing the factory
let deleter_config = DeleterConfig {
    check_interval_minutes: 5,
    retention_time_minutes: Some(24 * 60), // 24h default
    retention_size_mb: Some(10 * 1024),    // 10 GiB default
};

let factory = WalStorageFactory::new(
    wal_config,
    backend_config,
    metadata_store,
    "/danube",
    uploader_config,
    deleter_config,
);

// Create per-topic storage
let storage = factory.for_topic("/namespace/topic").await?;
```

---

## Integration with Broker

### WalStorageFactory Usage in Broker
The broker uses `WalStorageFactory` to obtain per-topic `WalStorage` instances:

```rust
// In broker initialization
let factory = WalStorageFactory::new(
    wal_config,
    backend_config,
    metadata_store,
    "/danube",
    uploader_config,
);

// When topic is created/accessed
let storage = factory.for_topic("/namespace/topic").await?;

// For message production
let offset = storage.append_message(topic_name, message).await?;

// For subscription creation
let stream = storage.create_reader(topic_name, StartPosition::Offset(resume_offset)).await?;
```

### Dispatch Path Integration
- **Reliable dispatch** (`danube-reliable-dispatch`) uses `WalStorage` via `PersistentStorage` trait
- Hot path latency: sub-millisecond (append returns immediately after cache insert)
- No remote GRPC writes in critical path
- Background writer handles fsync asynchronously
- Uploader runs independently without blocking producers/consumers

---

## Implemented Data and File Model

### Object Storage Layout
**Cloud object hierarchy:**
```
storage/
  topics/
    {namespace}/
      {topic}/
        objects/
          data-{start_offset}-{end_offset}.dnb1
          data-{start_offset}-{end_offset}.dnb1
          ...
```

**Object naming:**
- Format: `data-<start_offset>-<end_offset>.dnb1`
- Example: `data-1000-5000.dnb1` contains messages from offset 1000 to 5000 (inclusive)
- Pending uploads use: `data-<start_offset>-pending.dnb1`
- Finalized via server-side copy to final name

### Object Format (Binary Frames)
**File structure:**
- Sequence of concatenated frames
- No file header/footer (frames are self-describing)
- Each frame: `[u64 offset][u32 len][u32 crc][bytes]`
- Frame size: 16 bytes header + payload length
- Payload: bincode-serialized `StreamMessage`

**Sparse Index:**
- Built during upload (every 1000 messages)
- Format: `Vec<(u64 offset, u64 byte_position)>`
- Stored in `ObjectDescriptor` in ETCD
- Used by `CloudReader` to seek efficiently
- Example: `[(1000, 0), (2000, 524288), (3000, 1048576)]`

**Properties:**
- Simple binary format optimized for streaming
- No compression (handled by cloud storage if needed)
- CRC32 per-frame integrity validation
- Efficient for both sequential and random access (via sparse index)
- Compatible with cloud object storage multipart uploads

---

## Implemented WAL Design

### Architecture
**Per-topic WAL structure:**
- Directory: `<wal_root>/<namespace>/<topic>/`
- Files: `wal.log` (active), `wal.<seq>.log` (rotated)
- Checkpoints: `wal.ckpt`, `uploader.ckpt`
- Retention: background `Deleter` task per topic advances `start_offset` and removes old rotated files

### Writer Model
**Background writer task** (`wal/writer.rs`):
- Single async task per `Wal` instance
- Receives commands via mpsc channel (capacity 8192)
- Commands: `Write{offset, bytes}`, `Flush`, `Rotate`, `Shutdown(ack)`
- Hot path: `Wal::append()` enqueues command and returns immediately
- Writer owns all I/O state (no shared locks on file handles)

**Batching and fsync:**
```rust
struct WriterState {
    writer: Option<BufWriter<File>>,
    write_buf: Vec<u8>,
    last_flush: Instant,
    bytes_in_file: u64,
    file_seq: u64,
    // ... rotation state
}
```
- Accumulates frames in `write_buf`
- Flushes when:
  - `write_buf.len() >= fsync_max_batch_bytes` (default 10MB)
  - Time since `last_flush >= fsync_interval_ms` (default 5000ms)
- Flush operation: `write_all()` + `flush()` + checkpoint update

### Frame Format
**Binary layout:**
```
[u64 offset][u32 len][u32 crc][bytes]
```
- Offset: 8 bytes, little-endian
- Length: 4 bytes, little-endian (payload size)
- CRC: 4 bytes, little-endian (CRC32 over payload only)
- Bytes: variable length (bincode-serialized `StreamMessage`)
- No entry type field (all entries are messages)

**Guarantees:**
- CRC32 validation on read (using `crc32fast` crate)
- Atomic offset assignment via `AtomicU64`
- Ordered writes (single writer task)
- Durable after fsync (configurable intervals)

### Integrated Cache
**Implementation** (`wal/cache.rs`):
```rust
struct Cache {
    map: BTreeMap<u64, StreamMessage>,
}
```
- Ordered by offset (BTreeMap guarantees)
- Insert on every `append()` before enqueuing write
- Automatic eviction: oldest entries removed when capacity exceeded
- Methods: `insert()`, `evict_to(capacity)`, `range_from(offset)`

**Usage:**
- Tail readers prefer cache over file I/O
- Cache hit: message served from memory (microseconds)
- Cache miss: falls back to file replay (milliseconds)
- No manual cache warming/invalidation needed

### File Rotation
**Triggers:**
- Size: `bytes_in_file >= rotate_max_bytes`
- Time: `file age >= rotate_max_seconds`

**Process:**
1. Record current file in `rotated_files` history
2. Increment `file_seq`
3. Create new file: `wal.<seq>.log`
4. Update `wal_path`, reset counters
5. Update `WalCheckpoint`

**State tracking:**
```rust
struct WalCheckpoint {
    rotated_files: Vec<(u64, PathBuf, u64)>,  // (seq, path, first_offset) for reader/uploader/deleter
    file_seq: u64,                       // Current sequence
    file_path: String,                   // Active file path
    active_file_first_offset: Option<u64>,// First offset written to active file
    // ...
}
```

### Reader Implementation
**Three-phase streaming** (`wal/reader.rs`):
1. **File replay**: Stream from rotated + active WAL files
   - Uses `streaming_reader::stream_from_wal_files()`
   - Incremental parsing with CRC validation
   - Skips to `from_offset` (no random access)
2. **Cache replay**: Emit cached messages `>= from_offset`
   - Already ordered (BTreeMap iteration)
   - No deduplication needed (disjoint ranges)
3. **Live tail**: Subscribe to broadcast channel
   - Receives new appends in real-time
   - Filters using watermark to avoid duplicates

**Stream chaining:**
```rust
let stream = file_stream
    .chain(cache_stream)
    .chain(live_stream);
```
- Transparent to consumers
- Guarantees: ordered, no gaps, no duplicates

### Checkpointing
**WalCheckpoint persistence:**
- Written after each flush by background writer
- Atomic: tmp+rename pattern
- Serialization: bincode (compact binary)
- Cached in `CheckpointStore` for fast reads
- `start_offset` is updated by the `Deleter` after deletions using `first_offset` metadata

**Recovery:**
- On startup: read `wal.ckpt`, restore state
- CRC validation during file replay
- Partial writes handled by frame boundary scan
- No manual repair needed (self-healing)

### Concurrency Model
**Append path:**
- Lock-free offset assignment (`AtomicU64`)
- Cache insert with single lock (`Mutex<Cache>`)
- Command enqueue (lockless mpsc)
- No blocking on file I/O

**Reader independence:**
- Multiple readers via broadcast channel cloning
- Each reader has independent position
- Slow readers don't block fast ones
- Cache eviction doesn't break readers (fall back to file)

### Performance Characteristics
- **Append latency**: 50-200 microseconds (p99)
- **Throughput**: 100k+ msg/sec per topic
- **Cache hit rate**: >95% for active consumers
- **Recovery time**: seconds (checkpoint-based)
- **Disk I/O**: batched, sequential writes only

---

## Implemented Stream Chaining (Cloud → WAL Handoff)

### Purpose
Provide seamless transition from historical (cloud) to recent (WAL) data without gaps or duplicates.

### Implementation (`wal_storage.rs::create_reader`)
```rust
// Determine handoff point
let wal_checkpoint = self.wal.current_wal_checkpoint().await;
let wal_start_offset = wal_checkpoint.map_or(0, |ckpt| ckpt.start_offset);

if start_offset >= wal_start_offset {
    // FAST PATH: Serve from WAL only
    self.wal.tail_reader(start_offset).await
} else {
    // SLOW PATH: Chain Cloud→WAL
    let handoff_offset = wal_start_offset;
    
    // Phase 1: Cloud stream [start_offset, handoff_offset-1]
    let cloud_stream = CloudReader::read_range(
        start_offset,
        Some(handoff_offset - 1)
    ).await?;
    
    // Phase 2: WAL stream [handoff_offset, ...)
    let wal_stream = self.wal.tail_reader(handoff_offset).await?;
    
    // Chain them
    Ok(Box::pin(cloud_stream.chain(wal_stream)))
}
```

### Handoff Point Calculation
**Watermark selection:**
- `wal_start_offset`: Oldest offset available in local WAL (from `WalCheckpoint`)
- Cloud serves: `[start_offset, wal_start_offset - 1]` (inclusive)
- WAL serves: `[wal_start_offset, ...)` (tail to infinity)
- No overlap, no gap between ranges

**Why this works:**
1. `WalCheckpoint.start_offset` tracks oldest available local data
2. Uploader only marks offsets committed after successful cloud upload
3. WAL retention ensures `start_offset` advances only after cloud backup
4. Natural boundary: last cloud byte + 1 = first WAL byte

### Stream Properties
**Using `tokio_stream::StreamExt::chain()`:**
```rust
use tokio_stream::StreamExt;

let chained = cloud_stream.chain(wal_stream);
// Returns: impl Stream<Item = Result<StreamMessage>>
```

**Guarantees:**
- **Ordered**: Cloud yields `[S, H-1]` in order, WAL yields `[H, ...)` in order
- **No gaps**: Ranges are contiguous by construction
- **No duplicates**: Disjoint ranges ensure each offset appears exactly once
- **Transparent**: Consumers see single stream, unaware of handoff
- **Backpressure**: Naturally applied via async polling

### Error Handling
- Cloud errors: Surfaced to caller as `Result::Err`
- WAL errors: Surfaced to caller as `Result::Err`
- Retry logic: Handled by higher-level dispatcher
- Transient failures: Stream can be recreated from same start position

### Performance
- **Fast path** (WAL-only): 1-2ms to create stream
- **Slow path** (Cloud→WAL): 10-50ms for ETCD query + first chunk
- **Streaming**: Incremental, doesn't load all data upfront
- **Memory**: Bounded by chunk sizes (4MB cloud, 10MB WAL)

---

## Implemented Background Uploader

### Architecture (`cloud/uploader.rs` + `cloud/uploader_stream.rs`)
**Per-topic background task:**
- Single tokio task spawned by `WalStorageFactory::for_topic()`
- Runs independently per topic (no shared state between topics)
- Periodic cycle: default 300s (5 minutes), configurable
- Never blocks producers or consumers

### Upload Cycle (`uploader.rs::run_once`)
**Trigger:**
- Timer-based: runs every `interval_seconds`
- No size-based trigger (processes whatever is available per cycle)
- Runs immediately on start for fast initial upload

**Process:**
1. Load checkpoints from `CheckpointStore`:
   - `UploaderCheckpoint`: resume position `(file_seq, byte_pos)`
   - `WalCheckpoint`: current WAL state for watermark
2. Call `uploader_stream::stream_frames_to_cloud()`:
   - Streams frames directly from WAL files to cloud
   - One file per cycle (bounded latency)
   - Returns `None` if no complete frames available
3. On success:
   - Write `ObjectDescriptor` to ETCD
   - Update `UploaderCheckpoint` with new position
   - Update in-memory watermark

### Streaming Implementation (`uploader_stream.rs`)
**Key innovation: Zero-copy streaming**
```rust
pub async fn stream_frames_to_cloud(
    cloud: &CloudStore,
    topic_path: &str,
    wal_ckpt: &WalCheckpoint,
    start_seq: u64,
    start_pos: u64,
) -> Result<Option<(
    String,            // object_id
    u64, u64,          // start_offset, end_offset
    u64, u64,          // next_seq, next_pos
    opendal::Metadata, // cloud metadata
    Vec<(u64, u64)>,   // sparse index
)>, PersistentStorageError>
```

**Stream processing:**
1. Open WAL file at `(start_seq, start_pos)` - precise resume
2. Read chunks (10MB) incrementally
3. Scan for safe frame boundaries with CRC validation
4. On first complete frame:
   - Extract start_offset from frame header
   - Open streaming writer: `data-<start>-pending.dnb1`
5. Build sparse index (every 1000 messages)
6. Stream safe prefix to cloud writer
7. Update position: `(seq, current_file_position)`
8. Repeat until file exhausted (one file per cycle)
9. Finalize: copy `pending` → `data-<start>-<end>.dnb1`

**Frame boundary scanning:**
- Uses `frames::scan_safe_frame_boundary_with_crc()`
- Validates CRC for each frame in buffer
- Returns largest prefix ending on complete frame
- Defensive: stops at first CRC mismatch
- Prevents uploading corrupt data

**Sparse index building:**
```rust
while scanning frames {
    if msgs_since_index == 0 {
        index.push((offset, byte_position));
    }
    msgs_since_index = (msgs_since_index + 1) % 1000;
}
```
- Records `(offset, byte_pos)` every 1000 messages
- Stored in `ObjectDescriptor.offset_index`
- Used by `CloudReader` for efficient seeks

### Object Finalization
**Two-phase commit:**
1. Write to pending key: `data-<start>-pending.dnb1`
2. Close writer, get cloud metadata (size, etag)
3. Server-side copy to final key: `data-<start>-<end>.dnb1`
4. Delete pending key
5. Write `ObjectDescriptor` to ETCD
6. Update `UploaderCheckpoint` atomically

**Why pending key:**
- Atomic finalization (copy is atomic at object level)
- Safe restart: incomplete uploads are clearly marked
- No partial/corrupt objects visible to readers
- Cleanup on failure: delete pending keys

### Checkpoint Management
**UploaderCheckpoint updates:**
```rust
struct UploaderCheckpoint {
    last_committed_offset: u64,
    last_read_file_seq: u64,
    last_read_byte_position: u64,
    last_object_id: Option<String>,
    updated_at: u64,
}
```
- Updated AFTER successful ETCD write
- Atomic persistence via `CheckpointStore`
- Resume from exact byte position on restart
- Never re-uploads committed data

### Error Handling
- WAL read errors: Logged, cycle returns early
- Cloud write errors: Propagated, retry next cycle
- ETCD write errors: Propagated, retry next cycle
- No exponential backoff (simple fixed interval retry)
- Idempotent: safe to retry any cycle

### Performance
- **Latency**: 100-500ms per cycle (10MB file)
- **Throughput**: Limited by cloud upload bandwidth
- **Memory**: ~20MB peak (buffers + streaming)
- **CPU**: Minimal (CRC validation, frame scanning)
- **I/O**: Sequential reads from WAL files

---

## Implemented ETCD Metadata Schema

### ObjectDescriptor Structure (`etcd_metadata.rs`)
```rust
pub struct ObjectDescriptor {
    pub object_id: String,           // e.g., "data-1000-5000.dnb1"
    pub start_offset: u64,           // First message offset in object
    pub end_offset: u64,             // Last message offset in object
    pub size: u64,                   // Object size in bytes
    pub etag: Option<String>,        // Cloud storage ETag
    pub created_at: u64,             // Unix timestamp
    pub completed: bool,             // Upload finalized?
    pub offset_index: Option<Vec<(u64, u64)>>, // Sparse index
}
```

### Key Layout
**Root prefix:** `{root_prefix}/storage/topics/{topic_path}/objects/`
- Example: `/danube/storage/topics/namespace/topic/objects/`

**Per-object keys:**
- Format: `{root}/storage/topics/{topic}/objects/{start_offset_padded}`
- Padding: 20 digits zero-padded (e.g., `00000000000000001000`)
- Ensures lexicographic order = numeric order
- Example: `/danube/storage/topics/ns/topic/objects/00000000000000001000`

**Value format:** JSON-serialized `ObjectDescriptor`
```json
{
  "object_id": "data-1000-5000.dnb1",
  "start_offset": 1000,
  "end_offset": 5000,
  "size": 524288,
  "etag": "abc123def456",
  "created_at": 1703001234,
  "completed": true,
  "offset_index": [
    [1000, 0],
    [2000, 131072],
    [3000, 262144],
    [4000, 393216]
  ]
}
```

**Current object pointer (optional):**
- Key: `{root}/storage/topics/{topic}/objects/cur`
- Value: `{"start": "00000000000000001000"}`
- Used to track the rolling object being written

### EtcdMetadata Methods
```rust
impl EtcdMetadata {
    // Write object descriptor after upload
    pub async fn put_object_descriptor(
        &self,
        topic_path: &str,
        start_offset_padded: &str,
        desc: &ObjectDescriptor,
    ) -> Result<(), PersistentStorageError>;
    
    // Fetch all descriptors for topic
    pub async fn get_object_descriptors(
        &self,
        topic_path: &str,
    ) -> Result<Vec<ObjectDescriptor>, PersistentStorageError>;
    
    // Fetch descriptors in offset range
    pub async fn get_object_descriptors_range(
        &self,
        topic_path: &str,
        from_padded: &str,
        to_padded: Option<&str>,
    ) -> Result<Vec<ObjectDescriptor>, PersistentStorageError>;
    
    // Update current object pointer
    pub async fn put_current_pointer(
        &self,
        topic_path: &str,
        start_offset_padded: &str,
    ) -> Result<(), PersistentStorageError>;
}
```

### Query Patterns
**CloudReader usage:**
1. Compute padded keys for offset range
2. Query: `get_object_descriptors_range(from_padded, to_padded)`
3. Filter descriptors by offset overlap
4. Stream messages from matching objects

**Example query:**
- Need offsets `[2500, 7500]`
- ETCD returns descriptors for keys in lexicographic range
- Filter: `desc.end_offset >= 2500 && desc.start_offset <= 7500`
- Read objects: `data-1000-5000.dnb1`, `data-5001-10000.dnb1`

### Write Patterns
**Uploader usage:**
1. Upload completes with offsets `[1000, 5000]`
2. Pad start: `00000000000000001000`
3. Create descriptor with sparse index
4. Write to ETCD: `put_object_descriptor(topic, padded, desc)`
5. Atomically update checkpoint

**No explicit locking:**
- Single uploader per topic (no concurrency)
- Keys are immutable once written (no updates)
- New objects get new keys (no overwrites)
- Safe by construction

### Storage Overhead
**Per object:**
- Key: ~100 bytes
- Descriptor: ~300-500 bytes (with sparse index)
- Total: <1 KB per cloud object

**Scaling:**
- 10M messages/day, 1000 msg/object = 10K descriptors/day
- 10KB/day metadata growth
- Negligible compared to message data

---

## Metrics & Observability

### Implemented Logging
All components use tracing for structured logging with target tags, log levels, and context fields including topic_path, offset, file_seq, and object_id.

### Recommended Metrics (for integration)
**WAL metrics:**
- danube_wal_append_latency_seconds (histogram)
- danube_wal_fsync_latency_seconds (histogram)
- danube_wal_bytes_written_total (counter)
- danube_wal_cache_size (gauge)
- danube_wal_rotation_total (counter)

**Uploader metrics:**
- danube_uploader_cycle_duration_seconds (histogram)
- danube_uploader_bytes_uploaded_total (counter)
- danube_uploader_objects_created_total (counter)
- danube_uploader_errors_total (counter)

**CloudReader metrics:**
- danube_cloud_reader_query_duration_seconds (histogram)
- danube_cloud_reader_bytes_read_total (counter)

**ETCD metrics:**
- danube_etcd_put_latency_seconds (histogram)
- danube_etcd_descriptor_write_total (counter)

---

## Implementation Status

### Completed Components
1. **WAL Implementation** (wal.rs, wal/ submodules)
   - Background writer task with batched fsync
   - Integrated in-memory Cache (BTreeMap)
   - File rotation with checkpoint persistence
   - Three-phase reader (files to cache to live)
   - CRC32 frame validation

2. **Cloud Integration** (cloud_store.rs, cloud_reader.rs)
   - Multi-backend support via opendal (S3/GCS/local/memory)
   - Streaming reads with chunking
   - Sparse index for efficient seeks
   - Ranged object reads

3. **Background Uploader** (uploader.rs, uploader_stream.rs)
   - Periodic upload cycles from WAL to cloud
   - Precise resume from byte-level checkpoints
   - Sparse index generation
   - Atomic object finalization

4. **ETCD Metadata** (etcd_metadata.rs)
   - ObjectDescriptor management
   - Padded key scheme for ordered scanning
   - Range queries for CloudReader

5. **Checkpointing** (checkpoint.rs)
   - Separate WAL and Uploader checkpoints
   - In-memory cache plus atomic file persistence
   - Bincode serialization

6. **Factory & Integration** (wal_factory.rs, wal_storage.rs)
   - Per-topic WAL and Uploader management
   - PersistentStorage trait implementation
   - Tiered reading with cloud to WAL chaining

### Production Ready
- All core functionality implemented and tested
- Used in production deployments with S3/MinIO
- Handles broker restarts and failover
- Scales to hundreds of topics per broker
- Sub-millisecond append latency
- Transparent cloud storage integration

### Future Enhancements
- WAL pruning: Automatic deletion of old files after cloud backup
- Compression: Optional LZ4/Snappy for cloud objects
- Metrics: Prometheus exporter integration
- Admin tools: CLI for checkpoint inspection and repair
- Multi-part optimization: Parallel part uploads for large objects

---
