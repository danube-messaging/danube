# WAL Retention Policy Implementation Design

## Status
**DESIGN PHASE** - Implementation document for WAL file retention/deletion mechanism

## Problem Statement

The Danube WAL writes files to disk indefinitely without cleanup. While data is safely uploaded to cloud storage, local WAL files accumulate causing:
- Disk space exhaustion
- Directory bloat
- Unnecessary I/O overhead

Since data is durably persisted to cloud and StatefulReader can read from cloud when needed, we can safely delete local WAL files after certain conditions are met.

---

## Design Goals

1. **Prevent disk exhaustion**: Automatically delete old WAL files
2. **Safety first**: Never delete data not uploaded to cloud
3. **Reader compatibility**: StatefulReader continues working (falls back to cache/cloud)
4. **Minimal disruption**: No blocking of write/upload path
5. **Configurable**: Per-topic and broker-level retention policies
6. **Observable**: Metrics and logging for retention operations

---

## Retention Strategy

### Industry Best Practices
- **Kafka**: Both time and size (`log.retention.hours`, `log.retention.bytes`)
- **Apache Pulsar**: Primarily time-based at namespace/topic level

### Our Approach: Hybrid (Time + Size)
- **Time-based**: Delete files older than X minutes
- **Size-based**: Delete oldest files when total exceeds Y MB
- **Both**: Whichever triggers first

### Scope: Per-Topic Retention
- Each topic has own WAL directory: `<root>/<namespace>/<topic>/`
- Independent retention settings per topic
- Aligns with existing WalStorageFactory architecture
- Configuration hierarchy: Topic → Namespace → Broker defaults

---

## Safety Constraints

### Files That Must NOT Be Deleted

1. **Active file**: Current file being written (`WalCheckpoint.file_seq`)
2. **Files not fully uploaded**: `seq >= UploaderCheckpoint.last_read_file_seq`
3. **Files with uncommitted data**: `file_max_offset > UploaderCheckpoint.last_committed_offset`

### Deletion Safety Rule

A rotated WAL file `wal.<seq>.log` can be deleted **IF AND ONLY IF**:
```
seq < WalCheckpoint.file_seq                               (not active)
AND seq < UploaderCheckpoint.last_read_file_seq           (fully processed)
AND file_end_offset <= UploaderCheckpoint.last_committed_offset  (in cloud)
AND (file_age > retention_time OR total_size > retention_size)
```

---

## Architecture Design

### New Component: WAL Deleter

**Location**: `danube-persistent-storage/src/wal/deleter.rs`

**Responsibility**: Background task that periodically evaluates and deletes eligible WAL files

**Key Structures**:
```rust
pub struct DeleterConfig {
    pub check_interval_minutes: u64,          // Default: 5 minutes
    pub retention_time_minutes: Option<u64>,  // Default: 1440 (24 hrs)
    pub retention_size_mb: Option<u64>,       // Default: 10240 (10 GB)
}

pub struct Deleter {
    topic_path: String,
    wal_dir: PathBuf,
    checkpoint_store: Arc<CheckpointStore>,
    config: DeleterConfig,
}
```

### Deleter Algorithm (Per Cycle)

```
1. Load checkpoints: wal_ckpt, uploader_ckpt
2. Build list of rotated files from wal_ckpt.rotated_files
3. For each file:
   - Safety checks (active, uploading, uncommitted)
   - Get file metadata (size, modified time)
4. Apply retention policies:
   - Time-based: age > retention_time_seconds
   - Size-based: total_size > retention_size_bytes, delete oldest
5. Delete eligible files
6. Update WalCheckpoint.rotated_files (remove deleted)
7. Update WalCheckpoint.start_offset to oldest remaining file's first offset
8. Persist updated checkpoint atomically
```

### Synchronization via CheckpointStore

- Deleter **reads**: `get_wal()`, `get_uploader()` (RwLock read)
- Deleter **writes**: `update_wal()` after deletion (RwLock write)
- Writer/Uploader: Only add/advance
- Deleter: Only removes old entries
- No race conditions possible

---

## Implementation Phases

### Phase 1: Core Deleter Logic ✅ (4-6 hours)
**Create**: `danube-persistent-storage/src/wal/deleter.rs`
**Modify**: `wal.rs`, `wal_factory.rs`

Tasks:
- [x] Create `DeleterConfig` and `Deleter` structs
- [x] Implement `Deleter::new()`, `start()`, `run_cycle()`
- [x] Implement safety checks for file eligibility
- [x] Implement time-based and size-based retention
- [x] Implement file deletion and checkpoint update
- [x] Add logging (info, warn, error, debug)
- [x] Error handling (continue on single file errors)

### Phase 2: start_offset Management ✅ (2-3 hours)
**Problem**: `start_offset` currently hardcoded to 0 in `writer.rs:185`
**Solution**: Update after deletions to first offset in oldest remaining file

Tasks:
- [x] Implement `scan_file_first_offset()` helper
- [x] Update `start_offset` in Deleter after deletions
- [x] Test `start_offset` advancing
- [x] Verify WalStorage respects updated `start_offset`

### Phase 3: Configuration Integration ✅ (2-3 hours)
**Modify**: `danube_broker.yml`, `service_configuration.rs`, `main.rs`, `wal_factory.rs`

Configuration in `danube_broker.yml`:
```yaml
wal_cloud:
  wal:
    retention:
      time_minutes: 1440          # 24 hours (1440 minutes) default
      size_mb: 10240              # 10 GB (10240 MB) default
      check_interval_minutes: 5   # 5 minutes default
```

Add to `service_configuration.rs`:
```rust
pub struct WalRetentionNode {
    pub time_minutes: Option<u64>,
    pub size_mb: Option<u64>,
    pub check_interval_minutes: Option<u64>,
}
```

Tasks:
- [x] Add `WalRetentionNode` to service_configuration.rs
- [x] Update broker YAML with retention config
- [x] Pass `DeleterConfig` to WalStorageFactory
- [x] Spawn Deleter in `for_topic()` alongside Uploader

### Phase 4: Testing & Validation ✅ (6-8 hours)
**Create**: `tests/wal_retention_test.rs`, `src/wal/deleter_test.rs`

Test Coverage:
- [x] Does NOT delete active file
- [x] Does NOT delete file being uploaded
- [x] Does NOT delete file with uncommitted data
- [x] Time-based retention triggers correctly
- [x] Size-based retention triggers correctly
- [x] `start_offset` updates correctly
- [x] StatefulReader works after deletion
- [x] Checkpoint updates are atomic
- [x] Handles file system errors gracefully
- [x] Multiple topics work independently

### Phase 5: Observability & Metrics ✅ (2-3 hours)
**Metrics** (prometheus/tracing):
- `wal_retention_files_deleted_total{topic}`
- `wal_retention_bytes_reclaimed_total{topic}`
- `wal_retention_errors_total{topic}`
- `wal_retention_last_run_timestamp{topic}`
- `wal_total_disk_usage_bytes{topic}`

**Logging**:
- INFO: File deletions (offset range, size, age)
- WARN: Safety violations
- ERROR: File system errors, checkpoint failures
- DEBUG: Retention evaluation details

---

## Integration Points

### 1. WalStorageFactory Changes

```rust
// Add deleter_config and deleters map
pub struct WalStorageFactory {
    deleter_config: DeleterConfig,
    deleters: Arc<DashMap<String, JoinHandle<()>>>,
    // ... existing fields ...
}

// Constructor accepts DeleterConfig
pub fn new(..., deleter_config: DeleterConfig) -> Self

// for_topic spawns Deleter
pub async fn for_topic(&self, topic_name: &str) -> Result<...> {
    // ... existing WAL/Uploader code ...
    
    // Start deleter
    if !self.deleters.contains_key(&topic_path) {
        let deleter = Arc::new(Deleter::new(
            topic_path.clone(),
            wal_dir.clone(),
            ckpt_store.clone(),
            self.deleter_config.clone(),
        ));
        let handle = deleter.start();
        self.deleters.insert(topic_path.clone(), handle);
    }
}
```

### 2. Broker main.rs Changes

```rust
// Parse retention config from YAML
let deleter_config = DeleterConfig {
    check_interval_minutes: wal_cfg.wal.retention
        .as_ref()
        .and_then(|r| r.check_interval_minutes)
        .unwrap_or(5),
    retention_time_minutes: wal_cfg.wal.retention
        .as_ref()
        .and_then(|r| r.time_minutes),
    retention_size_mb: wal_cfg.wal.retention
        .as_ref()
        .and_then(|r| r.size_mb),
};

// Pass to factory
let wal_factory = WalStorageFactory::new(
    wal_base_cfg,
    cloud_backend,
    metadata_store.clone(),
    etcd_root,
    uploader_base_cfg,
    deleter_config, // NEW
);
```

### 3. Checkpoint Updates

**No new checkpoint fields needed**. Use existing:
- `WalCheckpoint.rotated_files` - Remove deleted entries
- `WalCheckpoint.start_offset` - Update to oldest remaining file

---

## Key Implementation Details

### File Scanning Helpers

```rust
// Scan for maximum offset (last message)
async fn scan_file_max_offset(path: &Path) -> Result<u64, ...> {
    // Read frame headers, track max offset
    // Used to verify all data is uploaded
}

// Scan for minimum offset (first message)
async fn scan_file_first_offset(path: &Path) -> Result<u64, ...> {
    // Read first frame header
    // Used to update start_offset after deletion
}
```

### Retention Policy Application

```rust
async fn apply_retention_policy(
    &self,
    candidates: &[(u64, PathBuf, u64, Option<SystemTime>)],
) -> Vec<(u64, PathBuf, u64)> {
    let mut to_delete = Vec::new();
    
    // Time-based: files older than threshold
    if let Some(retention_seconds) = self.config.retention_time_seconds {
        for file in candidates.iter().filter(|f| age(f) > retention_seconds) {
            to_delete.push(file);
        }
    }
    
    // Size-based: delete oldest until under limit
    if let Some(retention_bytes) = self.config.retention_size_bytes {
        let total = candidates.iter().map(|f| f.size).sum();
        if total > retention_bytes {
            let excess = total - retention_bytes;
            let mut freed = 0;
            for file in candidates.iter() {
                if freed < excess {
                    to_delete.push(file);
                    freed += file.size;
                }
            }
        }
    }
    
    to_delete
}
```

---

## Testing Strategy

### Unit Tests (`deleter_test.rs`)
- Test retention policy logic in isolation
- Mock file system operations
- Verify checkpoint updates

### Integration Tests (`wal_retention_test.rs`)
- Create real WAL files, rotate, upload
- Trigger deletion, verify files removed
- Test StatefulReader after deletion
- Multi-topic scenarios

### End-to-End Tests (broker-level)
- Full broker startup with retention enabled
- Produce messages, wait for rotation/upload/deletion
- Verify disk usage stays bounded
- Consumer restarts work correctly

---

## Rollout Plan

1. **Phase 1-2**: Implement core deleter (can be disabled by default)
2. **Phase 3**: Add configuration (retention disabled by default)
3. **Phase 4**: Complete testing, enable in dev/staging
4. **Phase 5**: Add observability, monitor metrics
5. **Production**: Enable with conservative defaults (48hrs OR 20GB)
6. **Tuning**: Adjust based on production metrics

---

## Configuration Defaults

| Parameter | Default | Rationale |
|-----------|---------|-----------|
| `check_interval_minutes` | 5 | Balance responsiveness vs overhead |
| `retention_time_minutes` | 1440 (24 hours) | One day of local buffer |
| `retention_size_mb` | 10240 (10 GB) | ~10M messages @ 1KB each |

**Conservative production defaults**:
- Time: 2880 minutes (48 hours / 2 days)
- Size: 20480 MB (20 GB) per topic
- Allows time for debugging, reduces deletion frequency

---

## Success Criteria

✅ **Functional**:
- WAL disk usage bounded by retention policy
- No data loss (all files verified uploaded before deletion)
- Readers continue working after file deletion

✅ **Performance**:
- Deletion cycle < 1 second for 100 files
- No impact on write/read latency
- Memory overhead < 10 MB per topic

✅ **Operational**:
- Clear metrics for monitoring
- Detailed logs for debugging
- Configuration errors caught at startup
- Graceful degradation on file system errors

---

## Future Enhancements

1. **Compaction**: Merge small rotated files before deletion
2. **Intelligent retention**: Keep files with active readers longer
3. **Namespace-level policies**: Override per namespace
4. **Soft deletion**: Move to trash before permanent delete
5. **Cloud-first mode**: Delete immediately after upload (aggressive)

---

## Summary

This retention implementation:
- ✅ Solves disk exhaustion problem
- ✅ Maintains safety guarantees (never delete uncommitted data)
- ✅ Minimal code changes (single new module + integrations)
- ✅ Backward compatible (disabled by default)
- ✅ Well-tested and observable
- ✅ Industry-standard approach (time + size)

**Total estimated effort**: 16-23 hours across all phases
