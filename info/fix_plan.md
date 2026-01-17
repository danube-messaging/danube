# Fix Plan for Reliable Topic Move Issue

## Priority: CRITICAL - Data Integrity Bug

## Problem Statement
When a reliable topic is moved from one broker to another, the new broker creates a WAL starting from offset 0, causing offset collision with previously committed messages. This results in message loss and data corruption.

## Solution Strategy

### Phase 1: Read Sealed State (Infrastructure)

**File: `danube-persistent-storage/src/etcd_metadata.rs`**

Add function to read sealed state:
```rust
/// Get sealed state marker from `/storage/topics/<ns>/<topic>/state`.
pub async fn get_storage_state_sealed(
    &self,
    topic_path: &str,
) -> Result<Option<StorageStateSealed>, PersistentStorageError> {
    let key = format!("{}/storage/topics/{}/state", self.root, topic_path);
    match self.store.get(&key, MetaOptions::None).await {
        Ok(Some(value)) => {
            let state = serde_json::from_value::<StorageStateSealed>(value)
                .map_err(|e| PersistentStorageError::Other(e.to_string()))?;
            Ok(Some(state))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(PersistentStorageError::Metadata(e.to_string())),
    }
}
```

### Phase 2: WAL Initial Offset Support

**File: `danube-persistent-storage/src/wal.rs`**

Modify `with_config_with_store` to accept initial offset:

```rust
pub async fn with_config_with_store(
    cfg: WalConfig,
    ckpt_store: Option<Arc<CheckpointStore>>,
    initial_offset: Option<u64>,  // NEW PARAMETER
) -> Result<Self, PersistentStorageError> {
    // ... existing setup code ...
    
    let start_offset = initial_offset.unwrap_or(0);
    
    let wal = Self {
        inner: Arc::new(WalInner {
            next_offset: AtomicU64::new(start_offset),  // CHANGED
            // ... rest unchanged ...
        }),
    };
    
    // ... rest unchanged ...
}
```

Update backward-compatible wrapper:
```rust
pub async fn with_config(cfg: WalConfig) -> Result<Self, PersistentStorageError> {
    Self::with_config_with_store(cfg, None, None).await  // Add None for offset
}
```

### Phase 3: Factory Integration

**File: `danube-persistent-storage/src/wal_factory.rs`**

Modify `get_or_create_wal` to check sealed state:

```rust
async fn get_or_create_wal(
    &self,
    topic_path: &str,
) -> Result<(Wal, Option<std::path::PathBuf>, Option<std::sync::Arc<CheckpointStore>>), PersistentStorageError> {
    if let Some(existing) = self.topics.get(topic_path) {
        return Ok((existing.clone(), None, None));
    }
    
    // Build per-topic config
    let mut cfg = self.base_cfg.clone();
    let mut root_path: Option<std::path::PathBuf> = None;
    let mut ckpt_store: Option<std::sync::Arc<CheckpointStore>> = None;
    
    // ... existing directory setup code ...
    
    // NEW: Check for sealed state to determine initial offset
    let initial_offset = match self.etcd.get_storage_state_sealed(topic_path).await {
        Ok(Some(sealed_state)) if sealed_state.sealed => {
            let next_offset = sealed_state.last_committed_offset + 1;
            info!(
                target = "wal_factory",
                topic = %topic_path,
                sealed_offset = sealed_state.last_committed_offset,
                resuming_from = next_offset,
                "found sealed state, resuming WAL from next offset"
            );
            Some(next_offset)
        }
        Ok(Some(_)) => {
            // State exists but not sealed - shouldn't happen normally
            warn!(
                target = "wal_factory",
                topic = %topic_path,
                "found unsealed state marker, ignoring"
            );
            None
        }
        Ok(None) => {
            // No sealed state - new topic or same broker
            None
        }
        Err(e) => {
            warn!(
                target = "wal_factory",
                topic = %topic_path,
                error = %e,
                "failed to read sealed state, starting from 0"
            );
            None
        }
    };
    
    // Create WAL with initial offset
    let wal = Wal::with_config_with_store(cfg, ckpt_store.clone(), initial_offset).await?;
    self.topics.insert(topic_path.to_string(), wal.clone());
    Ok((wal, root_path, ckpt_store))
}
```

### Phase 4: Cleanup Sealed State (Optional but Recommended)

**File: `danube-persistent-storage/src/wal_factory.rs`**

After successfully loading a topic with sealed state, delete the sealed state marker:

```rust
// In for_topic() after get_or_create_wal succeeds
if initial_offset.is_some() {
    // Topic loaded successfully from sealed state, clean up marker
    if let Err(e) = self.etcd.delete_storage_state_sealed(&topic_path).await {
        warn!(
            target = "wal_factory",
            topic = %topic_path,
            error = %e,
            "failed to cleanup sealed state marker (non-critical)"
        );
    }
}
```

Add to `etcd_metadata.rs`:
```rust
pub async fn delete_storage_state_sealed(
    &self,
    topic_path: &str,
) -> Result<(), PersistentStorageError> {
    let key = format!("{}/storage/topics/{}/state", self.root, topic_path);
    self.store
        .delete(&key)
        .await
        .map_err(|e| PersistentStorageError::Metadata(e.to_string()))?;
    Ok(())
}
```

## Testing Plan

### Test 1: Basic Topic Move
```
1. Create topic on broker A
2. Produce messages 0-9
3. Consumer reads 0-4, cursor at 5
4. Unload topic from broker A
5. Topic assigned to broker B
6. Verify: sealed state exists with last_committed_offset=9
7. Verify: WAL on broker B starts at offset 10
8. Produce messages on broker B
9. Verify: new messages get offsets 10, 11, 12...
10. Consumer reconnects, reads from offset 5
11. Verify: receives messages 5-12 in order
```

### Test 2: Multiple Moves
```
1. Move topic A → B (messages 0-9)
2. Produce on B (messages 10-19)
3. Move topic B → C
4. Verify: sealed state has last_committed_offset=19
5. Verify: WAL on C starts at offset 20
```

### Test 3: Broker Restart (No Move)
```
1. Topic on broker A with messages 0-9
2. Restart broker A
3. Verify: Uses local checkpoint, NOT sealed state
4. Verify: Can continue producing from offset 10
```

### Test 4: New Topic
```
1. Create new topic (never moved)
2. Verify: No sealed state exists
3. Verify: WAL starts at offset 0
```

### Test 5: Edge Cases
```
1. Sealed state exists but uploader checkpoint is newer
   - Should use max(sealed, checkpoint) or checkpoint takes precedence
2. Multiple sealed states (shouldn't happen)
   - Latest one wins
3. Corrupted sealed state
   - Fall back to offset 0 with warning
```

## Rollback Plan

If issues arise:
1. Revert code changes
2. Sealed states in ETCD are benign (not read by old code)
3. No data migration needed

## Performance Impact

- **Minimal**: One extra ETCD read per topic load
- Only happens during topic assignment/move
- Cached in memory after initial load

## Backward Compatibility

- ✅ Topics without sealed state work as before
- ✅ Existing local checkpoints continue to work
- ✅ New topics start from offset 0
- ✅ Only affects topic move scenario (which is currently broken)

## Code Review Checklist

- [ ] `get_storage_state_sealed()` handles all error cases
- [ ] WAL initial offset parameter properly propagates
- [ ] Factory checks sealed state before creating WAL
- [ ] Logging added for observability
- [ ] Sealed state cleanup doesn't break anything if it fails
- [ ] Unit tests for sealed state read/write
- [ ] Integration test for topic move
- [ ] Documentation updated

## Related Issues to Consider

### Issue: What if both brokers load the topic simultaneously?

This shouldn't happen due to cluster assignment logic, but if it does:
- Both would read same sealed state
- Both would start from same offset
- Cluster assignment should prevent dual ownership
- Not introduced by this fix (existing problem if any)

### Issue: What about non-reliable topics?

Non-reliable topics don't use WAL or cloud storage:
- No sealed state written during unload
- No offset continuity needed
- This fix only applies to reliable topics

### Issue: What if local checkpoint > sealed state?

Priority should be:
1. Local checkpoint (broker wasn't moved, just restarted)
2. Sealed state (broker was moved)
3. Default to 0 (new topic)

Current implementation: Local checkpoint files are loaded by CheckpointStore, WAL writer uses them. Sealed state only matters when there's no local checkpoint.

## Estimated Effort

- **Phase 1**: 30 min (simple ETCD get function)
- **Phase 2**: 1 hour (WAL API change + update callers)
- **Phase 3**: 1 hour (Factory logic + error handling)
- **Phase 4**: 30 min (Cleanup function)
- **Testing**: 2-3 hours (Integration tests)
- **Total**: 5-6 hours

## Conclusion

This is a critical fix for reliable topic move functionality. The implementation is straightforward:
1. Read sealed state from ETCD
2. Initialize WAL with correct offset
3. Clean up sealed state after successful load

No data migration required. Backward compatible. Low risk.
