# Topic Move Investigation Report

## Problem Summary

When a reliable topic is moved from one broker to another via `unload` command, messages produced after the move are lost until the consumer reconnects. Specifically, messages with offsets 2-13 are missing when the consumer reads from offset 6.

## Root Cause Analysis

### The Core Issue: WAL Offset Initialization

**The WAL always starts with offset 0, regardless of whether the topic has previous committed state.**

Location: `danube-persistent-storage/src/wal.rs`
- Line 132: `next_offset: AtomicU64::new(0)`  (default constructor)
- Line 239: `next_offset: AtomicU64::new(0)`  (with_config constructor)

### What Should Happen During Topic Move

1. **Old Broker (10285063371164059634):**
   - Produces messages with offsets 0-21
   - Uploads to cloud storage (object with `end_offset=21`)
   - On unload, calls `flush_and_seal()` which:
     - Flushes WAL
     - Stops uploader after drain
     - Writes sealed state to ETCD with `last_committed_offset=21`
   - Deletes local WAL files

2. **New Broker (10421046117770015389):**
   - Receives topic assignment
   - Creates NEW WAL instance
   - **PROBLEM**: WAL starts from offset 0 (should start from 22)
   - Uploader resumes from checkpoint (`last_committed_offset=21`)
   - Producer sends new messages → assigned offsets 0, 1, 2, 3...

### The Offset Collision

```
Cloud Storage (from old broker):
  offsets 0-21 ← already uploaded

New WAL (on new broker):
  offsets 0, 1, 2, 3, 4, 5, 6, 7... ← COLLISION!
  
Uploader checkpoint says: "Already uploaded up to 21"
  → Won't re-upload offsets 0-21 from new WAL
  → New messages get stuck in local WAL only
```

### Evidence from Logs

**From etcd_move.md:**
```
Line 48: Sealed state written by old broker
  last_committed_offset=21

Line 68: New object created on new broker
  end_offset=20  ← This is WRONG! Should continue from 22+
```

**From brokers_log.md:**
```
Line 27 (old broker):
  "resumed uploader from checkpoint target="uploader" last_committed_offset=42"

Line 54 (new broker):
  "resumed uploader from checkpoint target="uploader" last_committed_offset=21"
```

The new broker's uploader knows the last committed offset, but the WAL doesn't!

### Why Messages Are Missing

When consumer connects and requests offset 6:
1. Consumer's subscription cursor is at offset 6
2. Subscription engine tries to read from offset 6
3. WAL cache has NEW messages (offsets 0-13 from new broker)
4. But offset 6 from NEW WAL is not the same message as offset 6 from OLD WAL
5. Consumer receives offset 14+ because those happen to be in the cache

## The Missing Piece

### Sealed State is Written But Never Read

**Location: `danube-persistent-storage/src/etcd_metadata.rs`**

Functions available:
- ✅ `put_storage_state_sealed()` - Line 128 (writes sealed state)
- ❌ `get_storage_state_sealed()` - **DOES NOT EXIST**

**Location: `danube-persistent-storage/src/wal_factory.rs`**

The `for_topic()` method (line 121) creates or reuses a WAL but:
- Does NOT check for existing sealed state in ETCD
- Does NOT initialize WAL offset from sealed state
- Always creates WAL starting from offset 0

## Impact on Different Operations

### Topic Move (Unload → Reassign)
- ❌ **BROKEN**: Offset collision causes message loss
- Old messages (0-21) are in cloud
- New messages (0-N) start from 0 again
- Uploader won't upload new 0-21 because it thinks they're already done

### Broker Restart (Same Broker)
- ✅ **WORKS**: Local checkpoint files preserve WAL state
- CheckpointStore loads from disk
- WAL writer resumes from checkpoint

### Complete Topic Delete → Recreate
- ✅ **WORKS**: Fresh start, no previous state
- New topic starts from offset 0 correctly

## Code Flow Analysis

### Current Unload Flow

```
topics_admin.rs:326
  └─> topic_cluster.rs:206 post_unload_topic()
       └─> mark_topic_for_unload()
       └─> schedule_topic_deletion()
            └─> broker_watcher.rs:192 handle_delete_event()
                 └─> topic_control.rs:356 unload_reliable_topic()
                      ├─> flush_subscription_cursors()
                      ├─> flush_and_seal()  ← Writes sealed state to ETCD
                      │    └─> wal_factory.rs:252
                      │         └─> etcd.put_storage_state_sealed()
                      └─> delete_all_producers()
```

### Current Topic Creation Flow on New Broker

```
broker_watcher.rs:49 handle_put_event()
  └─> topic_control.rs:60 ensure_local()
       └─> wal_factory.rs:121 for_topic()
            └─> get_or_create_wal()  ← Creates WAL from offset 0
                 └─> wal.rs:177 with_config_with_store()
                      └─> next_offset: AtomicU64::new(0)  ← PROBLEM!
```

**Missing Step**: Should query ETCD for sealed state before creating WAL!

## Required Fix

### 1. Add `get_storage_state_sealed()` to EtcdMetadata

File: `danube-persistent-storage/src/etcd_metadata.rs`

```rust
pub async fn get_storage_state_sealed(
    &self,
    topic_path: &str,
) -> Result<Option<StorageStateSealed>, PersistentStorageError>
```

### 2. Modify WAL Creation to Accept Initial Offset

File: `danube-persistent-storage/src/wal.rs`

Add parameter to `with_config_with_store()` to set initial offset:

```rust
pub async fn with_config_with_store(
    cfg: WalConfig,
    ckpt_store: Option<Arc<CheckpointStore>>,
    initial_offset: Option<u64>,  // ← NEW
) -> Result<Self, PersistentStorageError>
```

Then use: `next_offset: AtomicU64::new(initial_offset.unwrap_or(0))`

### 3. Update WalStorageFactory to Check Sealed State

File: `danube-persistent-storage/src/wal_factory.rs`

In `for_topic()` or `get_or_create_wal()`:
1. Check if sealed state exists in ETCD
2. If exists, use `last_committed_offset + 1` as initial offset
3. Pass this to WAL constructor

### 4. Handle Edge Cases

- **Unsealed to Sealed Transition**: When existing topic (not moved) continues
  - Local checkpoint should take precedence
  - Only use sealed state if no local checkpoint exists

- **Multiple Moves**: Topic moved multiple times
  - Always use the latest sealed state
  - Or delete sealed state when topic loads successfully

- **Concurrent Writes**: Two brokers trying to load same topic
  - Existing cluster assignment logic should prevent this
  - Sealed state is read-only for new broker

## Verification Steps

After implementing the fix:

1. **Test topic move:**
   - Create topic, produce messages 0-20
   - Unload topic
   - Produce messages on new broker
   - Verify messages get offsets 21, 22, 23...
   - Verify consumer can read all messages sequentially

2. **Test broker restart:**
   - Ensure local checkpoints still work
   - Verify sealed state doesn't interfere

3. **Test new topic:**
   - Ensure topics without sealed state still start from 0

## Files to Review/Modify

### Core Files (Must Change)
1. ✅ `danube-persistent-storage/src/etcd_metadata.rs` - Add get function
2. ✅ `danube-persistent-storage/src/wal.rs` - Accept initial offset
3. ✅ `danube-persistent-storage/src/wal_factory.rs` - Check sealed state
4. ✅ `danube-broker/src/topic_control.rs` - Ensure proper unload

### Supporting Files (Review)
5. `danube-broker/src/danube_service/broker_watcher.rs` - Topic loading
6. `danube-broker/src/topic_cluster.rs` - Unload coordination
7. `danube-persistent-storage/src/cloud/uploader.rs` - Checkpoint handling
8. `danube-broker/src/dispatcher/exclusive/reliable.rs` - Consumer reading

## Additional Observations

### Why the Uploader Shows Offset 42 Initially?

From `brokers_log.md` line 27:
```
resumed uploader from checkpoint target="uploader" last_committed_offset=42
```

This suggests the topic had previous activity before this test, and offset 42 was from an earlier session. This doesn't affect the current issue but indicates checkpoint persistence is working.

### Why Consumer Sees Offset 14+?

The consumer subscription cursor was at offset 13 (from ETCD line 27). When it reconnects:
1. Tries to read from offset 14 onwards
2. NEW WAL has messages starting from 0
3. Messages 0-13 might be in WAL cache
4. Message 14+ from NEW WAL are delivered
5. These are NOT the same as old offset 14+ (which don't exist yet)

## Conclusion

The topic move feature is fundamentally broken due to missing offset continuity. The sealed state is written but never consumed. The fix requires:
1. Adding a read function for sealed state
2. Modifying WAL to accept initial offset
3. Updating factory to check and apply sealed state

This is a critical data integrity issue for reliable messaging in a multi-broker cluster.
