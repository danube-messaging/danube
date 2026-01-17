# Topic Move Bug - Executive Summary

## The Problem in Simple Terms

When you move a reliable topic from Broker A to Broker B:
- Broker A says: "I've handled messages 0 through 21, here's the sealed state"
- Broker B says: "Thanks! But I'm going to start numbering from 0 again"
- **Result**: Offset collision and message loss

## What's Happening

### Normal Flow (Working)
```
Broker A:
  Messages: 0, 1, 2, 3, ..., 21
  Cloud:    0, 1, 2, 3, ..., 21 ✅
  Sealed:   last_committed_offset = 21 ✅
```

### After Move to Broker B (BROKEN)
```
Broker B:
  WAL starts from: 0 ❌ (should be 22)
  New messages:    0, 1, 2, 3, 4, 5... ❌
  
  These collide with:
  Cloud storage:   0, 1, 2, 3, ..., 21
```

### What Consumer Sees
```
Consumer cursor: 6
Tries to read:   6, 7, 8, 9, 10...

Gets:
  - Offset 6 from NEW WAL (not the same as offset 6 from cloud!)
  - Messages 2-13 from producer are LOST
  - Only sees offset 14+ because that's what's in cache
```

## Root Cause

**Location**: `danube-persistent-storage/src/wal.rs` lines 132, 239
```rust
next_offset: AtomicU64::new(0)  // Always starts from 0!
```

**Missing**: Code to read sealed state and initialize from `last_committed_offset + 1`

## The Fix (3 Simple Steps)

### Step 1: Add Function to Read Sealed State
```rust
// File: etcd_metadata.rs
pub async fn get_storage_state_sealed(...) -> Option<StorageStateSealed>
```

### Step 2: Allow WAL to Start from Non-Zero Offset
```rust
// File: wal.rs
pub async fn with_config_with_store(
    cfg: WalConfig,
    ckpt_store: Option<Arc<CheckpointStore>>,
    initial_offset: Option<u64>,  // ← Add this
) -> Result<Self, PersistentStorageError>
```

### Step 3: Check Sealed State Before Creating WAL
```rust
// File: wal_factory.rs
// In get_or_create_wal():

let sealed = etcd.get_storage_state_sealed(topic).await?;
let initial_offset = sealed.map(|s| s.last_committed_offset + 1);
let wal = Wal::with_config_with_store(cfg, ckpt_store, initial_offset).await?;
```

## Evidence

### From Your Logs

**ETCD shows the problem** (`info/etcd_move.md` line 68):
```
# After move, new broker creates object:
PUT /danube-data/storage/topics/default/reliable_topic/objects/00000000000000000000
{"end_offset":20, ...}  ← Should start from 22, not 0!
```

**Broker logs confirm** (`info/brokers_log.md`):
```
# Old broker sealed at 21:
Line 27: "resumed uploader from checkpoint last_committed_offset=42"

# New broker starts from 21 in uploader, but 0 in WAL:
Line 54: "resumed uploader from checkpoint last_committed_offset=21"
Line 59: "creating reader from WAL only start=14 wal_start=0"  ← WAL starts at 0!
```

**Consumer sees gap** (`info/the_process.md`):
```
Producer sent:    messages 2-7 (after move)
Consumer gets:    offset 14 onwards
Missing:          offsets 2-13 ❌
```

## Why This Happens

### Unload Process (CORRECT)
1. ✅ Flush WAL
2. ✅ Stop uploader
3. ✅ Write sealed state with `last_committed_offset=21`
4. ✅ Delete local WAL files

### Load Process (BROKEN)
1. ✅ Check for uploader checkpoint → finds 21
2. ❌ Does NOT check sealed state
3. ❌ Creates WAL from offset 0
4. ❌ Producer gets offsets 0, 1, 2, 3...
5. ❌ Uploader sees checkpoint=21, won't upload new 0-21
6. ❌ Messages stuck in local WAL, never reach cloud

## Impact

**Severity**: CRITICAL - Data Loss Bug

**Affects**:
- ✅ Topic moves between brokers
- ✅ Any scenario where topic is unloaded and reassigned

**Does NOT affect**:
- ✅ Same broker restart (uses local checkpoint)
- ✅ New topics (correctly start from 0)
- ✅ Non-reliable topics (don't use WAL/cloud)

## Files Involved

### Must Modify (Core Fix)
1. `danube-persistent-storage/src/etcd_metadata.rs` - Add get function
2. `danube-persistent-storage/src/wal.rs` - Accept initial offset
3. `danube-persistent-storage/src/wal_factory.rs` - Check and apply sealed state

### Review (Understanding)
4. `danube-broker/src/topic_control.rs` - Unload logic
5. `danube-broker/src/danube_service/broker_watcher.rs` - Topic loading
6. `danube-broker/src/topic_cluster.rs` - Move coordination

## Next Steps

1. **Read**: Full details in `info/investigation_report.md`
2. **Plan**: Implementation steps in `info/fix_plan.md`
3. **Test**: Verify with topic move test case
4. **Fix**: Implement 3-step solution above

## Key Insight

The infrastructure is already there:
- ✅ Sealed state is written correctly
- ✅ ETCD stores it properly
- ✅ All the metadata is available

**Missing**: Just 3 functions to READ and USE the sealed state!

---

**Bottom Line**: When moving a topic, the new broker needs to ask "where did the old broker leave off?" and continue from there, not restart from zero.
