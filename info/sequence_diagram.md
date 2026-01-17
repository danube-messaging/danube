# Topic Move - Sequence Diagrams

## Current Broken Behavior

```
┌────────────┐  ┌────────────┐  ┌─────────┐  ┌──────────┐  ┌──────────┐
│  Producer  │  │ Broker A   │  │  ETCD   │  │ Broker B │  │ Consumer │
└────────────┘  └────────────┘  └─────────┘  └──────────┘  └──────────┘
      │               │              │             │              │
      │  Produce 0-21 │              │             │              │
      │──────────────>│              │             │              │
      │               │              │             │              │
      │               │ Cloud Upload │             │              │
      │               │ (offsets 0-21)            │              │
      │               │──────────────>             │              │
      │               │              │             │              │
      │         [Admin: Unload Topic]             │              │
      │               │              │             │              │
      │               │ Seal State   │             │              │
      │               │ (last=21)    │             │              │
      │               │─────────────>│             │              │
      │               │              │             │              │
      │               │ Delete WAL   │             │              │
      │               │ (local files)│             │              │
      │               │              │             │              │
      │               │              │ Assign Topic│              │
      │               │              │────────────>│              │
      │               │              │             │              │
      │               │              │      Create WAL            │
      │               │              │      ❌ Starts at 0!       │
      │               │              │             │              │
      │  Produce 2-7  │              │             │              │
      │──────────────────────────────────────────>│              │
      │               │              │      ❌ Get offsets 0-5    │
      │               │              │             │              │
      │               │              │      Uploader checkpoint=21│
      │               │              │      Won't upload 0-5      │
      │               │              │             │              │
      │               │              │             │  Subscribe   │
      │               │              │             │<─────────────│
      │               │              │             │  (cursor=6)  │
      │               │              │             │              │
      │               │              │      ❌ Reads from offset 14+│
      │               │              │             │  (wrong data)│
      │               │              │             │─────────────>│
      │               │              │             │              │
      │               │              │             │ ❌ Messages  │
      │               │              │             │    2-13 LOST │
```

## Expected Correct Behavior (After Fix)

```
┌────────────┐  ┌────────────┐  ┌─────────┐  ┌──────────┐  ┌──────────┐
│  Producer  │  │ Broker A   │  │  ETCD   │  │ Broker B │  │ Consumer │
└────────────┘  └────────────┘  └─────────┘  └──────────┘  └──────────┘
      │               │              │             │              │
      │  Produce 0-21 │              │             │              │
      │──────────────>│              │             │              │
      │               │              │             │              │
      │               │ Cloud Upload │             │              │
      │               │ (offsets 0-21)            │              │
      │               │──────────────>             │              │
      │               │              │             │              │
      │         [Admin: Unload Topic]             │              │
      │               │              │             │              │
      │               │ Seal State   │             │              │
      │               │ (last=21)    │             │              │
      │               │─────────────>│             │              │
      │               │              │             │              │
      │               │ Delete WAL   │             │              │
      │               │ (local files)│             │              │
      │               │              │             │              │
      │               │              │ Assign Topic│              │
      │               │              │────────────>│              │
      │               │              │             │              │
      │               │              │      Read Sealed State     │
      │               │              │<────────────│              │
      │               │              │ (last=21)   │              │
      │               │              │             │              │
      │               │              │      Create WAL            │
      │               │              │      ✅ Starts at 22!      │
      │               │              │             │              │
      │  Produce 2-7  │              │             │              │
      │──────────────────────────────────────────>│              │
      │               │              │      ✅ Get offsets 22-27  │
      │               │              │             │              │
      │               │              │      Upload to Cloud       │
      │               │              │      (offsets 22-27)       │
      │               │              │             │              │
      │               │              │             │  Subscribe   │
      │               │              │             │<─────────────│
      │               │              │             │  (cursor=6)  │
      │               │              │             │              │
      │               │              │      Read from Cloud 6-21  │
      │               │              │             │              │
      │               │              │      Then WAL 22-27        │
      │               │              │             │─────────────>│
      │               │              │             │              │
      │               │              │             │ ✅ All msgs  │
      │               │              │             │    delivered │
```

## Component Interaction Details

### Unload Flow (Topic Control → WAL Factory → ETCD)

```
TopicManager.unload_reliable_topic()
    │
    ├─> flush_subscription_cursors()  
    │   └─> [Subscription] flush_progress_now()
    │       └─> ETCD: /topics/.../subscriptions/.../cursor
    │
    ├─> flush_and_seal()
    │   │
    │   ├─> WAL.flush()
    │   │   └─> Writer: fsync pending data
    │   │
    │   ├─> Uploader: cancel_and_drain()
    │   │   └─> Upload remaining frames
    │   │       └─> ETCD: /storage/topics/.../objects/...
    │   │
    │   ├─> Deleter: cancel()
    │   │
    │   └─> ETCD.put_storage_state_sealed()
    │       └─> ETCD: /storage/topics/.../state
    │           {
    │             sealed: true,
    │             last_committed_offset: 21,
    │             broker_id: 10285...,
    │             timestamp: ...
    │           }
    │
    └─> delete_all_producers()
        └─> ETCD: /topics/.../producers/...
```

### Load Flow (Broker Watcher → Topic Manager → WAL Factory)

#### Current (Broken)
```
BrokerWatcher.handle_put_event()
    │
    └─> TopicManager.ensure_local()
        │
        └─> WalFactory.for_topic()
            │
            └─> get_or_create_wal()
                │
                ├─> ❌ Does NOT check ETCD sealed state
                │
                └─> Wal::with_config_with_store()
                    └─> next_offset = 0  ❌ ALWAYS ZERO!
```

#### Fixed
```
BrokerWatcher.handle_put_event()
    │
    └─> TopicManager.ensure_local()
        │
        └─> WalFactory.for_topic()
            │
            └─> get_or_create_wal()
                │
                ├─> ✅ EtcdMetadata.get_storage_state_sealed()
                │   │
                │   └─> ETCD: /storage/topics/.../state
                │       Returns: { last_committed_offset: 21, ... }
                │
                ├─> initial_offset = 21 + 1 = 22  ✅
                │
                └─> Wal::with_config_with_store(cfg, ckpt, Some(22))
                    └─> next_offset = 22  ✅ CORRECT!
```

## State Transitions

### Broker A (Before Unload)
```
WAL State:
┌──────────────────┐
│ next_offset: 22  │  (ready for next message)
│ cache: [0..21]   │  (recent messages)
│ checkpoint: 21   │  (persisted to disk)
└──────────────────┘

Cloud State (ETCD):
┌────────────────────────┐
│ objects/0000..00000    │
│   {start: 0, end: 21}  │
└────────────────────────┘

Topic State: ACTIVE
```

### Broker A (After Unload)
```
WAL State:
┌──────────────────┐
│ ❌ DELETED       │  (files removed)
└──────────────────┘

Cloud State (ETCD):
┌────────────────────────┐
│ objects/0000..00000    │
│   {start: 0, end: 21}  │
│                        │
│ ✅ state               │
│   {sealed: true,       │
│    last_offset: 21}    │
└────────────────────────┘

Topic State: UNASSIGNED
```

### Broker B (Current - Broken)
```
WAL State:
┌──────────────────┐
│ next_offset: 0   │  ❌ WRONG!
│ cache: []        │
│ checkpoint: none │
└──────────────────┘

Uploader State:
┌──────────────────┐
│ checkpoint: 21   │  (from ETCD/local)
│ won't upload 0-21│  (thinks it's done)
└──────────────────┘

Result: OFFSET COLLISION
```

### Broker B (After Fix)
```
WAL State:
┌──────────────────┐
│ next_offset: 22  │  ✅ CORRECT!
│ cache: []        │
│ checkpoint: none │
└──────────────────┘

Uploader State:
┌──────────────────┐
│ checkpoint: 21   │  (from sealed state)
│ will upload 22+  │  ✅ CORRECT!
└──────────────────┘

Result: CONTINUITY PRESERVED
```

## Data Flow

### Message Offset Assignment

#### Broken Flow
```
Producer → Broker B
              ├─> WAL.append()
              │     └─> fetch_add(next_offset)  [0, 1, 2, 3...]
              │         ❌ Collision with cloud [0..21]
              │
              └─> Uploader sees checkpoint=21
                  └─> Skips offsets 0-21
                      ❌ New messages never uploaded!
```

#### Fixed Flow
```
Producer → Broker B
              ├─> WAL.append()
              │     └─> fetch_add(next_offset)  [22, 23, 24...]
              │         ✅ Continues from 22
              │
              └─> Uploader sees checkpoint=21
                  └─> Uploads offsets 22+
                      ✅ All messages uploaded!
```

### Consumer Read Path

#### Broken
```
Consumer.subscribe(cursor=6)
    ├─> SubscriptionEngine.init_from_progress()
    │     └─> start_offset = 6
    │
    ├─> WalStorage.create_reader(offset=6)
    │     ├─> WAL checkpoint: start=0
    │     ├─> Request offset 6 >= WAL start 0
    │     └─> ❌ Read from NEW WAL (wrong offset 6!)
    │
    └─> Consumer receives:
        ├─> ❌ Offset 14 from NEW WAL (not same as old offset 14)
        └─> ❌ Messages 6-13 missing/corrupted
```

#### Fixed
```
Consumer.subscribe(cursor=6)
    ├─> SubscriptionEngine.init_from_progress()
    │     └─> start_offset = 6
    │
    ├─> WalStorage.create_reader(offset=6)
    │     ├─> WAL checkpoint: start=22
    │     ├─> Request offset 6 < WAL start 22
    │     ├─> ✅ Read from CLOUD (offsets 6-21)
    │     └─> ✅ Chain to WAL (offsets 22+)
    │
    └─> Consumer receives:
        ├─> ✅ Offsets 6-21 from cloud
        └─> ✅ Offsets 22+ from WAL
            ✅ All messages in order!
```

## Critical Code Paths

### Path 1: Sealed State Write (Working)
```
topic_control.rs:362
  → wal_factory.rs:252 flush_and_seal()
    → wal.rs:413 flush()
    → uploader.rs:100 cancel (triggers drain)
    → wal_factory.rs:284 last_offset = wal.current_offset() - 1
    → wal_factory.rs:292 etcd.put_storage_state_sealed()
      → etcd_metadata.rs:128 ✅ WRITES sealed state
```

### Path 2: Sealed State Read (MISSING!)
```
broker_watcher.rs:130 ensure_local()
  → wal_factory.rs:121 for_topic()
    → wal_factory.rs:188 get_or_create_wal()
      → ❌ NO call to etcd.get_storage_state_sealed()
      → wal.rs:177 with_config_with_store(cfg, ckpt, None)
        → wal.rs:239 next_offset: AtomicU64::new(0)
          ❌ ALWAYS ZERO!
```

### Path 3: Sealed State Read (FIXED)
```
broker_watcher.rs:130 ensure_local()
  → wal_factory.rs:121 for_topic()
    → wal_factory.rs:188 get_or_create_wal()
      → ✅ etcd.get_storage_state_sealed()  [NEW!]
      → ✅ initial_offset = sealed.last + 1  [NEW!]
      → wal.rs:177 with_config_with_store(cfg, ckpt, Some(22))
        → wal.rs:239 next_offset: AtomicU64::new(22)
          ✅ CORRECT!
```
