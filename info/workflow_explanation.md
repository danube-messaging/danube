# Reliable Topic Workflow: Understanding Message Flow During Broker Moves

## Document Purpose

This document explains how Danube's reliable topics work under the hood, with a focus on what happens when a topic is moved from one broker to another. 

## Topic Moves: Why and When

A topic may be moved between brokers for several reasons:

1. **Manual Load Balancing** (Current): An operator uses `danube-admin-cli topics unload` to explicitly move a topic
2. **Automated Load Balancing** (Future): The LoadManager detects imbalance and automatically reassigns topics
3. **Broker Maintenance**: A broker needs to be taken offline for upgrades or repairs
4. **Failure Recovery**: A broker crashes and topics need to be reassigned

In all cases, the underlying mechanics are the same: the topic is unloaded from one broker and loaded onto another while preserving message offset continuity and consumer progress.

## Core Concepts

Before diving into the workflow, let's establish key concepts:

- **WAL (Write-Ahead Log)**: Local, per-topic storage on each broker for recent messages
- **Cloud Storage**: Long-term archival storage for historical messages (S3-compatible)
- **Offset**: A monotonically increasing ID (0, 1, 2, ...) assigned to each message in a topic
- **Sealed State**: Metadata written to ETCD when a topic is unloaded, capturing the last committed offset
- **Subscription Cursor**: Tracks which message a consumer last acknowledged
- **Tiered Reading**: Strategy where consumers read old messages from cloud, new messages from WAL

The critical guarantee: **Offsets must be globally unique and continuous for a topic, regardless of which broker hosts it.**

---

## Normal Operation (Single Broker)

When a topic is hosted on a single broker with no moves, the workflow is straightforward. This section establishes the baseline behavior that topic moves must preserve.

### Message Production Flow
```
Producer sends message
    ↓
Topic.append_to_storage()
    ↓
WAL.append(msg)
    ├─> Assign offset: next_offset.fetch_add(1)  // e.g., 0, 1, 2, 3...
    ├─> Store in cache: cache.insert(offset, msg)
    ├─> Write to disk: Writer queue (async fsync)
    └─> Broadcast: tx.send(offset, msg)  // for live consumers
    ↓
Uploader (background task, every 30s)
    ├─> Read WAL files
    ├─> Stream frames to cloud storage
    └─> Write object descriptor to ETCD
        • /storage/topics/default/topic/objects/00000000000000000000
        • {start_offset: 0, end_offset: 21, object_id: "data-0-21.dnb1"}
```

**What's happening here?**

Each message produced to a topic goes through a three-stage pipeline:

1. **Offset Assignment**: The WAL atomically increments `next_offset` and assigns it to the message. This offset is the message's permanent ID.
2. **Local Persistence**: The message is written to the WAL (both in-memory cache and on-disk log) and broadcast to live consumers via a channel.
3. **Cloud Upload**: A background uploader periodically batches WAL segments and uploads them to cloud storage, writing object descriptors to ETCD for later retrieval.

The key insight: **The offset is assigned by the WAL on the hosting broker**. When a topic moves, the new broker must continue the offset sequence from where the old broker left off.

### Message Consumption Flow
```
Consumer subscribes
    ↓
SubscriptionEngine.init_stream_from_progress_or_latest()
    ├─> Check ETCD for cursor: /topics/.../subscriptions/sub_name/cursor
    ├─> If exists: start_offset = cursor_value (e.g., 6)
    └─> If not exists: start_offset = latest
    ↓
WalStorage.create_reader(start_offset=6)
    ├─> Get WAL checkpoint: wal_start_offset = 0
    ├─> Compare: 6 >= 0 (within WAL retention)
    └─> Return: WAL.tail_reader(from=6, live=false)
        ├─> Replay from cache/files: offsets 6, 7, 8...
        └─> Switch to live: broadcast channel for new messages
    ↓
Dispatcher.poll_next()
    ├─> Read message from stream
    ├─> Send to consumer
    └─> Wait for ACK
    ↓
On ACK received:
    ├─> SubscriptionEngine.on_acked(offset)
    └─> Periodically flush cursor to ETCD (every 1000 acks or 5s)
```

**What's happening here?**

Consumers track their progress through a topic using a **cursor** (subscription offset) stored in ETCD:

1. **Initialization**: When a consumer subscribes, the subscription engine checks ETCD for an existing cursor. If found, it resumes from that offset; otherwise, it starts from the latest.
2. **Tiered Reading**: The `WalStorage` determines where to read messages from:
   - If the requested offset is within the WAL's retention range, read directly from WAL
   - If older, read from cloud storage first, then chain to WAL for newer messages
3. **Progress Tracking**: As messages are acknowledged, the cursor advances and is periodically persisted to ETCD

This cursor-based design is **broker-agnostic**: it doesn't matter which broker hosts the topic, as long as the offset space is continuous.

### State at Rest (Single Broker)

**Broker Memory:**
```
WAL:
  next_offset: 22        ← Ready for next message
  cache: [17..21]        ← Recent messages (capacity=1024)
  
Uploader:
  checkpoint: 21         ← Last uploaded offset
  
Subscription "subs_reliable":
  cursor_in_memory: 13   ← Last acked by consumer
```

**ETCD:**
```
/topics/default/reliable_topic/delivery: "Reliable"
/topics/default/reliable_topic/subscriptions/subs_reliable/cursor: 13
/storage/topics/default/reliable_topic/objects/00000000000000000000:
  {start_offset: 0, end_offset: 21, object_id: "data-0-21.dnb1"}
```

**Cloud Storage:**
```
s3://bucket/default/reliable_topic/data-0-21.dnb1
  Contains: Binary frames for offsets 0-21
```

---

## Topic Move Workflow (With Fix)

This section describes the complete flow when a topic is moved from Broker A to Broker B. The move can be triggered in two ways:

1. **Manual (Current)**: An operator runs `danube-admin-cli topics unload /default/reliable_topic`
2. **Automated (Future)**: The LoadManager detects load imbalance and triggers reassignment programmatically

Regardless of trigger, the mechanics are identical. The critical requirement is to **preserve offset continuity** so that:
- Producers on the new broker continue assigning offsets from where the old broker left off
- Consumers can read the entire message history seamlessly across both brokers
- No message is lost, duplicated, or assigned a conflicting offset

The fix introduced a **sealed state mechanism** that captures the last committed offset when unloading and uses it to initialize the WAL on the new broker.

### Step 1: Unload from Broker A

**Trigger**: Either `danube-admin-cli topics unload /default/reliable_topic` or automated LoadManager decision

```
BrokerService.topic_cluster.post_unload_topic()
    ↓
Create unload marker in ETCD
    • /cluster/unassigned/default/reliable_topic
    • {reason: "unload", from_broker: 10285063371164059634}
    ↓
Delete broker assignment
    • /cluster/brokers/10285063371164059634/default/reliable_topic
    ↓
Broker A's watcher sees DELETE event
    ↓
TopicManager.unload_reliable_topic()
    ├─> Flush subscription cursors to ETCD
    │   • /topics/.../subscriptions/subs_reliable/cursor: 13
    │
    ├─> WalFactory.flush_and_seal()
    │   ├─> WAL.flush() → fsync all pending writes
    │   │
    │   ├─> Uploader: cancel and drain
    │   │   ├─> Upload remaining frames to cloud
    │   │   └─> Update ETCD descriptors
    │   │       • objects/00000000000000000000: end_offset=21
    │   │
    │   └─> Write sealed state to ETCD ✅
    │       • /storage/topics/default/reliable_topic/state
    │       • {
    │           sealed: true,
    │           last_committed_offset: 21,  ← CRITICAL!
    │           broker_id: 10285063371164059634,
    │           timestamp: 1768625254
    │         }
    │
    └─> Delete local WAL files
        • rm ./danube-data/wal/default/reliable_topic/*
```

**Broker A Final State:**
- ❌ No local WAL files
- ✅ All messages 0-21 in cloud storage
- ✅ Subscription cursor at 13 in ETCD
- ✅ Sealed state at 21 in ETCD

**What just happened?**

The unload process ensures data durability and captures critical state:

1. **Flush Everything**: All pending writes are committed, and the uploader drains its queue to ensure all messages reach cloud storage
2. **Write Sealed State**: The broker writes `{sealed: true, last_committed_offset: 21}` to ETCD. This is the **key piece of state** that the new broker will use to initialize its WAL correctly.
3. **Clean Up Local State**: The WAL files are deleted since they're now in cloud storage, and the topic is removed from the broker

At this point, Broker A no longer owns the topic, but all messages 0-21 are safely stored in the cloud, and the metadata in ETCD contains everything needed to restore the topic elsewhere.

### Step 2: Assign to Broker B

**LoadManager assigns topic to Broker B** (cluster decides which broker gets it based on load)

```
Write to ETCD:
    • /cluster/brokers/10421046117770015389/default/reliable_topic: null
    ↓
Broker B's watcher sees PUT event
    ↓
TopicManager.ensure_local("/default/reliable_topic")
    ↓
WalFactory.for_topic("default/reliable_topic")
    ↓
get_or_create_wal("default/reliable_topic")
    │
    ├─> ✅ NEW: Check sealed state
    │   EtcdMetadata.get_storage_state_sealed("default/reliable_topic")
    │   Returns: {sealed: true, last_committed_offset: 21, ...}
    │
    ├─> ✅ NEW: Calculate initial offset
    │   initial_offset = 21 + 1 = 22
    │
    ├─> ✅ NEW: Create CheckpointStore (empty on new broker)
    │   wal.ckpt: None
    │   uploader.ckpt: None
    │
    └─> ✅ NEW: Create WAL with initial offset
        Wal::with_config_with_store(cfg, ckpt_store, Some(22))
        └─> next_offset: AtomicU64::new(22)  ← Continues from 22!
        ↓
    Start Uploader
    ├─> Check checkpoint: None (new broker)
    ├─> ✅ Create initial checkpoint from sealed state
    │   uploader_checkpoint: {last_committed_offset: 21, ...}
    └─> Ready to upload from offset 22+
```

**Broker B Initial State:**
```
WAL:
  next_offset: 22        ✅ Continues where A left off
  cache: []              (empty initially)
  
Uploader:
  checkpoint: 21         ✅ From sealed state
  
Subscription "subs_reliable":
  Not loaded yet (no consumer connected)
```

**What just happened?**

This is the **critical fix** that prevents offset collisions:

1. **Sealed State Discovery**: When Broker B loads the topic, it checks ETCD for a sealed state marker
2. **Offset Calculation**: Finding `last_committed_offset: 21`, it calculates `initial_offset = 22`
3. **WAL Initialization**: The WAL is created with `next_offset = 22` instead of the default `0`
4. **Uploader Restoration**: The uploader checkpoint is set to 21, so it knows messages 0-21 are already in cloud storage

**Without this fix**, Broker B would initialize `next_offset = 0`, causing new messages to collide with old offsets and leading to data corruption and message loss.

**With the fix**, offset 22 is the first message produced on Broker B, maintaining a continuous sequence: `[..., 19, 20, 21] (Broker A) → [22, 23, 24, ...] (Broker B)`

### Step 3: Producer Reconnects and Sends Messages

**Producer automatic reconnection**: The Danube client detects the topic has moved and automatically reconnects to Broker B

```
Producer connects to Broker B
    ↓
Sends 6 new messages (message IDs 2-7 in producer)
    ↓
Broker B assigns offsets:
    WAL.append(msg_2) → offset 22 ✅
    WAL.append(msg_3) → offset 23 ✅
    WAL.append(msg_4) → offset 24 ✅
    WAL.append(msg_5) → offset 25 ✅
    WAL.append(msg_6) → offset 26 ✅
    WAL.append(msg_7) → offset 27 ✅
    ↓
Messages stored:
    ├─> WAL cache: [22, 23, 24, 25, 26, 27]
    ├─> WAL file: ./danube-data/wal/default/reliable_topic/wal.log
    └─> Broadcast: (for live consumers)
    ↓
Uploader runs (30s later):
    ├─> Stream offsets 22-27 from WAL files
    ├─> Upload to cloud: data-22-27.dnb1
    └─> Write to ETCD:
        • objects/00000000000000000022:
          {start_offset: 22, end_offset: 27, ...}
```

**State After Production:**
```
Cloud Storage:
  data-0-21.dnb1    (from Broker A)
  data-22-27.dnb1   (from Broker B) ✅ Continuous!

ETCD Objects:
  /storage/.../objects/00000000000000000000: {start: 0, end: 21}
  /storage/.../objects/00000000000000000022: {start: 22, end: 27} ✅

WAL on Broker B:
  next_offset: 28
  cache: [22, 23, 24, 25, 26, 27]
```

**What just happened?**

The producer has no awareness that the topic moved—from its perspective, it's just producing messages as normal:

1. **Client Routing**: The Danube client queries ETCD for the topic's current broker assignment and connects to Broker B
2. **Continuous Offsets**: Messages are assigned offsets 22-27, which is exactly what we want—a continuation from Broker A's 0-21
3. **Cloud Upload**: After 30 seconds (or when the segment is large enough), the uploader streams offsets 22-27 to cloud storage

The offset space is now continuous across both brokers: `[0..21] (Broker A) + [22..27] (Broker B)`. This is the foundation for consumers to work correctly.

### Step 4: Consumer Reconnects

**Consumer automatic reconnection**: The Danube client detects the topic has moved and reconnects to Broker B with the same subscription

```
Consumer subscribes to "subs_reliable"
    ↓
SubscriptionEngine.init_stream_from_progress_or_latest()
    ├─> Read cursor from ETCD: 13
    └─> start_offset = 14 (next unacked message)
    ↓
WalStorage.create_reader(start_offset=14)
    │
    ├─> Get WAL checkpoint on Broker B:
    │   wal_start_offset = 22 (WAL only has 22+)
    │
    ├─> Compare: 14 < 22 (requested offset is OLDER than WAL)
    │   
    └─> ✅ Tiered Reading Strategy:
        │
        ├─> Step 1: Read from CLOUD (14-21)
        │   CloudReader.read_range(14, 21)
        │   ├─> Query ETCD: get objects covering 14-21
        │   │   Returns: objects/00000000000000000000 (0-21)
        │   ├─> Download: data-0-21.dnb1
        │   ├─> Extract frames: offsets 14-21
        │   └─> Stream: [msg_14, msg_15, ..., msg_21]
        │
        └─> Step 2: Chain to WAL (22+)
            WAL.tail_reader(22, false)
            ├─> Read from cache: [22, 23, 24, 25, 26, 27]
            └─> Stream: [msg_22, msg_23, ..., msg_27]
            └─> Then switch to live broadcast for future messages
    ↓
Consumer receives continuous stream:
    [14, 15, 16, 17, 18, 19, 20, 21] ← from cloud
    [22, 23, 24, 25, 26, 27]         ← from WAL
    ✅ No gaps, no duplicates!
```

**What just happened?**

This is where the magic of **tiered reading** shines:

1. **Cursor Resume**: The consumer's subscription cursor was at 13 (last ACKed message), so it resumes from offset 14
2. **Storage Decision**: The consumer asks for offset 14, but Broker B's WAL only has offsets 22+. The system detects this gap.
3. **Cloud Fallback**: For offsets 14-21, the reader automatically fetches data from cloud storage (uploaded by Broker A)
4. **WAL Handoff**: Once caught up to offset 22, the reader seamlessly switches to reading from Broker B's local WAL
5. **Continuous Stream**: From the consumer's perspective, it's a single continuous stream of messages—it has no idea the topic moved!

This works **only because offsets are continuous**: there's no collision between Broker A's offsets (0-21) and Broker B's offsets (22+).

### Step 5: Consumer ACKs and Cursor Updates

```
Consumer processes and ACKs messages:
    ACK(14) → ACK(15) → ... → ACK(27)
    ↓
SubscriptionEngine.on_acked(offset)
    ├─> Update in-memory cursor: 27
    └─> Batch flush to ETCD (every 1000 acks or 5s):
        • /topics/.../subscriptions/subs_reliable/cursor: 27
```

---

## Key Mechanisms Ensuring Continuity

### 1. Offset Continuity via Sealed State
```
Broker A: Messages 0-21
    ↓ (sealed state: last=21)
Broker B: Messages 22+ ✅ Continuous!
```

### 2. Consumer Read Strategy (Tiered)
```
Consumer wants offset X
    ↓
Is X in WAL range?
    YES → Read from WAL only
    NO  → Read from Cloud, then chain to WAL
```

### 3. Uploader State Preservation
```
Old Broker:
  Uploader checkpoint: 21
    ↓ (written to sealed state)
New Broker:
  Uploader checkpoint: 21 (from sealed state)
  Won't re-upload 0-21 ✅
  Will upload 22+ ✅
```

### 4. Subscription Cursor Independence
```
Subscription cursor in ETCD: 13
    ↓ (persisted independently)
Consumer reads from 14 regardless of which broker
    ✅ Works because offset space is continuous
```

---

## Complete Timeline Example

| Time | Event | Broker A | Broker B | Cloud | Consumer |
|------|-------|----------|----------|-------|----------|
| T1 | Produce msgs | Offsets 0-21 | - | - | - |
| T2 | Upload | - | - | 0-21 stored | - |
| T3 | Consumer reads | - | - | - | Reads 0-13, cursor=13 |
| T4 | **UNLOAD** | Seal state (21) | - | State saved | - |
| T5 | Assign to B | - | Load, WAL=22 ✅ | - | - |
| T6 | Produce msgs | - | Offsets 22-27 ✅ | - | - |
| T7 | Upload | - | - | 22-27 stored | - |
| T8 | Consumer reconnect | - | - | - | Reads 14-21 (cloud) |
| T9 | Continue read | - | - | - | Reads 22-27 (WAL) ✅ |
| T10 | ACK all | - | - | - | Cursor=27 ✅ |


---

## Conclusion

The reliable topic workflow is designed to be **broker-agnostic**: a topic can be moved between brokers without any impact on producers or consumers, as long as offset continuity is preserved.

The **sealed state mechanism** is the linchpin that enables this:
- Old broker captures `last_committed_offset` when unloading
- New broker reads this state and initializes its WAL from `last_committed_offset + 1`
- Offset space remains continuous across the move
- Consumers read seamlessly from cloud (old messages) and WAL (new messages)

This foundation enables both:
- **Current**: Manual topic moves for maintenance and troubleshooting
- **Future**: Automated load balancing for dynamic, self-healing clusters

The architecture is built to handle topic mobility at scale, making Danube suitable for cloud-native deployments where resources are constantly shifting.
