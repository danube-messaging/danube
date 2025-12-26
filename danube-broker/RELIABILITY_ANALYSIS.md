# Danube Reliable Dispatch - Architecture Analysis & Recommendations

**Date**: December 26, 2025  
**Scope**: Reliable message delivery mechanism analysis  
**Status**: Identified gaps and proposing improvements

---

## Executive Summary

The current reliable dispatch mechanism has a **critical dependency on notifiers** that could lead to:
1. **Missed messages** during subscription creation race conditions
2. **Silent failures** if notifier tasks die or miss notifications
3. **No gap detection** - offsets could be skipped without detection

**Recommendation**: Implement **offset-based verification** in SubscriptionEngine to complement (not replace) the notifier mechanism.

---

## Current Architecture

### Message Flow (Reliable Mode)

```text
┌─────────────────────────────────────────────────────────────────────┐
│                       CURRENT RELIABLE FLOW                          │
└─────────────────────────────────────────────────────────────────────┘

Producer                  WAL                   Notifier           Dispatcher
   │                       │                       │                   │
   │  1. publish_message   │                       │                   │
   ├──────────────────────▶│                       │                   │
   │                       │  2. fetch_add(1)      │                   │
   │                       │     offset = 100      │                   │
   │                       │  3. stamp message     │                   │
   │                       │     msg.offset = 100  │                   │
   │                       │  4. append to disk    │                   │
   │                       │                       │                   │
   │                       │  5. notify_one() ────▶│                   │
   │                       │                       │  PollAndDispatch  │
   │                       │                       ├──────────────────▶│
   │                       │                       │                   │
   │                       │                       │                   │  6. poll_next()
   │                       │  ◄────────────────────┼───────────────────┤
   │                       │  7. read offset 100   │                   │
   │                       ├───────────────────────┼──────────────────▶│
   │                       │                       │                   │  8. send to consumer
   │                       │                       │                   │  9. wait for ack
   ▼                       ▼                       ▼                   ▼

ISSUE: Entirely depends on notifier firing. No fallback mechanism.
```

### Key Components

#### 1. WalStorage (`danube-persistent-storage/src/wal.rs`)
```rust
pub async fn append(&self, msg: &StreamMessage) -> Result<u64> {
    let offset = self.inner.next_offset.fetch_add(1, Ordering::AcqRel); // ← Atomic increment
    let mut stamped = msg.clone();
    stamped.msg_id.topic_offset = offset;  // ← Stamp message with offset
    // ... write to disk ...
    // ... broadcast to live readers (optional optimization) ...
}
```

**Offsets are monotonically increasing** - no gaps should exist in the WAL.

#### 2. Topic Publish (`danube-broker/src/topic.rs`)
```rust
DispatchStrategy::Reliable => {
    store.store_message(stream_message).await?;  // ← Append to WAL
    
    let notifier_guard = self.notifiers.lock().await;
    for notifier in notifier_guard.iter() {
        notifier.notify_one();  // ← Notify all subscriptions
    }
}
```

**All subscriptions are notified** after WAL write succeeds.

#### 3. SubscriptionEngine (`danube-broker/src/dispatcher/subscription_engine.rs`)
```rust
pub(crate) struct SubscriptionEngine {
    pub(crate) topic_store: Arc<TopicStore>,
    pub(crate) stream: Option<TopicStream>,       // ← Reads from WAL
    pub(crate) last_acked: Option<u64>,           // ← Tracks acks
    // ... no tracking of last_dispatched or expected_offset ...
}

pub(crate) async fn poll_next(&mut self) -> Result<Option<StreamMessage>> {
    let stream = match &mut self.stream { /* ... */ };
    stream.next().await  // ← Just reads next from stream, no verification
}
```

**No offset verification** - assumes stream is always correct.

#### 4. UnifiedSingleDispatcher (`danube-broker/src/dispatcher/unified_single.rs`)
```rust
let mut pending = false;
let mut pending_message: Option<StreamMessage> = None;

loop {
    match cmd {
        DispatcherCommand::PollAndDispatch => {
            if pending { continue; }  // ← Ack-gating
            
            // Poll next message from engine
            let msg = engine.lock().await.poll_next().await?;
            
            if let Some(msg) = msg {
                pending_message = Some(msg.clone());  // ← Buffer for retry
                cons.send_message(msg).await?;
                pending = true;  // ← Wait for ack
            }
        }
        DispatcherCommand::MessageAcked(ack) => {
            engine.lock().await.on_acked(ack.msg_id).await?;  // ← Track progress
            pending = false;  // ← Allow next dispatch
            // Immediately trigger next poll
            tx.send(DispatcherCommand::PollAndDispatch).await;
        }
    }
}
```

**Relies entirely on PollAndDispatch commands** (triggered by notifiers).

---

## Identified Vulnerabilities

### 🔴 Critical Issue #1: Subscription Creation Race Condition

**Scenario**: Message published during subscription creation, before notifier is registered.

```text
Timeline:
T0: Subscription created, TopicStream initialized at offset=100
T1: create_new_dispatcher() creates SubscriptionEngine
T2: SubscriptionEngine.init_stream_from_progress_or_latest()
     → creates reader starting at offset=100 (Latest)
T3: >>> RACE WINDOW BEGINS <<<
T4: Producer publishes message, WAL assigns offset=100
T5: WAL.append() succeeds, message written to disk
T6: Topic.notifiers.lock().await → EMPTY!
T7: No notification sent (notifier not registered yet)
T8: >>> RACE WINDOW ENDS <<<
T9: create_new_dispatcher() returns notifier
T10: Topic.notifiers.push(notifier)
T11: Dispatcher never wakes up to poll offset=100

RESULT: Message at offset=100 is in WAL but never dispatched.
```

**Current code** (`subscription.rs:168-174`):
```rust
let new_dispatcher = UnifiedSingleDispatcher::new_reliable(engine);
new_dispatcher.ready().await;  // ← Stream initialized
let notifier = new_dispatcher.get_notifier();  // ← Notifier created
// Race: messages published here won't be notified
return (Dispatcher::UnifiedOneConsumer(new_dispatcher), Some(notifier));
```

**Where it gets registered** (`topic.rs` - assumed flow):
```rust
if let Some(notifier) = subscription.create_new_dispatcher(...).await? {
    // Messages can be published between create and push
    self.notifiers.lock().await.push(notifier);  // ← Registration happens AFTER return
}
```

### 🔴 Critical Issue #2: Notifier Task Death

**Scenario**: Notifier task panics or is cancelled.

The notifier spawns an infinite loop:
```rust
// From unified_single.rs:285-290
pub(crate) fn get_notifier(&self) -> Arc<Notify> {
    let notify = Arc::new(Notify::new());
    let tx = self.control_tx.clone();
    let n = notify.clone();
    tokio::spawn(async move {  // ← Task could panic or be cancelled
        loop {
            n.notified().await;
            let _ = tx.send(DispatcherCommand::PollAndDispatch).await;
        }
    });
    notify
}
```

**If this task dies**:
- Future notifications are silently dropped
- Dispatcher never receives PollAndDispatch
- Messages accumulate in WAL but are never dispatched
- **No monitoring or recovery mechanism**

### 🟡 Medium Issue #3: No Offset Gap Detection

**Current code doesn't verify offset continuity**:

```rust
// SubscriptionEngine.poll_next() - NO VERIFICATION
pub(crate) async fn poll_next(&mut self) -> Result<Option<StreamMessage>> {
    let stream = &mut self.stream.as_mut()?;
    let msg_opt = stream.next().await.transpose()?;
    // MISSING: Check if msg.offset == expected_next_offset
    return Ok(msg_opt);
}
```

**If the stream returns**:
- Offset 100 ✓
- Offset 101 ✓
- Offset 105 ⚠️ **GAP! Offsets 102-104 missing**

The dispatcher would dispatch offset 105 without detecting the gap.

### 🟡 Medium Issue #4: poll_next() Can Return None

**From `subscription_engine.rs:99-125`**:
```rust
pub(crate) async fn poll_next(&mut self) -> Result<Option<StreamMessage>> {
    let stream = match &mut self.stream {
        Some(s) => s,
        None => return Ok(None),  // ← Returns None if stream not initialized
    };
    
    let msg_opt = stream.next().await.transpose()?;  // ← Can return None
    // ... rate limiter logic ...
    return Ok(msg_opt);
}
```

**When does stream.next() return None?**
- Stream exhausted (no more messages currently available)
- Stream encounterserror and terminates
- WAL reader reaches end of file before new message is appended

**Current dispatcher behavior**:
```rust
// From unified_single.rs:224-231
let msg_to_send = match engine.lock().await.poll_next().await {
    Ok(msg_opt) => msg_opt,  // ← Could be None
    Err(e) => {
        warn!("poll_next error: {}", e);
        None
    }
};
// If None, dispatcher just waits for next PollAndDispatch
// If notifier didn't fire, message is missed
```

### 🟢 Low Issue #5: Consumer Reconnect Timing

When consumer reconnects, `trigger_dispatcher_on_reconnect()` notifies dispatchers:

```rust
// topic_control.rs:464-474
dispatcher.reset_pending().await?;  // ← Clear pending flag

let notifier_guard = topic.notifiers.lock().await;
for notifier in notifier_guard.iter() {
    notifier.notify_one();  // ← Wake up dispatcher
}
```

**Potential issue**: If a message was written to WAL during disconnect, it might not be in `pending_message`. The dispatcher polls from the stream, but if the stream is at the wrong offset, it could miss messages.

---

## Root Cause Analysis

### Why Notifier-Only Is Insufficient

The current design has a **single point of failure**:

```text
Messages are ONLY dispatched when:
  1. Notifier.notify_one() is called, AND
  2. Notifier task is alive and sends PollAndDispatch, AND
  3. Dispatcher receives PollAndDispatch, AND
  4. poll_next() returns a message (not None)

If ANY of these fail → message is missed.
```

**No fallback mechanism** exists to:
- Detect if a notification was missed
- Verify offset continuity
- Self-heal by polling periodically
- Detect if notifier task died

---

## Proposed Solution: Offset-Based Verification

### Design Principles

1. **Keep notifiers** - they provide low-latency dispatch for the hot path
2. **Add offset verification** - detect gaps and missed notifications
3. **Add periodic polling** - fallback mechanism if notifier fails
4. **Track expected_next_offset** - verify stream continuity
5. **Add monitoring** - detect anomalies in dispatch

### Enhanced SubscriptionEngine

```rust
pub(crate) struct SubscriptionEngine {
    pub(crate) topic_store: Arc<TopicStore>,
    pub(crate) stream: Option<TopicStream>,
    pub(crate) last_acked: Option<u64>,
    pub(crate) dirty: bool,
    pub(crate) last_flush_at: Instant,
    
    // NEW: Offset tracking for gap detection
    pub(crate) expected_next_offset: Option<u64>,  // ← What offset we expect next
    pub(crate) last_dispatched: Option<u64>,       // ← Last offset we dispatched
    
    // NEW: Periodic polling for fallback
    pub(crate) last_poll_at: Instant,              // ← When we last polled
    pub(crate) poll_interval: Duration,            // ← How often to poll (e.g., 100ms)
    
    // NEW: Gap detection metrics
    pub(crate) gap_count: u64,                     // ← Number of detected gaps
    pub(crate) notification_miss_count: u64,       // ← Estimated missed notifications
}
```

### Enhanced poll_next() with Offset Verification

```rust
/// Polls next message from the stream with offset verification.
/// Returns Err if a gap is detected (reliability violation).
pub(crate) async fn poll_next(&mut self) -> Result<Option<StreamMessage>> {
    let stream = match &mut self.stream {
        Some(s) => s,
        None => return Ok(None),
    };
    
    self.last_poll_at = Instant::now();  // ← Track polling activity
    
    let msg_opt = stream.next().await.transpose()?;
    
    if let Some(msg) = &msg_opt {
        let current_offset = msg.msg_id.topic_offset;
        
        // Offset verification: check for gaps
        if let Some(expected) = self.expected_next_offset {
            if current_offset != expected {
                // GAP DETECTED!
                self.gap_count += 1;
                warn!(
                    subscription = %self._subscription_name,
                    expected = expected,
                    received = current_offset,
                    gap_size = current_offset.saturating_sub(expected),
                    "RELIABILITY VIOLATION: Offset gap detected in stream"
                );
                
                // CRITICAL: Should we return error or try to recover?
                // Option A: Return error and let dispatcher handle
                return Err(anyhow!(
                    "Offset gap detected: expected {}, got {}",
                    expected,
                    current_offset
                ));
                
                // Option B: Log and continue (at-least-once becomes "best-effort")
                // (Not recommended for reliable mode)
            }
        }
        
        // Update tracking
        self.last_dispatched = Some(current_offset);
        self.expected_next_offset = Some(current_offset + 1);  // ← Expect next sequential
        
        // Rate limiter check (existing code)
        if let Some(lim) = &self.dispatch_rate_limiter {
            if !lim.try_acquire(1).await {
                // ... backoff logic ...
            }
        }
    }
    
    Ok(msg_opt)
}
```

### Enhanced init_stream with Initial Offset Tracking

```rust
pub(crate) async fn init_stream_from_progress_or_latest(&mut self) -> Result<()> {
    let start_offset = if let (Some(topic), Some(res_mx)) =
        (self.topic_name.as_deref(), self.progress_resources.as_ref())
    {
        let mut res = res_mx.lock().await;
        if let Ok(Some(cursor)) = res
            .get_subscription_cursor(&self._subscription_name, topic)
            .await
        {
            let next_offset = cursor.saturating_add(1);
            info!(
                subscription = %self._subscription_name,
                resume_from = next_offset,
                "Resuming from persisted cursor"
            );
            next_offset
        } else {
            // New subscription, start from latest
            let latest = self.topic_store.storage.wal.current_offset();
            info!(
                subscription = %self._subscription_name,
                start_from = latest,
                "New subscription, starting from latest offset"
            );
            latest
        }
    } else {
        self.topic_store.storage.wal.current_offset()
    };
    
    // Initialize offset tracking
    self.expected_next_offset = Some(start_offset);  // ← Set expected offset
    self.last_dispatched = None;
    self.last_poll_at = Instant::now();
    
    // Create stream
    let stream = self
        .topic_store
        .create_reader(StartPosition::Offset(start_offset))
        .await?;
    self.stream = Some(stream);
    
    Ok(())
}
```

### Periodic Polling Fallback

Add a periodic polling mechanism that doesn't rely on notifiers:

```rust
// In UnifiedSingleDispatcher::new_reliable()
tokio::spawn(async move {
    let mut consumers: Vec<Consumer> = Vec::new();
    let mut pending = false;
    let mut pending_message: Option<StreamMessage> = None;
    
    // NEW: Periodic poll interval (independent of notifier)
    let poll_interval = Duration::from_millis(100);  // ← Configurable
    let mut poll_timer = tokio::time::interval(poll_interval);
    poll_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    loop {
        tokio::select! {
            cmd_result = control_rx.recv() => {
                // ... existing command handling ...
            }
            
            // NEW: Periodic polling fallback
            _ = poll_timer.tick() => {
                if !pending && active_consumer.is_some() {
                    // Check if we haven't polled recently
                    let engine_guard = engine.lock().await;
                    let elapsed = engine_guard.last_poll_at.elapsed();
                    drop(engine_guard);
                    
                    if elapsed > Duration::from_millis(200) {
                        // Notifier might have missed, trigger poll
                        warn!(
                            subscription = %subscription_name,
                            elapsed_ms = elapsed.as_millis(),
                            "Periodic fallback poll triggered (possible missed notification)"
                        );
                        let _ = reliable_tx_for_task
                            .send(DispatcherCommand::PollAndDispatch)
                            .await;
                    }
                }
            }
        }
    }
});
```

### Notifier Health Monitoring

Add monitoring to detect if notifier task dies:

```rust
pub(crate) fn get_notifier(&self) -> Arc<Notify> {
    let notify = Arc::new(Notify::new());
    let tx = self.control_tx.clone();
    let n = notify.clone();
    let subscription_name = self.subscription_name.clone();  // For logging
    
    tokio::spawn(async move {
        let mut notification_count = 0u64;
        let mut last_log = Instant::now();
        
        loop {
            n.notified().await;
            notification_count += 1;
            
            // Send PollAndDispatch
            if let Err(e) = tx.send(DispatcherCommand::PollAndDispatch).await {
                error!(
                    subscription = %subscription_name,
                    notification_count = notification_count,
                    "Notifier task: control channel closed, exiting: {}",
                    e
                );
                // Metric: notifier_task_deaths
                counter!("danube_notifier_task_deaths_total",
                    "subscription" => subscription_name.clone()
                ).increment(1);
                break;
            }
            
            // Periodic health logging
            if last_log.elapsed() > Duration::from_secs(60) {
                trace!(
                    subscription = %subscription_name,
                    notification_count = notification_count,
                    "Notifier task health check: alive"
                );
                last_log = Instant::now();
            }
        }
        
        warn!(
            subscription = %subscription_name,
            "Notifier task exited after {} notifications",
            notification_count
        );
    });
    
    notify
}
```

---

## Proposed Implementation Plan

### Phase 1: Add Offset Tracking (Low Risk)
**Timeline**: 1-2 days

1. Add `expected_next_offset` and `last_dispatched` to `SubscriptionEngine`
2. Update `init_stream_from_progress_or_latest()` to set initial expected offset
3. Update `poll_next()` to track offsets (log warnings on gaps, don't fail yet)
4. Add metrics:
   - `danube_subscription_offset_gaps_total`
   - `danube_subscription_last_dispatched_offset` (gauge)
5. Monitor logs in production for any gap warnings

**Expected outcome**: Visibility into whether gaps occur in practice

### Phase 2: Add Gap Detection (Medium Risk)
**Timeline**: 2-3 days

1. Make `poll_next()` return error on gap detection
2. Update dispatcher to handle gap errors:
   - Log critical error
   - Reset stream to expected offset
   - Retry from correct position
3. Add recovery mechanism:
   ```rust
   if let Err(e) = poll_next().await {
       if e.to_string().contains("Offset gap") {
           // Reset stream to last_acked + 1
           let resume_offset = last_acked.unwrap_or(0) + 1;
           engine.reset_stream_to_offset(resume_offset).await?;
       }
   }
   ```
4. Test gap recovery with integration tests

**Expected outcome**: System can detect and recover from offset gaps

### Phase 3: Add Periodic Polling Fallback (Low Risk)
**Timeline**: 1 day

1. Add `poll_interval` configuration to `SubscriptionEngine`
2. Add `last_poll_at` tracking
3. Update dispatcher with periodic polling fallback (tokio::select!)
4. Add metric: `danube_subscription_fallback_polls_total`
5. Monitor if fallback polls are needed (indicates notifier issues)

**Expected outcome**: System doesn't rely entirely on notifiers

### Phase 4: Fix Subscription Creation Race (Medium Risk)
**Timeline**: 2 days

1. Change `create_new_dispatcher()` to return dispatcher and notifier separately
2. Add `register_notifier()` method that:
   - Adds notifier to Topic.notifiers
   - Immediately triggers `notify_one()` to catch any missed messages
3. Update subscription creation flow:
   ```rust
   let (dispatcher, notifier) = subscription.create_new_dispatcher(...).await?;
   
   // Register notifier and trigger initial poll
   if let Some(n) = notifier {
       topic.register_notifier(n).await;  // ← Adds and triggers immediately
   }
   ```
4. Add metric: `danube_subscription_creation_races_detected_total`

**Expected outcome**: No messages missed during subscription creation

### Phase 5: Add Notifier Health Monitoring (Low Risk)
**Timeline**: 1 day

1. Update `get_notifier()` with health monitoring code
2. Add metrics:
   - `danube_notifier_task_deaths_total`
   - `danube_notifier_notifications_total`
3. Add alerting if notifier task dies

**Expected outcome**: Visibility into notifier task health

---

## Alternative Approaches Considered

### Alternative 1: Remove Notifiers Entirely
**Approach**: Use only periodic polling (e.g., every 10ms)

**Pros**:
- Simpler architecture
- No race conditions
- No notifier task death issues

**Cons**:
- Higher latency (minimum 10ms)
- Higher CPU usage (constant polling)
- Wastes CPU when idle

**Verdict**: ❌ Not recommended. Notifiers provide low-latency dispatch.

### Alternative 2: WAL Directly Notifies Dispatchers
**Approach**: WAL maintains list of subscription dispatchers and notifies them directly

**Pros**:
- No Topic.notifiers intermediate layer
- Tighter coupling might reduce race conditions

**Cons**:
- WAL shouldn't know about dispatchers (layering violation)
- Harder to test WAL in isolation
- Doesn't solve gap detection or notifier task death

**Verdict**: ❌ Not recommended. Architectural complexity increases.

### Alternative 3: At-Most-Once with Monitoring
**Approach**: Accept that messages might be missed, add extensive monitoring

**Pros**:
- Simplest code
- Fastest performance

**Cons**:
- **Violates reliability guarantee**
- Not acceptable for reliable dispatch mode
- Users expect at-least-once delivery

**Verdict**: ❌ Not acceptable. Reliability is a core feature.

---

## Success Metrics

### Reliability Metrics (Critical)
- **Zero offset gaps detected** in production
- **Zero notifier task deaths** without recovery
- **100% message delivery** from WAL to consumers (when consumers are active)

### Performance Metrics (Important)
- **P99 dispatch latency < 5ms** (notifier path should remain fast)
- **Fallback poll rate < 1%** of total dispatches (notifiers should work)
- **CPU overhead < 1%** from periodic polling

### Operational Metrics (Monitoring)
- `danube_subscription_offset_gaps_total` - should be 0
- `danube_subscription_fallback_polls_total` - should be low
- `danube_notifier_task_deaths_total` - should be 0
- `danube_subscription_creation_races_detected_total` - track occurrences

---

## Testing Strategy

### Unit Tests
1. **Offset gap detection**: Inject messages with skipped offsets
2. **Notifier task death**: Cancel notifier task, verify fallback works
3. **Subscription creation race**: Publish during creation, verify no loss
4. **Stream returns None**: Mock stream to return None, verify retry

### Integration Tests
1. **High throughput**: Publish 10K msgs/sec, verify no gaps
2. **Consumer churn**: Connect/disconnect during publish, verify reliability
3. **Notifier failure**: Kill notifier task, verify fallback recovers
4. **WAL rotation**: Trigger WAL rotation during dispatch, verify continuity

### Chaos Tests
1. **Random notifier drops**: Randomly drop notify_one() calls, verify fallback
2. **Random consumer disconnects**: Disconnect during dispatch, verify recovery
3. **Network partitions**: Simulate broker network partition, verify catch-up

---

## Conclusion

The current notifier-based mechanism is **insufficient for guaranteed reliable delivery** due to:

1. **Race conditions** during subscription creation
2. **Single point of failure** (notifier task death)
3. **No offset verification** (gaps undetected)
4. **No fallback mechanism** if notifiers fail

**Recommended approach**:
- ✅ **Keep notifiers** for low-latency hot path
- ✅ **Add offset tracking** to detect gaps
- ✅ **Add periodic polling** as fallback
- ✅ **Add monitoring** to detect anomalies

This provides **defense in depth** - multiple layers of protection ensure reliability even if individual components fail.

**Complexity trade-off**:
- Adds ~200 lines of code
- Adds 3-4 new configuration parameters
- Provides **guaranteed reliability** for hundreds of subscriptions

The additional complexity is **justified** given that reliability is a core feature of Danube's value proposition.

---

## Open Questions for Discussion

1. **Gap detection behavior**: Should we fail-fast on gaps, or try to recover?
2. **Periodic poll interval**: 100ms? Configurable per subscription?
3. **Notifier task death**: Should we automatically recreate the task?
4. **Offset verification**: Should it be optional (for testing/debugging)?
5. **Performance impact**: What is acceptable overhead for reliability?

---

**Next Steps**: Review this analysis, discuss alternatives, and prioritize implementation phases based on risk and customer impact.
