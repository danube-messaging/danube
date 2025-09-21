# Danube Dispatcher Architecture Migration Strategy

## Overview

This document outlines a phased migration strategy to simplify the Danube dispatcher architecture while focusing on a cleaner, easier-to-implement API. Backward compatibility is NOT a requirement for this migration. We will preserve and streamline the two dispatch strategies and the subscription model:
- Non-Reliable Dispatch (fire-and-forget fan-out; no durable storage path)
- Reliable Dispatch (store-then-forward via WAL + Cloud persistence)

Reference subscription model: https://danube-docs.dev-state.com/architecture/subscriptions/

**Key Change**: This strategy eliminates the `danube-reliable-dispatch` crate entirely, consolidating all dispatch logic within `danube-broker`.

### Current Issues
1. **Unnecessary Crate Separation**: `danube-reliable-dispatch` adds complexity without clear benefits
2. **Dual Dispatch Layers**: `SubscriptionDispatch` + `DispatcherReliable*` doing overlapping work
3. **Code Duplication**: Similar patterns across reliable single/multiple dispatchers
4. **Complex Message Flow**: Multiple async boundaries and command passing
5. **Artificial Separation**: Reliable vs non-reliable handled at enum level

## Target Architecture

### Consolidated Broker Architecture
```rust
// danube-broker/src/topic.rs - Consolidated TopicStore
pub(crate) struct TopicStore {
    topic_name: String,
    persistent: WalStorage,
}

impl TopicStore {
    pub(crate) async fn store_message(&self, message: StreamMessage) -> Result<u64> { /* append to WAL; return offset */ }
    pub(crate) async fn create_reader(&self, start: StartPosition) -> Result<TopicStream> { /* WAL tail or CloudReader via WalStorage */ }
}

// danube-broker/src/dispatcher/mod.rs - Unified Dispatchers
pub(crate) enum DispatchMode {
    Reliable,
    NonReliable,
}

pub(crate) enum Dispatcher {
    Single(UnifiedSingleDispatcher),
    Multiple(UnifiedMultipleDispatcher),
}

pub(crate) struct UnifiedSingleDispatcher {
    mode: DispatchMode,
    reliable: Option<SubscriptionEngine>, // present when mode == Reliable
    consumers: Vec<Consumer>,
}

pub(crate) struct UnifiedMultipleDispatcher {
    mode: DispatchMode,
    reliable: Option<SubscriptionEngine>,
    consumers: Vec<Consumer>,
    round_robin_index: AtomicUsize,
}
```

### Reliable Subscription Engine
```rust
// danube-broker/src/dispatcher/subscription_engine.rs
// Encapsulates reliable delivery mechanics (retries, ack tracking, progress updates)
pub(crate) struct SubscriptionEngine {
    topic_store: Arc<TopicStore>,
    stream: Option<TopicStream>,
    pending_ack: Option<(u64, MessageID)>,
    retry_count: u8,
    last_retry_at: Option<Instant>,
}

impl SubscriptionEngine {
    // Polls next message from TopicStream; manages WAL/cloud handoff implicitly via TopicStore
    pub(crate) async fn poll_next(&mut self) -> Result<StreamMessage> { /* ... */ }

    // Called when a message is acked by a consumer
    pub(crate) async fn on_acked(&mut self, request_id: u64, msg_id: MessageID) -> Result<Option<StreamMessage>> { /* ... */ }
}
```

Notes:
- Non-Reliable mode bypasses `TopicStore` for read path and simply fans out inbound messages to connected consumers with minimal buffering. Writes MAY still go to WAL if the topic is configured as reliable; for non-reliable topics we keep messages in-memory only.
- Reliable mode uses `TopicStore` which in turn uses the WAL tail and CloudReader handoff per `info/WAL_Cloud_Persistence_Design.md`.
- Subscription start policy aligns with the WAL design:
  - New subscription: `StartPosition::Latest`.
  - Existing subscription: resume from last committed offset (At-Least-Once).
  - This policy can be made configurable if needed.

### Decisions Confirmed
- Non-Reliable persistence remains fire-and-forget from the producer perspective; only currently active subscribers receive messages. No WAL writes and no replay for non-reliable topics.
- Start position default for brand-new subscriptions is `Latest`.
- Reliable dispatch is strictly ack-gated: the dispatcher delivers at most one in-flight message per reliable subscription and only fetches/sends the next after receiving an explicit consumer acknowledgment for the previous message.

## Migration Phases

### Phase 1: Consolidate Within Broker (No Feature Flags)
Goal: Move all reliable dispatch logic into `danube-broker` and introduce unified dispatcher types. API changes are allowed.

Tasks:
1. Move `TopicStore` to `danube-broker/src/topic.rs` and wire to `danube-persistent-storage` WAL/Cloud components.  — DONE
2. Create `SubscriptionEngine` in `danube-broker/src/dispatcher/subscription_engine.rs` (logic extracted from `SubscriptionDispatch`). — DONE
3. Implement `UnifiedSingleDispatcher` and `UnifiedMultipleDispatcher` with `DispatchMode::{Reliable, NonReliable}`. — DONE
4. Replace existing dispatcher instantiation in `danube-broker/src/subscription.rs` to use unified types directly (no flags). — DONE
5. Update `danube-broker/Cargo.toml` to depend directly on `danube-persistent-storage`. — DONE

Files to Create/Modify:
- Create: `danube-broker/src/dispatcher/subscription_engine.rs`
- Create: `danube-broker/src/dispatcher/unified_single.rs`
- Create: `danube-broker/src/dispatcher/unified_multiple.rs`
- Update: `danube-broker/src/dispatcher/mod.rs`
- Update: `danube-broker/src/topic.rs`
- Update: `danube-broker/Cargo.toml`

Testing Strategy:
- Unit tests (in-module, colocated with the code under test):
  - `danube-broker/src/dispatcher/subscription_engine_test.rs`
    - Mock `TopicStoreLike` to drive sequences; verify `poll_next()`, `on_acked()` behavior.
    - Cover start policies: `init_stream_latest()` default and a helper for `Offset(0)` for tests.
  - `danube-broker/src/dispatcher/unified_single_test.rs`
    - Non-reliable: dispatch to active consumer only; verify no acks needed, skips unhealthy.
    - Reliable: strict ack-gating (one in flight); next only after `ack_message(request_id, msg_id)`; uses `get_notifier()` to trigger reads after WAL append.
  - `danube-broker/src/dispatcher/unified_multiple_test.rs`
    - Non-reliable: round-robin delivery across healthy consumers; skips unhealthy.
    - Reliable: round-robin target selection with strict ack-gating across the subscription.
- Integration tests (under `danube-broker/tests/`):
  - `topic_store_wal.rs`
    - Store-and-read via `TopicStore` with `StartPosition::Offset(n)`.
    - Tailing from `StartPosition::Latest` receives only post-subscription messages.
  - `unified_dispatch_reliable.rs`
    - End-to-end append via `TopicStore.store_message()` then notify -> dispatcher delivers, requires ack to proceed.
    - Multiple consumers: round-robin plus ack-gating validation.
  - `unified_dispatch_non_reliable.rs`
    - Active-only fan-out; late joiners do not replay.
  - Subscription lifecycle: add/remove/disconnect; brand-new subs start at `Latest` unless test-specific override is used.

Notes on porting legacy tests:
- The following tests from `danube-reliable-dispatch` remain valuable and should be adapted:
  - `src/topic_storage_test.rs` → `danube-broker/tests/topic_store_wal.rs` (use broker `TopicStore` + `PersistentStorage` trait).
  - `src/dispatch_test.rs` → split across `unified_dispatch_reliable.rs` and `unified_dispatch_non_reliable.rs`, replacing `SubscriptionDispatch` with `SubscriptionEngine` + unified dispatchers.
- Start position difference:
  - New default is `StartPosition::Latest` for brand-new subs. Tests that expect replay from the beginning should explicitly initialize the engine from `Offset(0)` (add a test-only helper if needed).

### Phase 2: Remove `danube-reliable-dispatch` and Old Dispatchers
Goal: Hard cut-over to unified architecture and delete the legacy crate and codepaths.

Tasks:
1. Remove `danube-reliable-dispatch` from workspace and from all `Cargo.toml` files. — DONE
2. Delete old dispatcher files in `danube-broker/src/dispatcher/*reliable*` and `*single_consumer*/*multiple_consumers*` where superseded. — PENDING (files still present on disk)
3. Update imports and clean up any adapters or compatibility shims. — DONE
4. Update documentation and examples to the new API (producer/consumer unaffected; broker-internal API changed). — IN PROGRESS

Files to Remove:
- Entire crate: `danube-reliable-dispatch/` — PENDING (directory removal)
- `danube-broker/src/dispatcher/dispatcher_reliable_single_consumer.rs` — PENDING
- `danube-broker/src/dispatcher/dispatcher_reliable_multiple_consumers.rs` — PENDING
- Any legacy adapters tied to segments or old storage — N/A

### Phase 3: Optimization, Observability, and Docs
Goal: Optimize unified dispatchers, finalize metrics, and complete documentation.

Tasks:
1. Performance optimization: lock contention, memory footprint, batching.
2. Metrics: consumer lag, WAL/cloud read latencies, non-reliable fan-out drop/queue metrics.
3. Failure injection tests: cloud outages, WAL corruption, leader change, stalled subscriptions.
4. Documentation:
   - Update architecture docs to reflect unified broker design.
   - Remove references to `danube-reliable-dispatch`.
   - Explicitly describe Non-Reliable vs Reliable behavior and configuration.

## Mode Selection

Dispatcher mode is selected by the client at topic creation time. No broker-side dispatcher configuration section is required in `config/danube_broker.yml`.

Notes:
- In reliable mode, publish path writes to WAL and consumers read from WAL/Cloud via `TopicStore`.
- In non-reliable mode, publish path directly fans out to subscribers; no WAL writes for that topic and only currently active subscribers receive messages (no replay for later joiners).

## Risk Mitigation

Technical risks and mitigations remain similar, with simplifications due to removal of feature flags and legacy code paths:
1. **Performance Regression**: Benchmark unified dispatchers under mixed workloads.
2. **Behavioral Changes**: Re-align tests to the new API; validate subscription semantics against the docs.
3. **Memory Leaks**: Long-running tests with leak detectors.
4. **Race Conditions**: Stress testing with high concurrency.
5. **Dependency Issues**: Single direct dependency (`danube-persistent-storage`) from broker simplifies the graph.

## Success Metrics

- Eliminate entire `danube-reliable-dispatch` crate.
- Reduce dispatcher-related code by ~50%.
- Maintain or improve throughput and tail latency in reliable mode; lowest latency in non-reliable mode.
- 30% memory reduction through consolidated components.
- Cleaner dependency graph and reduced binary size.

## Post-Migration Benefits

1. **Eliminated Crate**: Remove `danube-reliable-dispatch` entirely (~1000+ lines of code)
2. **Simplified Architecture**: All dispatch logic in single location
3. **Reduced Maintenance**: Single codebase for all dispatcher functionality
4. **Improved Performance**: Fewer async boundaries, consolidated components
5. **Cleaner Dependencies**: Direct usage of `danube-persistent-storage`
6. **Easier Testing**: All components in same crate
7. **Explicit Modes**: Clear Non-Reliable vs Reliable behavior with simple configuration

## Execution Checklist (Phase 1 → Phase 2)
- Broker wiring
  - [x] Implement `dispatcher/subscription_engine.rs` (port from `SubscriptionDispatch`)
  - [x] Implement `dispatcher/unified_single.rs` and `dispatcher/unified_multiple.rs` with strict ack-gating for reliable mode (one in-flight per subscription)
  - [x] Update `subscription.rs` factory to instantiate unified dispatchers
  - [x] Move/ensure `TopicStore` is in `src/topic.rs` and backed by WAL/Cloud
- Remove legacy
  - [x] Remove `danube-reliable-dispatch` from workspace and dependencies
  - [ ] Delete old dispatcher files in `danube-broker/src/dispatcher/*`
- Docs
  - [ ] Update README and architecture docs to reflect unified design
- Tests
  - [ ] Unit: `subscription_engine_test.rs`, `unified_single_test.rs`, `unified_multiple_test.rs`
  - [ ] Integration: `tests/topic_store_wal.rs`, `tests/unified_dispatch_reliable.rs`, `tests/unified_dispatch_non_reliable.rs`
  - [ ] Reliable mode: WAL tail + Cloud→WAL handoff, ack/progress, retries
  - [ ] Reliable mode: strict ack gating (no next message before ack), both single and multiple consumers
  - [ ] Non-reliable mode: fan-out semantics; only active subscribers receive; no replay
  - [ ] Subscription lifecycle: add/remove/disconnect; start position = `Latest` for new subs

## Crate Structure After Migration

```
danube/
├── danube-broker/
│   ├── src/
│   │   ├── topic.rs                    # Contains TopicStore
│   │   ├── dispatcher/
│   │   │   ├── mod.rs                  # Unified Dispatcher enum
│   │   │   ├── unified_single.rs       # Single consumer dispatcher
│   │   │   ├── unified_multiple.rs     # Multiple consumer dispatcher
│   │   │   ├── subscription_engine.rs  # Reliable subscription engine
│   │   │   └── ...
│   │   └── ...
│   └── Cargo.toml                      # Direct dependency on danube-persistent-storage
├── danube-persistent-storage/          # WAL implementation
├── danube-client/
├── danube-core/
└── [danube-reliable-dispatch REMOVED]
