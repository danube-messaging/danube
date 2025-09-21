# Danube Dispatcher Architecture Migration Strategy

## Overview

This document outlines a phased migration strategy to simplify the Danube dispatcher architecture while maintaining full backward compatibility for all existing dispatcher types:
- Reliable Single Consumer
- Reliable Multiple Consumers  
- Non-Reliable Single Consumer
- Non-Reliable Multiple Consumers

**Key Change**: This strategy eliminates the `danube-reliable-dispatch` crate entirely, consolidating all dispatch logic within `danube-broker`.

## Current Architecture Analysis

### Current Components
- `danube-reliable-dispatch/src/dispatch.rs` - `SubscriptionDispatch` (WAL streaming + message tracking) **[TO BE REMOVED]**
- `danube-reliable-dispatch/src/topic_storage.rs` - `TopicStore` (WAL interface wrapper) **[TO BE MOVED]**
- `danube-broker/src/dispatcher.rs` - `Dispatcher` enum with 4 variants
- `danube-broker/src/dispatcher/dispatcher_reliable_single_consumer.rs`
- `danube-broker/src/dispatcher/dispatcher_reliable_multiple_consumers.rs`
- `danube-broker/src/dispatcher/dispatcher_single_consumer.rs`
- `danube-broker/src/dispatcher/dispatcher_multiple_consumers.rs`

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
    pub(crate) async fn store_message(&self, message: StreamMessage) -> Result<()> { ... }
    pub(crate) async fn create_reader(&self, start: StartPosition) -> Result<TopicStream> { ... }
}

// danube-broker/src/dispatcher/mod.rs - Unified Dispatchers
pub(crate) enum Dispatcher {
    Single(UnifiedSingleDispatcher),
    Multiple(UnifiedMultipleDispatcher),
}

pub(crate) struct UnifiedSingleDispatcher {
    reliable_core: Option<ReliableCore>,
    consumers: Vec<Consumer>,
    control_tx: mpsc::Sender<DispatcherCommand>,
    notify: Arc<Notify>,
}

pub(crate) struct UnifiedMultipleDispatcher {
    reliable_core: Option<ReliableCore>,
    consumers: Vec<Consumer>,
    round_robin_index: AtomicUsize,
    control_tx: mpsc::Sender<DispatcherCommand>,
    notify: Arc<Notify>,
}
```

### Consolidated Reliable Core
```rust
// danube-broker/src/dispatcher/reliable_core.rs - All reliable dispatch logic
pub(crate) struct ReliableCore {
    topic_store: TopicStore,
    stream: Option<TopicStream>,
    pending_ack: Option<(u64, MessageID)>,
    retry_count: u8,
    last_retry_timestamp: Option<Instant>,
}

impl ReliableCore {
    // All logic from SubscriptionDispatch moved here
    pub(crate) async fn poll_next(&mut self) -> Result<StreamMessage> { ... }
    pub(crate) async fn handle_message_acked(&mut self, request_id: u64, msg_id: MessageID) -> Result<Option<StreamMessage>> { ... }
}
```

## Migration Phases

### Phase 1: Consolidate Within Broker 
**Goal**: Move all reliable dispatch logic into danube-broker without breaking existing functionality

#### Tasks:
1. **Move TopicStore to danube-broker/src/topic.rs**
   - Copy `TopicStore` implementation from `danube-reliable-dispatch/src/topic_storage.rs`
   - Update imports and dependencies in `danube-broker/Cargo.toml`
   - Add direct dependency on `danube-persistent-storage`

2. **Create ReliableCore in danube-broker**
   - Extract all logic from `SubscriptionDispatch` into `ReliableCore`
   - Move to `danube-broker/src/dispatcher/reliable_core.rs`
   - Include WAL streaming, message tracking, retry logic

3. **Create Shared Components**
   - `danube-broker/src/dispatcher/periodic_notifier.rs`
   - `danube-broker/src/dispatcher/command_handler.rs`

#### Files to Create/Modify:
- **Move**: `TopicStore` to `danube-broker/src/topic.rs`
- **Create**: `danube-broker/src/dispatcher/reliable_core.rs`
- **Create**: `danube-broker/src/dispatcher/periodic_notifier.rs`
- **Create**: `danube-broker/src/dispatcher/command_handler.rs`
- **Update**: `danube-broker/Cargo.toml` (add danube-persistent-storage dependency)

#### Testing:
- Unit tests for `ReliableCore` with mock `TopicStore`
- Integration tests ensuring WAL streaming works correctly
- Verify existing tests still pass with moved components

### Phase 2: Implement Unified Dispatchers 
**Goal**: Create new unified dispatchers that work with consolidated components

#### Tasks:
1. **Implement UnifiedSingleDispatcher**
   - Use `ReliableCore` for reliable mode (when `Some`)
   - Direct message dispatch for non-reliable mode (when `None`)
   - Maintain exact same external API as current dispatchers

2. **Implement UnifiedMultipleDispatcher**
   - Use `ReliableCore` for reliable mode
   - Round-robin dispatch for non-reliable mode
   - Maintain exact same external API as current dispatchers

3. **Add Feature Flag**
   - `use_unified_dispatchers` configuration option
   - Default to `false` (use existing dispatchers)

#### Files to Create/Modify:
- **Create**: `danube-broker/src/dispatcher/unified_single.rs`
- **Create**: `danube-broker/src/dispatcher/unified_multiple.rs`
- **Update**: `danube-broker/src/dispatcher/mod.rs` with feature flag logic
- **Update**: `danube-broker/src/config.rs` (add configuration option)

#### Testing:
- Full integration tests with both reliable and non-reliable modes
- Consumer lifecycle tests (add/remove/disconnect)
- Message acknowledgment tests
- Failover and retry behavior tests

### Phase 3: Gradual Migration 
**Goal**: Migrate existing functionality while maintaining compatibility

#### Tasks:
1. **Update Dispatcher Factory**
   - Modify dispatcher creation logic in `danube-broker/src/subscription.rs`
   - Use unified dispatchers when feature flag enabled
   - Ensure seamless fallback to existing dispatchers

2. **Remove danube-reliable-dispatch Dependencies**
   - Update all imports to use consolidated components
   - Remove `danube-reliable-dispatch` from `danube-broker/Cargo.toml`
   - Update workspace `Cargo.toml`

3. **Extended Testing**
   - Run all existing tests with unified dispatchers enabled
   - Performance comparison between old and new implementations
   - Load testing with mixed reliable/non-reliable workloads

#### Files to Modify:
- **Update**: `danube-broker/src/subscription.rs` (dispatcher creation)
- **Update**: `danube-broker/src/topic.rs` (dispatcher integration)
- **Update**: `danube-broker/Cargo.toml` (remove danube-reliable-dispatch dependency)
- **Update**: `Cargo.toml` (workspace level)
- **Update**: Configuration examples in `config/`

### Phase 4: Default to Unified Architecture 
**Goal**: Make unified dispatchers the default and prepare for cleanup

#### Tasks:
1. **Default to Unified Dispatchers**
   - Change default configuration to use unified dispatchers
   - Add deprecation warnings for old dispatcher usage

2. **Performance Optimization**
   - Profile and optimize unified dispatchers
   - Remove any remaining performance bottlenecks
   - Optimize memory usage with consolidated architecture

3. **Documentation Updates**
   - Update architecture documentation
   - Remove references to `danube-reliable-dispatch`
   - Add migration guide for operators

#### Files to Modify:
- Configuration defaults
- Add deprecation warnings
- Update documentation

### Phase 5: Final Cleanup 
**Goal**: Remove deprecated code and finalize consolidated architecture

#### Tasks:
1. **Remove Old Dispatchers**
   - Delete `dispatcher_reliable_single_consumer.rs`
   - Delete `dispatcher_reliable_multiple_consumers.rs`
   - Remove feature flag and old code paths

2. **Remove danube-reliable-dispatch Crate**
   - Delete entire `danube-reliable-dispatch/` directory
   - Update workspace `Cargo.toml` to remove crate
   - Clean up any remaining references

3. **Final Simplification**
   - Simplify `Dispatcher` enum to only have `Single` and `Multiple` variants
   - Clean up any remaining unused code
   - Update all documentation

#### Files to Remove:
- **Entire crate**: `danube-reliable-dispatch/`
- `danube-broker/src/dispatcher/dispatcher_reliable_single_consumer.rs`
- `danube-broker/src/dispatcher/dispatcher_reliable_multiple_consumers.rs`
- Feature flag logic

## Backward Compatibility Strategy

### API Compatibility
- All public APIs remain unchanged
- Configuration options remain the same
- Behavior is identical from external perspective
- No changes needed for client applications

### Testing Strategy
- Run existing test suite against new implementation
- Add specific migration tests
- Performance regression tests
- Long-running stability tests

### Rollback Plan
- Feature flag allows immediate rollback to old implementation (Phases 2-3)
- Keep old code until Phase 5
- Monitor metrics and logs during migration

## Configuration Changes

### New Configuration Options
```yaml
# danube_broker.yml
dispatcher:
  # Phase 2-3: Feature flag for gradual migration
  use_unified_dispatchers: false  # Default false initially
  
  # Phase 4+: Remove feature flag, always use unified
  # No configuration changes needed for end users
```

### Dependency Changes
```toml
# danube-broker/Cargo.toml - After Phase 3
[dependencies]
# Remove: danube-reliable-dispatch = { path = "../danube-reliable-dispatch" }
danube-persistent-storage = { path = "../danube-persistent-storage" }  # Add direct dependency
```

## Risk Mitigation

### Technical Risks
1. **Performance Regression**: Comprehensive benchmarking at each phase
2. **Behavioral Changes**: Extensive integration testing
3. **Memory Leaks**: Memory profiling and long-running tests
4. **Race Conditions**: Stress testing with high concurrency
5. **Dependency Issues**: Careful management of crate dependencies

### Operational Risks
1. **Production Issues**: Feature flag allows immediate rollback
2. **Configuration Errors**: Backward compatible defaults
3. **Monitoring Gaps**: Enhanced logging during migration
4. **Training Needs**: Comprehensive documentation updates

## Success Metrics

### Code Quality
- **Eliminate entire crate**: Remove `danube-reliable-dispatch` (~1000+ lines)
- Reduce dispatcher-related code by ~50%
- Eliminate code duplication between reliable dispatchers
- Improve test coverage to >95%

### Performance
- No regression in message throughput
- Reduce memory usage by ~30% (fewer async tasks, consolidated components)
- Improve latency consistency
- Reduce binary size by eliminating crate

### Maintainability
- Single location for all dispatch logic
- Clearer separation of concerns
- Easier to add new features
- Simplified dependency graph

## Post-Migration Benefits

1. **Eliminated Crate**: Remove `danube-reliable-dispatch` entirely (~1000+ lines of code)
2. **Simplified Architecture**: All dispatch logic in single location
3. **Reduced Maintenance**: Single codebase for all dispatcher functionality
4. **Improved Performance**: Fewer async boundaries, consolidated components
5. **Cleaner Dependencies**: Direct usage of `danube-persistent-storage`
6. **Easier Testing**: All components in same crate
7. **Better Developer Experience**: Single place to understand dispatch logic
8. **Reduced Binary Size**: One fewer crate to compile and link

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
│   │   │   ├── reliable_core.rs        # All reliable dispatch logic
│   │   │   ├── periodic_notifier.rs    # Shared timing component
│   │   │   └── command_handler.rs      # Shared command processing
│   │   └── ...
│   └── Cargo.toml                      # Direct dependency on danube-persistent-storage
├── danube-persistent-storage/          # WAL implementation
├── danube-client/
├── danube-core/
└── [danube-reliable-dispatch REMOVED]
