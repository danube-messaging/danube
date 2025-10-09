# Topic Configuration and Producer Creation Refactoring Plan

## Document Status
- Created: 2025-10-09
- Status: Implementation Plan (actionable)
- Breaking Changes: YES (no backward compatibility layer)
- Related Documents:
  - `info/WAL_Cloud_Persistence_Design.md` (WAL/Cloud persistence architecture)

---

## Table of Contents
1. [Current Situation](#current-situation)
2. [Problem Statement](#problem-statement)
3. [Proposed Solution](#proposed-solution)
4. [Rationale and Industry Comparison](#rationale-and-industry-comparison)
5. [Implementation Plan](#implementation-plan)

---

## Current Situation

### Configuration Flow Today

**Global Broker Config** (`config/danube_broker.yml`)
- WAL settings (cache, fsync, rotation, retention) applied uniformly to **all topics**
- Read at broker startup → `WalStorageFactory::new()` → `danube-persistent-storage/src/wal_factory.rs`

**Producer Request** (`danube-core/proto/DanubeApi.proto`)
- Contains `TopicDispatchStrategy` with `ReliableOptions` (segment_size, retention_policy, retention_period)
- Sent by client during producer creation

**Topic Creation** (`danube-broker/src/topic.rs:68-71`)
```rust
let dispatch_strategy = match dispatch_strategy {
    ConfigDispatchStrategy::NonReliable => DispatchStrategy::NonReliable,
    ConfigDispatchStrategy::Reliable(_reliable_options) => DispatchStrategy::Reliable,
    // ↑ ReliableOptions DISCARDED - only boolean Reliable/NonReliable stored
};
```

**Critical Issue**: ReliableOptions from producer are **completely ignored**. Topic only stores enum (Reliable vs NonReliable).

### Files Involved

**Producer Creation Flow:**
- `danube-broker/src/broker_server/producer_handler.rs:18-88` - Producer creation handler
- `danube-broker/src/broker_service.rs:65-141` - get_topic() and create_topic_cluster()
- `danube-broker/src/broker_service.rs:328-402` - create_topic_locally()
- `danube-broker/src/topic.rs:60-84` - Topic::new()

**Client Side:**
- `danube-client/src/producer.rs:30-64, 312-316` - Producer builder
- `danube-client/examples/reliable_dispatch_producer.rs:24-34` - Example usage

**Admin CLI:**
- `danube-admin-cli/src/topics.rs:44-78, 174-224` - Topic creation commands
- `danube-core/proto/DanubeAdmin.proto:6-26, 66-71` - Admin API messages

**Storage:**
- `danube-persistent-storage/src/wal_factory.rs:32-117` - WalStorageFactory
- `danube-persistent-storage/src/wal.rs` - WAL implementation

---

## Problem Statement

### Core Issues

**1. Configuration Disconnect**
- Producer sends ReliableOptions → Broker ignores them → All topics get identical config
- No per-topic retention control (e.g., 1-hour metrics vs 30-day audit logs)

**2. No Validation on Producer Creation** (CRITICAL BUG)
```
Scenario:
- Topic exists with DispatchStrategy::NonReliable
- Producer connects with DispatchStrategy::Reliable
- Current: No error raised, producer created successfully
- Result: Silent failure - producer expects persistence that doesn't exist
```

**3. Mixed Responsibilities**
- Producer creation conflated with topic configuration
- Unclear ownership: developer vs operations team

**4. Admin CLI Ineffective**
- Can specify ReliableOptions but broker ignores them
- No way to view effective topic configuration

### Impact Scenarios

**A. Strategy Mismatch** (Silent Data Loss)
- Admin creates topic as NonReliable
- Producer connects as Reliable, succeeds without error
- Messages sent without persistence → data loss

**B. Configuration Ignored** (Compliance Issues)
- Producer requests 1-hour retention → Broker uses 48-hour global default
- Compliance requirements not met, confusion about actual behavior

**C. No Per-Topic Control** (Operational Inflexibility)
- Cannot have different retention per topic type
- All topics forced into one-size-fits-all configuration

---

## Proposed Implementation (high-level)

1) Producer API
- Simplify to carry only `DispatchStrategy` enum in `ProducerRequest`.
- Remove `TopicDispatchStrategy` and `ReliableOptions` from producer protocol and client API.

2) Strict Validation on Producer Creation
- When a topic exists, the broker must validate that the requested `DispatchStrategy` matches the topic's actual strategy.
- On mismatch, fail with `failed_precondition` (include remediation hint).

3) Broker Auto-Create Toggle (defaults only)
- Allow producers to auto-create topics when missing, controlled by broker config toggle:
  - `auto_create_topics: true|false` in `config/danube_broker.yml` (top-level).
- When enabled and a topic is missing, auto-create using:
  - Strategy = requested enum (Reliable/NonReliable) only.
  - WAL/Cloud settings from global defaults in `wal_cloud` section.

4) Per-Topic WAL Overrides (Admin only)
- Per-topic overrides are configured via Admin API/CLI and stored in metadata (ETCD), then merged with global defaults when creating local topic storage.
- Add `GetTopicConfig` for introspection.

5) Configuration Hierarchy (applied at runtime)
```
Topic overrides (ETCD) → Global defaults (`danube_broker.yml`)
```

---

## Rationale (brief)

- Safety: strategy mismatch must fail fast to avoid data loss.
- Clarity: ops own topic configuration; apps declare only Reliable vs NonReliable.
- Simplicity: globals first, with optional per-topic overrides via Admin.

---

## Implementation Plan and Status

### Phase 1: Producer/Admin API Simplification and Validation [COMPLETED]

Priority: HIGH (eliminates silent mismatch)  
Breaking Change: YES

Objectives:
- Producer and Admin APIs use only `DispatchStrategy` enum.
- Enforce strict strategy validation on producer creation when topic exists.
- Keep producer-driven topic auto-creation using defaults (toggle deferred to Phase 2+).

Key Changes (merged):
- Proto (Producer): `danube-core/proto/DanubeApi.proto`
  - `ProducerRequest.dispatch_strategy: DispatchStrategy` (enum).
  - Removed `TopicDispatchStrategy`, `ReliableOptions`, `RetentionPolicy` from Producer API.
- Proto (Admin): `danube-core/proto/DanubeAdmin.proto`
  - `NewTopicRequest.dispatch_strategy: DispatchStrategy` (enum).
  - `PartitionedTopicRequest.dispatch_strategy: DispatchStrategy` (enum).
  - Removed `TopicDispatchStrategy`, `ReliableOptions`, `RetentionPolicy` from Admin API.
- Core types: `danube-core/src/dispatch_strategy.rs`
  - `ConfigDispatchStrategy` now uses unit variant `Reliable` (no per-topic options type).
  - Removed conversions to/from removed proto types.
- Broker server:
  - `danube-broker/src/broker_server/producer_handler.rs`: parse enum via `TryFrom<i32>`, pass `Option<DispatchStrategy>` to `get_topic()`.
  - `danube-broker/src/broker_service.rs`: signatures updated to accept `Option<DispatchStrategy>`; added strategy mismatch validation for locally served topics; on post of new topic, map enum to `ConfigDispatchStrategy` (NonReliable|Reliable).
  - `danube-broker/src/admin/topics_admin.rs`: map Admin enum (i32) to core enum; removed `TopicDispatchStrategy` construction.
  - `danube-broker/src/topic.rs`: adjusted to unit `Reliable` variant.
- Client SDK:
  - `danube-client/src/producer.rs`: `with_reliable_dispatch()` is parameterless; removes `ConfigReliableOptions` usage.
  - `danube-client/src/topic_producer.rs`: send enum `DispatchStrategy` (i32) in `ProducerRequest`.
  - Removed `danube-client/src/reliable_options.rs` from the public API (module no longer exported).
  - Example updated: `danube-client/examples/reliable_dispatch_producer.rs` uses parameterless `with_reliable_dispatch()`.
- Admin CLI:
  - `danube-admin-cli/src/topics.rs`: uses enum-only dispatch; removed `ReliableOptions`/`RetentionPolicy` handling and flags; simplified `parse_dispatch_strategy()` to return enum.
- Tests:
  - `danube-broker/tests/reliable_dispatch_basic.rs` and `reliable_shared_redelivery.rs` updated to use parameterless `with_reliable_dispatch()`.

Testing status:
```
Topic exists: Reliable + Producer=Reliable → OK
Topic exists: NonReliable + Producer=Reliable → failed_precondition
Topic exists: Reliable + Producer=NonReliable → failed_precondition
Missing topic (producer path) → created with requested enum and defaults (no per-topic tuning)
Admin create (enum) → topics created with requested strategy
```

Deliverables:
- [x] Producer proto updated and regenerated
- [x] Admin proto updated and regenerated
- [x] Broker validation implemented (mismatch → failed_precondition)
- [x] Client SDK and examples updated
- [x] Admin CLI updated (enum-only)
- [x] Tests adjusted
- [ ] Optional: add broker toggle for auto-create (deferred)

---

### Phase 2: Admin Per-Topic WAL Overrides (Additive)

Objectives:
- Admin API/CLI to set per-topic WAL config (optional fields) and to fetch effective config.
- Persist overrides in ETCD and merge with global defaults at topic instantiation.

Key Changes:
- Admin Proto: `danube-core/proto/DanubeAdmin.proto`
  - Add `TopicWalConfig` (optional fields: cache_capacity, fsync/rotation, retention).
  - Add `GetTopicConfig` RPC.
- Admin CLI: `danube-admin-cli/src/topics.rs`
  - Add flags for WAL knobs on `create`.
  - Add `get-config` command to retrieve effective configuration.
- Metadata: `danube-broker/src/resources/topic.rs`
  - `add_topic_wal_config()`, `get_topic_wal_config()` under `/topics/{ns}/{topic}/wal_config`.
- Storage: `danube-persistent-storage/src/wal_factory.rs`
  - `merge_configs(global, topic_override)` and `for_topic_with_config(...)`.
- Broker: `create_topic_locally()` merges overrides and passes merged config to factory.

Testing:
```
Override present → merged config applied
Override absent → global defaults used
GetTopicConfig returns effective values
Multiple topics with different overrides coexist
```

Deliverables:
- [ ] Admin proto + RPCs
- [ ] Admin CLI commands
- [ ] Metadata storage for overrides
- [ ] Factory merge + usage
- [ ] Integration tests + docs

---

### Phase 3: Documentation

Objectives:
- Update WAL design doc with configuration hierarchy and examples.
- Document producer API change and strategy-mismatch error.
- Document Admin CLI overrides and `get-config` usage.

Deliverables:
- [ ] Update `info/WAL_Cloud_Persistence_Design.md` (config hierarchy section)
- [ ] `info/Admin_CLI_Topic_Operations.md` (new)
- [ ] README updates

---

## Summary of Changes by File

### Proto Files
- `danube-core/proto/DanubeApi.proto` - Simplify ProducerRequest (Phase 1)
- `danube-core/proto/DanubeAdmin.proto` - Add TopicWalConfig, GetTopicConfig RPC (Phase 2)

### Core Types
- `danube-core/src/dispatch_strategy.rs` - Refactor/simplify ReliableOptions usage (Phase 1)

### Broker
- `danube-broker/src/broker_server/producer_handler.rs` - Add validation (Phase 1)
- `danube-broker/src/broker_service.rs` - Add defaults helper, update create_topic_locally (Phase 1, 2)
- `danube-broker/src/resources/topic.rs` - WAL config storage methods (Phase 2)

### Storage
- `danube-persistent-storage/src/wal_factory.rs` - Add for_topic_with_config, merge_configs (Phase 2)

### Client
- `danube-client/src/producer.rs` - Simplify with_reliable_dispatch(), remove ConfigReliableOptions exposure (Phase 1)
- `danube-client/examples/reliable_dispatch_producer.rs` - Update example (Phase 1)
- `danube-client/src/lib.rs` - Remove or deprecate ConfigReliableOptions export (Phase 1)

### Admin CLI
- `danube-admin-cli/src/topics.rs` - Add WAL flags, GetConfig command (Phase 2)

### Documentation
- `info/WAL_Cloud_Persistence_Design.md` - Add config hierarchy section (Phase 3)
- `info/Client_Migration_Guide_v1.0.md` - NEW (Phase 3)
- `info/Admin_CLI_Topic_Operations.md` - NEW (Phase 3)



## Success Criteria

### Phase 1
- ✅ Producer creation validates strategy match
- ✅ Clear error returned on mismatch (not silent failure)
- ✅ Auto-created topics use sensible defaults
- ✅ All existing tests pass
- ✅ New validation tests cover mismatch scenarios

### Phase 2
- ✅ Admin CLI can set per-topic WAL configuration
- ✅ Topics load correct configuration on broker start
- ✅ Config hierarchy works correctly (topic > global)
- ✅ GetTopicConfig RPC returns accurate information
- ✅ Multiple topics with different configs coexist

### Phase 3
- ✅ Documentation covers new architecture
- ✅ Migration guide helps developers update code
- ✅ Admin CLI guide enables ops teams to manage topics
- ✅ All code examples updated and tested

---

## Open Items (future work)

1. Namespace-level defaults (defer).
2. Runtime updates/hot-reload for topic config (defer).
3. Broker-level toggle to enable/disable producer auto-create (`auto_create_topics`) and default broker strategy if producer omits (defer).

---

## Conclusion

Phase 1 is complete: producer/admin APIs use enum-only `DispatchStrategy`, client/CLI/broker are aligned, and strict validation prevents silent mismatches. Auto-create currently uses requested enum with defaults; the broker-level toggle and per-topic overrides remain scheduled for Phase 2+. This fixes the silent data-loss risk while keeping the system simple for application developers and controllable by admins at topic creation time.
