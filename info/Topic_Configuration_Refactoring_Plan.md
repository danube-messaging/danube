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

## Implementation Plan

### Phase 1: Producer API Simplification, Validation, Auto-Create Toggle

Priority: HIGH (eliminates silent mismatch)  
Breaking Change: YES

Objectives:
- Producer API uses only `DispatchStrategy` enum.
- Enforce strict strategy validation on producer creation.
- Support producer-driven topic auto-creation gated by broker toggle.

Key Changes:
- Proto: `danube-core/proto/DanubeApi.proto`
  - Change `ProducerRequest.dispatch_strategy` type to `DispatchStrategy` (enum).
  - Remove `TopicDispatchStrategy` and `ReliableOptions` from ProducerRequest.
- Broker: `danube-broker/src/broker_server/producer_handler.rs`
  - After resolving/creating topic, compare requested enum to topic’s stored strategy; return `failed_precondition` on mismatch.
- Broker: `danube-broker/src/broker_service.rs`
  - Accept `Option<DispatchStrategy>` instead of `Option<TopicDispatchStrategy>` in `get_topic()` and `create_topic_cluster()`.
  - On auto-create, convert enum to `ConfigDispatchStrategy` (no per-topic options) and persist.
- Config: `config/danube_broker.yml`
  - Add top-level `auto_create_topics: true|false` (default: true). When false, producer creation for missing topics returns `TopicNotFound`.
- Client SDK: `danube-client/src/producer.rs`
  - Replace `with_reliable_dispatch(options)` with parameterless `with_reliable_dispatch()`.
  - Remove `ConfigReliableOptions` exposure from public API.
- Examples: update `danube-client/examples/reliable_dispatch_producer.rs` accordingly.

Testing:
```
Topic=Reliable + Producer=Reliable → OK
Topic=NonReliable + Producer=Reliable → failed_precondition
Topic=Reliable + Producer=NonReliable → failed_precondition
Missing topic + auto_create_topics=true + Producer=Reliable → created with defaults
Missing topic + auto_create_topics=false → TopicNotFound
```

Deliverables:
- [ ] Proto updated and regenerated
- [ ] Broker validation implemented
- [ ] Auto-create toggle honored
- [ ] Client SDK and examples updated
- [ ] Unit & integration tests for the above

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
3. Optional broker default strategy if producer omits (defer).

---

## Conclusion

This plan removes producer-side detailed storage knobs, enforces strategy validation, introduces a broker-level auto-create toggle, and adds per-topic overrides via Admin. It fixes the silent data-loss risk and simplifies client usage while keeping operational flexibility via Admin.
