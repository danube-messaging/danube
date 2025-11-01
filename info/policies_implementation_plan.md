# Danube Broker Policies Implementation Plan

## Executive Summary

This document outlines the implementation plan for enforcing the 8 broker policies defined in the configuration file. The policies control resource limits (producers, consumers, subscriptions) and rate limits (publish rate, dispatch rate) at the broker, namespace, and topic levels.

---

## Policy Overview

The following policies need to be implemented:

1. **max_producers_per_topic** - Limits concurrent producers per topic
2. **max_subscriptions_per_topic** - Limits subscriptions per topic
3. **max_consumers_per_topic** - Limits concurrent consumers per topic
4. **max_consumers_per_subscription** - Limits consumers per subscription
5. **max_publish_rate** - Rate limits messages/bytes per second for producers
6. **max_dispatch_rate** - Rate limits dispatch messages/bytes per second for topics
7. **max_subscription_dispatch_rate** - Rate limits dispatch per subscription
8. **max_message_size** - Limits individual message payload size

---

## Architecture Context

### Policy Hierarchy
Policies follow a three-tier hierarchy with override capability:
1. **Broker-level policies** (from config file) - Default baseline
2. **Namespace-level policies** - Override broker defaults for all topics in namespace
3. **Topic-level policies** - Override namespace/broker policies for specific topic

### Current State
- Policies are already loaded from config: `service_configuration.rs` (line 34, 62, 108)
- Policy struct is defined: `policies.rs` with all 8 fields and public getters
- Topics have `topic_policies` field: `topic.rs`
- Method `policies_update` exists: `topic.rs`
- Topics receive policies during initialization: `topic_control.rs`

---

## Implementation Status (Phase 1)

Status: COMPLETED

- Policies getters added in `danube-broker/src/policies.rs` for all 8 fields.
- Topic authoritative helpers/validations added in `danube-broker/src/topic.rs`:
  - Counters: `producer_count()`, `subscription_count()`, `total_consumer_count()`
  - Validations: `can_add_producer()`, `can_add_subscription()`, `can_add_consumer_to_subscription()`, `validate_message_size()`
  - Wired into: `create_producer`, `subscribe`, and at entry of `publish_message_async` (message size)
- Broker service helper implemented in `danube-broker/src/broker_service.rs`:
  - `allow_subscription_creation(..)` is now async and enforces `max_subscriptions_per_topic` using `subscription_count()`
- Early gRPC checks added:
  - `broker_server/producer_handler.rs`: `create_producer` checks `can_add_producer()`; `send_message` validates `max_message_size`
  - `broker_server/consumer_handler.rs`: `subscribe` calls async `allow_subscription_creation()` and checks `max_consumers_per_topic`
- Tests:
  - Unit tests added in `src/topic_tests.rs` for all Phase 1 policies (limits and message size). Integration tests deferred for now.

Notes: `0` continues to mean unlimited for all count policies; default `max_message_size` remains 10 MiB.

---

## Implementation Plan by Policy

## 1. MAX_PRODUCERS_PER_TOPIC

### Purpose
Prevents resource exhaustion by limiting the number of concurrent producers that can publish to a topic.

### Enforcement Points

#### A. Producer Creation Path - MAIN CHECK
**File:** `danube-broker/src/broker_server/producer_handler.rs`  
**Function:** `create_producer` (lines 19-107)  
**Location:** Before calling `service.create_new_producer()` at line 83  
**Implementation:**
- After the producer existence check (lines 70-81) but before creating the new producer
- Retrieve the topic from `topic_worker_pool`
- Lock `topic.producers` and count current active producers
- Retrieve the policy value from `topic.topic_policies.max_producers_per_topic`
- If policy is 0 (unlimited), allow creation
- If current count >= policy limit, return error with `Status::resource_exhausted`
- Include informative error message with current count and limit

#### B. Topic Creation Helper
**File:** `danube-broker/src/topic.rs`  
**Function:** `create_producer` (lines 97-127)  
**Location:** At entry point, line 104  
**Implementation:**
- Add validation method `can_add_producer(&self) -> Result<()>`
- Check producer count against policy before adding to HashMap
- Return descriptive error if limit exceeded

#### C. Helper Method
**File:** `danube-broker/src/topic.rs`  
**Add new method:** `get_producer_count(&self) -> usize`
**Implementation:**
- Returns current number of producers in the HashMap
- Useful for metrics and policy checks

**File:** `danube-broker/src/policies.rs`  
**Add getter:** `pub fn get_max_producers_per_topic(&self) -> u32`

---

## 2. MAX_SUBSCRIPTIONS_PER_TOPIC

### Purpose
Limits the number of subscriptions that can be created on a topic to prevent metadata bloat and resource issues.

### Enforcement Points

#### A. Subscription Creation - MAIN CHECK
**File:** `danube-broker/src/broker_server/consumer_handler.rs`  
**Function:** `subscribe` (lines 20-110)  
**Location:** After line 77 policy check, before line 79  
**Implementation:**
- After `allow_subscription_creation` check (line 70)
- Retrieve topic from `service.topic_worker_pool`
- Lock `topic.subscriptions` and count entries
- Get policy value from `topic.topic_policies.max_subscriptions_per_topic`
- If policy is 0, allow creation
- If current count >= policy limit, return `Status::resource_exhausted`
- Message should indicate subscription limit reached

#### B. Topic Subscribe Method
**File:** `danube-broker/src/topic.rs`  
**Function:** `subscribe` (lines 279-340)  
**Location:** At entry, line 288, before creating subscription  
**Implementation:**
- Add check method `can_add_subscription(&self) -> Result<()>`
- Count subscriptions in HashMap against policy
- Return error if limit would be exceeded

#### C. Helper Method
**File:** `danube-broker/src/policies.rs`  
**Add getter:** `pub fn get_max_subscriptions_per_topic(&self) -> u32`

---

## 3. MAX_CONSUMERS_PER_TOPIC

### Purpose
Limits total concurrent consumers across all subscriptions on a topic to prevent resource exhaustion.

### Enforcement Points

#### A. Consumer Subscribe - MAIN CHECK
**File:** `danube-broker/src/broker_server/consumer_handler.rs`  
**Function:** `subscribe` (lines 20-110)  
**Location:** After subscription creation check (line 77), before calling `subscribe_async` (line 89)  
**Implementation:**
- Retrieve topic from topic_worker_pool
- Iterate through all subscriptions and sum consumer counts
- Check against `topic.topic_policies.max_consumers_per_topic`
- If policy is 0, allow
- If total consumers >= limit, return `Status::resource_exhausted`

#### B. Topic Subscribe Method
**File:** `danube-broker/src/topic.rs`  
**Function:** `subscribe` (lines 279-340)  
**Location:** Before calling `subscription.add_consumer` at line 337  
**Implementation:**
- Add method `get_total_consumer_count(&self) -> usize`
- Lock subscriptions, iterate and sum consumer counts from each subscription
- Check against policy before allowing consumer addition

#### C. Subscription Add Consumer
**File:** `danube-broker/src/subscription.rs`  
**Function:** `add_consumer` (lines 97-138)  
**Location:** At entry (line 102)  
**Implementation:**
- This is where the actual consumer is added
- The topic-level check should occur before this is called
- No additional check needed here if topic-level is enforced

#### D. Helper Methods
**File:** `danube-broker/src/topic.rs`  
**Add method:** `get_total_consumer_count(&self) -> usize`

**File:** `danube-broker/src/policies.rs`  
**Add getter:** `pub fn get_max_consumers_per_topic(&self) -> u32`

---

## 4. MAX_CONSUMERS_PER_SUBSCRIPTION

### Purpose
Limits concurrent consumers for a single subscription. Important for Exclusive subscriptions (should be 1) and controlling Shared subscription fan-out.

### Enforcement Points

#### A. Subscription Add Consumer - MAIN CHECK
**File:** `danube-broker/src/subscription.rs`  
**Function:** `add_consumer` (lines 97-138)  
**Location:** At entry, around line 102, before creating consumer  
**Implementation:**
- Count existing consumers in `self.consumers` HashMap
- Retrieve topic to get policies (need to pass topic reference or policies)
- Check against `topic.topic_policies.max_consumers_per_subscription`
- If policy is 0, allow
- If count >= limit, return error
- Note: Exclusive subscription check already exists at line 330-333 in `topic.rs`

#### B. Topic Subscribe Helper
**File:** `danube-broker/src/topic.rs`  
**Function:** `subscribe` (lines 279-340)  
**Location:** Before line 337 `subscription.add_consumer`  
**Implementation:**
- Pass topic policies to subscription or check here
- Validate consumer count for the specific subscription
- The existing exclusive check (lines 330-333) handles the case where max should be 1

#### C. Consumer Handler
**File:** `danube-broker/src/broker_server/consumer_handler.rs`  
**Function:** `subscribe` (lines 20-110)  
**Location:** Before subscribe_async call at line 89  
**Implementation:**
- Additional validation can be done here for early rejection
- Retrieve subscription from topic and check consumer count
- However, the main enforcement should be in subscription.rs

#### D. Changes Needed
**File:** `danube-broker/src/subscription.rs`  
**Modification:** Update `add_consumer` to accept policies or topic reference  
**Add method:** `can_add_consumer(&self, policies: &Policies) -> Result<()>`

**File:** `danube-broker/src/policies.rs`  
**Add getter:** `pub fn get_max_consumers_per_subscription(&self) -> u32`

---

## 5. MAX_PUBLISH_RATE

### Purpose
Rate-limits the number of messages and/or bytes per second that producers can publish to a topic. Prevents individual producers or topics from overwhelming the system.

### Implementation Strategy
Use a token bucket or leaky bucket algorithm with time-window tracking. Track both message count and byte count separately.

### Enforcement Points

#### A. Message Publishing - MAIN CHECK
**File:** `danube-broker/src/broker_server/producer_handler.rs`  
**Function:** `send_message` (lines 110-164)  
**Location:** After producer validation (lines 129-139), before `publish_message_async` at line 146  
**Implementation:**
- Retrieve topic from topic_worker_pool
- Check if rate limiter exists in topic
- Call rate limiter's `try_acquire(message_size)` method
- If rate limit exceeded, return `Status::resource_exhausted("Publish rate limit exceeded")`
- Include current rate and limit in error message

#### B. Topic Structure Enhancement
**File:** `danube-broker/src/topic.rs`  
**Location:** Add field to Topic struct (line 50-67)  
**Implementation:**
- Add field: `publish_rate_limiter: Option<Arc<Mutex<RateLimiter>>>`
- Initialize in `Topic::new` method based on policy
- Rate limiter should track both message count and bytes per second

#### C. Rate Limiter Implementation
**File:** `danube-broker/src/rate_limiter.rs` (NEW FILE)  
**Implementation:**
- Create `RateLimiter` struct with:
  - `max_messages_per_sec: u32`
  - `max_bytes_per_sec: u32`
  - `message_tokens: AtomicU64`
  - `byte_tokens: AtomicU64`
  - `last_refill: Mutex<Instant>`
- Implement token bucket algorithm
- Method: `try_acquire(message_count: u32, byte_count: u32) -> bool`
- Refill tokens based on elapsed time
- Deduct tokens if available, return false if insufficient

#### D. Configuration Interpretation
**File:** `danube-broker/src/policies.rs`  
**Note:** The current `max_publish_rate: u32` is a single value  
**Recommendation:** 
- For now, interpret as messages per second (primary constraint)
- In future, consider splitting into two fields: `max_publish_rate_msg` and `max_publish_rate_bytes`
- Or use rate as messages/sec and add a separate `max_publish_throughput_bytes` field

**Add getter:** `pub fn get_max_publish_rate(&self) -> u32`

---

## 6. MAX_DISPATCH_RATE

### Purpose
Rate-limits message dispatch from topic to all subscriptions combined. Prevents overwhelming consumers with too many messages.

### Implementation Strategy
Similar to publish rate, use token bucket for message/byte rate limiting at the topic dispatch level.

### Enforcement Points

#### A. Topic Dispatch - NonReliable Strategy
**File:** `danube-broker/src/topic.rs`  
**Function:** `dispatch_to_subscriptions_async` (lines 204-240)  
**Location:** At entry, before iterating subscriptions (line 207)  
**Implementation:**
- Check topic's `dispatch_rate_limiter` before dispatching
- Call `try_acquire(1, message_size)` for rate check
- If rate limit exceeded, drop message or queue for later (depending on strategy)
- For NonReliable mode, dropping is acceptable
- Log rate limit events for monitoring

#### B. Topic Dispatch - Reliable Strategy
**File:** `danube-broker/src/topic.rs`  
**Function:** `publish_message_async` (lines 153-201)  
**Location:** After WAL storage (line 189), before notifying subscriptions (line 194)  
**Implementation:**
- For reliable mode, messages are already persisted in WAL
- Rate limiting affects when they are dispatched to consumers
- Check rate limiter before notifying dispatchers
- If rate exceeded, delay notification (use throttled notify queue)
- Dispatchers will naturally pace themselves based on notification rate

#### C. Dispatcher Integration
**File:** `danube-broker/src/dispatcher/unified_single.rs` and `unified_multiple.rs`  
**Location:** In dispatch loops where messages are polled  
**Implementation:**
- Dispatchers receive messages from topic via notification
- The rate limiting should happen at topic level, not dispatcher level
- However, dispatchers should respect backpressure signals
- Add metrics to track dispatch rate limiting events

#### D. Rate Limiter for Dispatch
**File:** `danube-broker/src/topic.rs`  
**Location:** Add field to Topic struct  
**Implementation:**
- Add field: `dispatch_rate_limiter: Option<Arc<Mutex<RateLimiter>>>`
- Initialize based on `topic_policies.max_dispatch_rate`
- Use the same RateLimiter implementation as publish rate

**File:** `danube-broker/src/policies.rs`  
**Add getter:** `pub fn get_max_dispatch_rate(&self) -> u32`

---

## 7. MAX_SUBSCRIPTION_DISPATCH_RATE

### Purpose
Rate-limits message dispatch to each individual subscription. Allows fine-grained control over consumer groups.

### Implementation Strategy
Each subscription maintains its own rate limiter. Messages are throttled before being sent to the subscription's dispatcher.

### Enforcement Points

#### A. Subscription Dispatch - NonReliable
**File:** `danube-broker/src/subscription.rs`  
**Function:** `send_message_to_dispatcher` (lines 267-275)  
**Location:** At entry, before calling dispatcher.dispatch_message (line 270)  
**Implementation:**
- Check subscription's rate limiter before dispatching
- Call `try_acquire(1, message.payload.len())`
- If rate limit exceeded, return error or drop (NonReliable)
- Log rate limiting events

#### B. Subscription Structure Enhancement
**File:** `danube-broker/src/subscription.rs`  
**Location:** Add field to Subscription struct (line 27-34)  
**Implementation:**
- Add field: `dispatch_rate_limiter: Option<Arc<Mutex<RateLimiter>>>`
- Initialize in `Subscription::new` based on topic policies
- Pass policies to subscription during creation

#### C. Dispatcher Methods
**File:** `danube-broker/src/dispatcher/unified_single.rs`  
**Function:** Dispatch message handling  
**Location:** Where messages are sent to consumers  
**Implementation:**
- The rate limiting should occur before messages reach dispatcher
- Dispatchers implement per-consumer flow control
- Subscription-level rate limit is an additional layer on top

#### D. Topic Subscribe Method Update
**File:** `danube-broker/src/topic.rs`  
**Function:** `subscribe` (lines 279-340)  
**Location:** When creating new subscription (line 294)  
**Implementation:**
- Pass topic policies to `Subscription::new`
- Subscription initializes its rate limiter based on policy
- Rate limiter applies to all messages sent to that subscription

**File:** `danube-broker/src/policies.rs`  
**Add getter:** `pub fn get_max_subscription_dispatch_rate(&self) -> u32`

---

## 8. MAX_MESSAGE_SIZE

### Purpose
Limits the maximum payload size of individual messages to prevent memory issues and ensure fair resource usage.

### Enforcement Points

#### A. Message Reception - MAIN CHECK
**File:** `danube-broker/src/broker_server/producer_handler.rs`  
**Function:** `send_message` (lines 110-164)  
**Location:** Immediately after receiving message, at line 115  
**Implementation:**
- Check `stream_message.payload.len()` 
- Retrieve policy from topic: `topic.topic_policies.max_message_size`
- If payload size > max_message_size, reject immediately
- Return `Status::invalid_argument("Message size exceeds maximum allowed")`
- Include actual size and limit in error message
- This check happens before any persistence or processing

#### B. Topic Message Publishing
**File:** `danube-broker/src/topic.rs`  
**Function:** `publish_message_async` (lines 153-201)  
**Location:** At entry, line 165, after state check  
**Implementation:**
- Add validation method `validate_message_size(&self, message: &StreamMessage) -> Result<()>`
- Check payload size against policy
- Return error if exceeded
- This provides defense-in-depth even if handler check is bypassed

#### C. Configuration Default
**File:** `danube-broker/src/policies.rs`  
**Current state:** Default is 10 MB (line 40, 44-46)  
**Verification:**
- Ensure the default is correctly applied (10485760 bytes)
- Add getter method

**Add getter:** `pub fn get_max_message_size(&self) -> u32`

---

## Additional Policy Enforcement Points

### Policy Retrieval and Hierarchy

**File:** `danube-broker/src/topic.rs`  
**Current implementation:** Lines 108-128 in `topic_control.rs`  
**Enhancement needed:**
- Improve policy resolution to clearly show hierarchy
- Create helper method: `get_effective_policies(topic_name, resources) -> Policies`
- Check topic policies first, then namespace, then broker default
- Log which policy level is being applied for debugging

**File:** `danube-broker/src/topic_control.rs`  
**Function:** `ensure_local` (lines 62-140)  
**Current state:** Already implements policy hierarchy (lines 108-128)  
**Verification:**
- Ensure broker-level policies from config are properly propagated
- Namespace policies should come from resources.namespace
- Topic policies override both

### Broker Service Policy Check

**File:** `danube-broker/src/broker_service.rs`  
**Function:** `allow_subscription_creation`  
**Current state:** Implemented (async)  
**Implementation:**
- Retrieves topic from `topic_worker_pool`
- Compares `topic.subscription_count()` against `max_subscriptions_per_topic`
- Returns `false` when limit reached, `true` otherwise

---

## Suggested Additional Policies (Optional)

Based on analysis of the codebase and common messaging system requirements, here are 3 additional policies that would enhance the system:

### 1. MAX_UNACKED_MESSAGES_PER_CONSUMER

**Purpose:** Limits the number of in-flight (unacknowledged) messages per consumer in reliable mode. Prevents slow consumers from accumulating unbounded pending messages.

**Enforcement Location:**
- **File:** `danube-broker/src/dispatcher/unified_single.rs` and `unified_multiple.rs`
- **Location:** In the reliable dispatcher loops where messages are dispatched
- **Implementation:** Track pending message count per consumer, block dispatch if limit reached until acks received

**Rationale:** Currently, the reliable dispatchers use a pending flag for single consumer but don't have a limit for multiple consumer scenarios. This could cause memory issues with slow consumers.


### 4. CONNECTION_RATE_LIMIT

**Purpose:** Limits the rate at which new producers/consumers can connect to the broker. Prevents connection storms and DoS attacks.

**Enforcement Location:**
- **File:** `danube-broker/src/broker_server/producer_handler.rs` and `consumer_handler.rs`
- **Location:** At entry to `create_producer` and `subscribe` functions
- **Implementation:** Broker-level token bucket for connection attempts per time window

**Rationale:** Protects the broker from being overwhelmed by connection attempts, whether malicious or due to thundering herd scenarios (e.g., mass client restart).

---

## Implementation Phases

### Phase 1: Count-Based Policies (Immediate)
**Priority: HIGH**

Enforce simple count and size constraints with minimal surface changes. This phase adds read access to `Policies`, internal topic validations, early gRPC rejections, and a broker-side helper.

Subphases:

- **P1.0 – Policies getters (unblockers)**
  - Add read-only getters in `danube-broker/src/policies.rs` for all 8 fields so other modules can access limits.
  - Scope: internal only; no behavior change.

- **P1.1 – Topic helpers and authoritative checks**
  - File: `danube-broker/src/topic.rs`
  - Add helper counters:
    - `producer_count()`, `subscription_count()`, `total_consumer_count()` (iterate subs and sum).
  - Add validations (treat 0 as unlimited):
    - `can_add_producer()` vs `max_producers_per_topic`.
    - `can_add_subscription()` vs `max_subscriptions_per_topic`.
    - `can_add_consumer_to_subscription(sub)` vs `max_consumers_per_subscription`.
    - Check `total_consumer_count()` vs `max_consumers_per_topic`.
    - `validate_message_size(size)` vs `max_message_size`.
  - Wire-ins:
    - At `create_producer` entry: call `can_add_producer()` before insert.
    - At `subscribe` before creating subscription: `can_add_subscription()`.
    - At `subscribe` before `add_consumer`: `can_add_consumer_to_subscription()` and `total_consumer_count()`.
    - At `publish_message_async` entry: `validate_message_size()`.
  - Errors: return descriptive `anyhow::Error` including current count/size and limit.

- **P1.2 – Broker service helper**
  - File: `danube-broker/src/broker_service.rs`
  - Implement `allow_subscription_creation(topic_name)` by comparing `topic.subscription_count()` to `max_subscriptions_per_topic`.
  - Returns `false` if limit reached; used by gRPC early check.

- **P1.3 – Early gRPC rejections (UX/fast-fail)**
  - File: `broker_server/producer_handler.rs`
    - `create_producer`: after `get_topic`, fetch topic, call `can_add_producer()`. If violation, return `Status::resource_exhausted` with details.
    - `send_message`: validate size via topic `validate_message_size()`; on violation, return `Status::invalid_argument`.
  - File: `broker_server/consumer_handler.rs`
    - `subscribe`: after `get_topic`, call `allow_subscription_creation()`; if false, return `resource_exhausted`.
    - Also compute and check `max_consumers_per_topic` via `topic.total_consumer_count()`; early reject if at limit.
  - Note: topic-level checks remain authoritative to handle races.

- **P1.4 – Tests**
  - Unit: `Policies` getters and default `max_message_size`.
  - Integration: limits for producers, subscriptions, consumers (topic and per-subscription), and message size rejections; assert `ResourceExhausted` or `InvalidArgument`.

Implemented policies in Phase 1:

1. **max_producers_per_topic**
2. **max_subscriptions_per_topic**
3. **max_consumers_per_topic**
4. **max_consumers_per_subscription**
5. **max_message_size**

Rationale: Pure count/size checks add immediate protection and are easy to validate. Double-enforcement (handler + topic) ensures correctness under concurrency and better client UX.

### Phase 2: Rate Limiting Infrastructure
**Priority: HIGH**

Introduce a lightweight token-bucket for messages/sec first (bytes/sec can be added later). Wire scaffolding only in this phase; enforcement comes in Phase 3.

Subphases:

- **P2.0 – RateLimiter module**
  - New: `danube-broker/src/rate_limiter.rs`.
  - Struct `RateLimiter` (messages/sec initial):
    - Fields: `max_per_sec: u32`, `tokens: AtomicU64`, `last_refill: Mutex<Instant>`.
    - Methods: `new(max)`, `try_acquire(n: u32) -> bool`, internal `refill()`.
  - Future-ready: plan for bytes/sec by adding `max_bytes_per_sec`, `byte_tokens`.

- **P2.1 – Topic wiring (no enforcement yet)**
  - File: `danube-broker/src/topic.rs`
  - Add optional fields:
    - `publish_rate_limiter: Option<Arc<Mutex<RateLimiter>>>`
    - `dispatch_rate_limiter: Option<Arc<Mutex<RateLimiter>>>`
  - Initialize in `Topic::new()` or during `policies_update()` based on `max_publish_rate` and `max_dispatch_rate` (> 0).
  - Keep unused for now (no `try_acquire` calls yet).

- **P2.2 – Subscription wiring (no enforcement yet)**
  - File: `danube-broker/src/subscription.rs`
  - Add optional field:
    - `dispatch_rate_limiter: Option<Arc<Mutex<RateLimiter>>>`
  - Initialize in `Subscription::new()` (parameters supplied by `Topic::subscribe()` from topic policies) when `max_subscription_dispatch_rate` > 0.

- **P2.3 – Metrics and logging**
  - Counters for limiter utilization and throttle events to be incremented in Phase 3.

Notes and suggestions applied:
- Expose broker defaults via existing namespace creation path (already done by `danube_service.rs`); optional explicit broker fallback in `ensure_local()` can be considered later for legacy namespaces.
- Keep rate limit logic centralized (topic/ subscription), not in dispatchers; dispatchers will just observe pacing.

### Phase 3: Rate-Based Policies
**Priority: MEDIUM**  
Implement rate limiting enforcement:

1. **max_publish_rate** (producer -> topic)
2. **max_dispatch_rate** (topic -> subscriptions)
3. **max_subscription_dispatch_rate** (subscription -> consumers)

**Rationale:** Requires the rate limiter infrastructure from Phase 2. More complex than count-based policies.

### Phase 4: Testing and Tuning
**Priority: HIGH**  
Comprehensive testing of all policies:

1. Unit tests for each policy enforcement point
2. Integration tests for policy hierarchy (broker -> namespace -> topic)
3. Performance tests to ensure rate limiters don't add significant overhead
4. Load tests to verify policies prevent resource exhaustion


### Phase 5: Optional Enhancements
**Priority: LOW**  
If time permits, implement suggested additional policies:

1. MAX_UNACKED_MESSAGES_PER_CONSUMER
4. CONNECTION_RATE_LIMIT


---

## Testing Strategy

### Unit Tests

1. **Policy Struct Tests** (`policies.rs`)
   - Test serialization/deserialization
   - Test getter methods
   - Test default values

2. **Rate Limiter Tests** (`rate_limiter.rs`)
   - Test token bucket algorithm
   - Test refill rate
   - Test concurrent access
   - Test edge cases (zero limits, very high rates)

3. **Policy Enforcement Tests**
   - Test each enforcement point in isolation
   - Mock topic, subscription, consumer objects
   - Verify correct error codes and messages
   - Test policy hierarchy (broker < namespace < topic)

### Integration Tests

1. **Producer Tests**
   - Verify max_producers_per_topic prevents excess producers
   - Verify max_publish_rate throttles high-rate producers
   - Verify max_message_size rejects large messages

2. **Consumer Tests**
   - Verify max_consumers_per_topic and per_subscription limits
   - Verify max_subscription_dispatch_rate throttles dispatch

3. **End-to-End Tests**
   - Create topic with policies
   - Add producers/consumers up to limits
   - Verify behavior at limits
   - Verify policy updates propagate correctly

### Performance Tests

1. **Overhead Measurement**
   - Measure latency added by policy checks
   - Measure throughput impact of rate limiters
   - Verify locks don't cause contention

2. **Scale Tests**
   - Test with many topics (100s)
   - Test with many producers/consumers (1000s)
   - Verify policies scale linearly

---

## Metrics and Monitoring

### Metrics to Add

**File:** `danube-broker/src/broker_metrics.rs`

1. **POLICY_VIOLATION_COUNTER**
   - Labels: policy_type, topic_name, violation_reason
   - Tracks when policies reject operations

2. **RATE_LIMIT_THROTTLE_COUNTER**
   - Labels: rate_type (publish/dispatch/subscription), topic_name
   - Tracks rate limiting events

3. **PRODUCER_COUNT_GAUGE**
   - Labels: topic_name
   - Current producer count per topic

4. **CONSUMER_COUNT_GAUGE**
   - Labels: topic_name, subscription_name
   - Current consumer count per subscription

5. **MESSAGE_SIZE_HISTOGRAM**
   - Labels: topic_name
   - Distribution of message sizes

6. **RATE_LIMITER_UTILIZATION_GAUGE**
   - Labels: rate_type, topic_name
   - Current utilization percentage of rate limits

### Logging

Add structured logging at key enforcement points:
- Policy violations (WARN level)
- Rate limiting events (INFO level when starting, WARN when sustained)
- Policy configuration changes (INFO level)

---

## Error Handling

### gRPC Status Codes

1. **Resource Exhausted** (`Code::ResourceExhausted`)
   - For count-based limit violations (max producers, consumers, subscriptions)
   - For rate limit violations

2. **Invalid Argument** (`Code::InvalidArgument`)
   - For message size violations
   - For invalid policy values

3. **Failed Precondition** (`Code::FailedPrecondition`)
   - When policy prevents operation that might succeed later

### Error Messages

All policy violation errors should include:
1. Clear description of what limit was exceeded
2. Current value and limit value
3. Suggested action for the client
4. Topic/subscription name for context

Example:
```
"Producer limit reached for topic /namespace/topic-name. Current: 10, Limit: 10. Wait for existing producers to disconnect or increase max_producers_per_topic policy."
```

---

## Configuration Updates

### Policy Configuration Structure

The current config structure is good. Ensure documentation includes:

1. **Config File Comments** (`config/danube_broker.yml`)
   - Explain each policy's purpose
   - Provide recommended values for different use cases
   - Explain 0 = unlimited

2. **Policy Hierarchy Documentation**
   - Explain how broker -> namespace -> topic override works
   - Provide examples of setting policies at each level

3. **Rate Limit Interpretation**
   - Clarify what "rate" means (messages/sec, bytes/sec, or both)
   - Document token bucket parameters (burst allowance, refill rate)

---

## Dependencies and Prerequisites

### No New External Dependencies
The implementation can use existing Rust standard library and Tokio primitives:
- `std::time::Instant` for time tracking
- `std::sync::atomic::AtomicU64` for lock-free counters
- `tokio::sync::Mutex` for rate limiter state
- Existing `DashMap` for concurrent access

### Code Structure
- Add `rate_limiter.rs` module
- Update `mod.rs` to include rate_limiter
- Add getter methods to `policies.rs`
- No changes to proto definitions needed

---

## Migration and Backward Compatibility

### Existing Deployments
- All policies default to 0 (unlimited), maintaining current behavior
- No breaking changes to APIs or protocols
- Existing topics continue to work without policies until explicitly set

### Rollout Strategy
1. Deploy with all policies set to 0 (unlimited)
2. Monitor metrics to establish baseline usage
3. Gradually enable policies on test namespaces
4. Roll out to production namespaces based on observed patterns

---

## Summary of File Changes

### New Files
1. `danube-broker/src/rate_limiter.rs` - Token bucket rate limiter implementation

### Modified Files
1. `danube-broker/src/policies.rs` - Add getter methods for all policy fields
2. `danube-broker/src/topic.rs` - Add rate limiters, validation methods, helper methods
3. `danube-broker/src/subscription.rs` - Add rate limiter field, policy checks
4. `danube-broker/src/producer.rs` - Minor changes if needed for policy checks
5. `danube-broker/src/consumer.rs` - Minor changes if needed for policy checks
6. `danube-broker/src/broker_server/producer_handler.rs` - Add policy enforcement at producer creation and message send
7. `danube-broker/src/broker_server/consumer_handler.rs` - Add policy enforcement at consumer subscribe
8. `danube-broker/src/broker_service.rs` - Implement `allow_subscription_creation` logic
9. `danube-broker/src/topic_control.rs` - Enhance policy hierarchy resolution
10. `danube-broker/src/dispatcher/unified_single.rs` - Rate limit integration
11. `danube-broker/src/dispatcher/unified_multiple.rs` - Rate limit integration
12. `danube-broker/src/broker_metrics.rs` - Add new metrics for policy enforcement

### No Changes Needed
- `danube-broker/src/main.rs` - Policies already loaded from config
- `danube-broker/src/service_configuration.rs` - Policies already in config structs
- Proto definitions - No protocol changes required

---

## Conclusion

This implementation plan provides a comprehensive approach to enforcing all 8 broker policies throughout the Danube broker codebase. The plan follows the natural flow of requests through the system, ensuring policies are enforced at the appropriate points with minimal performance overhead.

The phased approach allows for incremental implementation and testing, with count-based policies providing immediate value while rate-based policies are developed. The suggested additional policies provide future enhancement opportunities based on operational needs.

All enforcement points have been identified with specific file locations and function names, making implementation straightforward for the development team.
