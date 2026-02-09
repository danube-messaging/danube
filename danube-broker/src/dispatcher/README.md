# Dispatcher Module

## Overview

The dispatcher module is the **core message routing engine** for Danube broker subscriptions. It handles message delivery from topics to consumers with different delivery guarantees (reliable/non-reliable) and distribution patterns (exclusive/shared).

## Architecture

### High-Level Component Hierarchy

```text
┌──────────────────────────────────────────────────────────────┐
│                      Subscription                            │
│  (owns dispatcher, manages lifecycle)                        │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         v
┌──────────────────────────────────────────────────────────────┐
│                   Dispatcher (Struct)                         │
│  Single facade: { control_tx, ready_rx }                     │
│  Factory constructors spawn the appropriate background task  │
├──────────────────────────────────────────────────────────────┤
│  Constructors:                                               │
│    - Dispatcher::non_reliable_exclusive()                    │
│    - Dispatcher::non_reliable_shared()                       │
│    - Dispatcher::reliable_exclusive(engine)                  │
│    - Dispatcher::reliable_shared(engine)                     │
└────────────────────────┬─────────────────────────────────────┘
                         │ spawns one of:
          ┌──────────────┼──────────────────────┐
          v              v                      v
┌──────────────┐ ┌──────────────┐  ┌─────────────────────────┐
│  exclusive/  │ │   shared/    │  │  Reliable variants use: │
│  non_reliable│ │  non_reliable│  │   SubscriptionEngine    │
│  reliable    │ │  reliable    │  │  (stream, progress,     │
└──────────────┘ └──────────────┘  │   lag detection)        │
                                   └─────────────────────────┘
```

## Key Concepts

### 1. Dispatcher Modes

#### Exclusive (Single Active Consumer)
- **Use Case**: Exclusive/Failover subscriptions
- **Behavior**: Only one consumer is active at a time
- **Failover**: When active consumer disconnects, next consumer becomes active
- **Examples**: Order processing, state machines, sequential workflows

#### Shared (Multiple Consumers, Round-Robin)
- **Use Case**: Shared/Round-Robin subscriptions
- **Behavior**: Messages distributed evenly across all consumers
- **Load Balancing**: Atomic round-robin counter ensures fair distribution
- **Examples**: Worker pools, parallel task processing, scalable microservices

### 2. Delivery Guarantees

#### Non-Reliable (At-Most-Once)
- **Acks**: No acknowledgment tracking
- **Dispatch**: Fire-and-forget, immediate send
- **Use Case**: Real-time streams, telemetry, non-durable subscriptions
- **Trade-off**: Maximum throughput, potential message loss

#### Reliable (At-Least-Once)
- **Acks**: Strict acknowledgment gating
- **Dispatch**: One message in-flight at a time, wait for ack
- **Retry**: Pending message buffer for automatic retry
- **Persistence**: Subscription cursor persisted to metadata
- **Watchdog**: 500ms heartbeat checks for lag
- **Use Case**: Durable subscriptions, financial transactions, critical data
- **Trade-off**: Guaranteed delivery, lower throughput

## High-Level Workflows

### Message Dispatch Flow

**Non-Reliable Dispatchers** (fire-and-forget):
- Topic → `dispatch_message()` → Select consumer → Send immediately → Return
- No ack tracking, no retry, minimal latency

**Reliable Dispatchers** (ack-gating):
- Topic notification OR heartbeat → `PollAndDispatch` command → Poll from SubscriptionEngine → Buffer message → Send to consumer → Wait for ack → Track progress → Clear pending → Repeat
- Full details in `exclusive/reliable.rs` and `shared/reliable.rs`

### Consumer Selection

**Exclusive**: Single active consumer selected from list, failover on disconnect  
**Shared**: Round-robin using atomic counter for fair distribution

### Reliability Mechanisms

1. **Ack-Gating**: Only one message in-flight at a time (pending buffer)
2. **Heartbeat Watchdog**: 500ms tick checks for lag, triggers dispatch if behind
3. **Progress Persistence**: Debounced cursor writes to metadata (5-second interval)
4. **Failover**: Pending message buffer ensures no loss during consumer disconnect

> **Note**: Detailed workflow diagrams and state machines are documented in each implementation file:
> - `exclusive/reliable.rs` - Full reliable exclusive flow with diagrams
> - `shared/reliable.rs` - Reliable shared with round-robin + ack-gating
> - `subscription_engine.rs` - Stream polling, progress tracking, lag detection flows

## Dispatcher Selection Guide

### When to Use Each Dispatcher Type

| Subscription Type | Consumers | Delivery Guarantee | Constructor | Use Case |
|-------------------|-----------|-------------------|-------------|----------|
| **Exclusive** | Single active | At-most-once | `Dispatcher::non_reliable_exclusive()` | Real-time exclusive feeds, non-durable |
| **Exclusive** | Single active | At-least-once | `Dispatcher::reliable_exclusive(engine)` | Order processing, state machines, durable exclusive |
| **Shared** | Multiple | At-most-once | `Dispatcher::non_reliable_shared()` | High-throughput workers, telemetry |
| **Shared** | Multiple | At-least-once | `Dispatcher::reliable_shared(engine)` | Task queues, parallel processing, durable shared |
| **Failover** | Multiple (1 active) | At-least-once | `Dispatcher::reliable_exclusive(engine)` | HA setups, backup consumers |

### Performance Characteristics

| Dispatcher | Throughput | Latency | Guarantees | Overhead |
|------------|-----------|---------|------------|----------|
| Non-Reliable Exclusive | High | Very Low | None | Minimal |
| Non-Reliable Shared | Very High | Low | None | Low |
| Reliable Exclusive | Medium | Medium | At-least-once | High (ack-gating + persistence) |
| Reliable Shared | Medium-High | Medium | At-least-once + Load balance | High (ack-gating + persistence) |


## SubscriptionEngine Responsibilities

1. **Stream Management**: Creates and manages `TopicStream` for reading from WAL/cloud
2. **Progress Tracking**: Maintains `last_acked` cursor (subscription position)
3. **Persistence**: Debounced writes to metadata store (5-second interval)
4. **Lag Detection**: Compares cursor against WAL head
5. **Rate Limiting**: Optional throttling of message dispatch

