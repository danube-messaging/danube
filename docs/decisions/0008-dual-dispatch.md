# 0008: Non-reliable and reliable dispatch side by side

**Status:** Accepted

## Decision

Each topic has one dispatch strategy, fixed at creation:

- **Non-reliable:** messages are kept in memory and pushed to connected consumers. No WAL and no redelivery after a broker restart. Lowest latency. Used by `realtime` and request/reply topics.
- **Reliable:** messages are appended to the WAL (sync level from the intent), delivered with ack tracking, a sliding window of unacked messages (`max_unacked_messages`), retries with backoff, and DLQ or block on poison messages.

The topic's strategy is the source of truth once it exists. A producer's requested strategy only decides the strategy of an auto-created topic (`danube produce -r` or `with_reliable_dispatch()`). For an existing topic:

- a non-reliable request on a reliable topic is accepted: the producer gets a stronger guarantee.
- a reliable request on a non-reliable topic is rejected: the producer expects durability it would not get.

The rule is asymmetric because `NonReliable` is the proto default (0), so the broker cannot tell "not set" from "explicitly non-reliable".

## Why

- Telemetry, heartbeats and token streaming should not pay for disk I/O. Task queues and audit logs must not lose messages.
- Running both in one cluster avoids deploying two systems.

## Subscription semantics to remember

- Exclusive, Shared, Failover and Key-Shared subscriptions work on both strategies.
- A **new** subscription on a reliable topic starts at the latest offset; history is reachable with `danube topic subscription seek` or `topic branch`.
- Key-Shared uses consistent hashing (100 virtual nodes) with per-key in-flight limits on reliable topics.

## Revisit if

Producers should adopt the topic's strategy automatically instead of declaring it. That is a likely UX improvement: the mismatch error is a common stumbling block.

