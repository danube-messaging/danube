# Phase 3: Reliable Dispatch with Persistent Subscription Cursors

## Introduction: Why this is needed
For reliable delivery, consumers must resume from the last acknowledged position after reconnection or broker reallocation. Without persisted cursors, consumers can only resume from "now" which risks duplicates or gaps depending on retention. Phase 3 introduces persisted per-subscription cursors and hydration so that reliable consumers resume exactly from the last committed position.

## Scope
- Server-side changes in reliable dispatch and broker to persist and hydrate cursors.
- Optional minor client changes (none strictly required if broker hydration is complete).

## Design Goals
- Persist per-subscription cursor under metadata storage.
- Ensure cursors survive broker restarts and broker reallocation.
- Handle retention (pruned segments) gracefully.

## Implementation Plan

- __[Cursor format and location]__
  - Store cursor at `/topics/{ns}/{topic}/subscriptions/{sub}/cursor`.
  - Content should include at minimum: segment_id and offset (or logical message ID) sufficient for `danube-reliable-dispatch` to position reads.
  - Consider versioning the cursor schema for future compatibility.

- __[Ack path persistence]__
  - In broker `Topic::ack_message()` and subscription dispatcher for reliable mode, after in-memory advancement, persist `{sub}/cursor` to metadata.
  - To reduce overhead:
    - Batch updates or debounce (e.g., write every N acks or every T milliseconds).
    - Ensure idempotent writes.

- __[Hydration on assignment and reattach]__
  - On topic assignment to a new broker, during topic hydration, fetch each subscription’s cursor and initialize the reliable dispatcher state to that position.
  - On consumer reattach, the broker uses the hydrated cursor to start delivering from the saved position (no client changes required).

- __[Retention and missing segments]__
  - If the cursor references a pruned segment (older than retention), advance the cursor to the earliest available segment and log a warning/metric.
  - Ensure `TopicStore`/`TopicCache` can query earliest available segment for a topic.

- __[Concurrency & correctness]__
  - Use atomic, ordered updates when multiple acks are in-flight per subscription.
  - Guard against out-of-order updates (e.g., compare greater-or-equal positions before persisting).

- __[Observability]__
  - Metrics: last persisted cursor lag vs delivered position, cursor write errors, hydration duration.
  - Logs: cursor set/advance, retention adjustments, reattach resume points.

## Testing Strategy
- Unit tests:
  - Cursor serialization/deserialization and ordering comparisons.
  - Ack path: cursor advances, batching logic, idempotency.
- Integration tests:
  - Publish messages in reliable mode, consume and ack part-way, restart broker or reassign topic, reattach consumer and verify resume from last ack.
  - Retention case: simulate pruning and verify cursor advancement to earliest available.

## Acceptance Criteria
- Reliable consumers resume from last acknowledged position after reconnect or reallocation.
- Cursors persist across broker restarts and rebalancing.
- Retention edge cases handled gracefully without panics.
