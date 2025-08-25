# Phase 2: Client Reconnect Improvements

## Introduction: Why this is needed
During broker failover or rebalancing, producers and consumers may experience transient errors or disconnects and must quickly reconnect to the correct broker. Today, clients retry, but lack structured backoff, redirect-hint usage, and validation while active connections are still sending/receiving. Phase 2 hardens client behavior to minimize downtime and avoid thundering herds while ensuring correctness.

## Scope
- Client library changes only (no server-side behavior required beyond optional redirect hints).
- Applies to both producers and consumers.
- Non-reliable and reliable modes benefit equally; exact resume for reliable is addressed in Phase 3.

## Design Goals
- Fast, correct re-resolution to the right broker after errors.
- Idempotent producer/consumer creation by name to handle retries.
- Reduce load spikes with exponential backoff + jitter.
- Validate routing even on active connections (detect stale ownership).

## Implementation Plan

- __[Error handling and retry policy]__
  - Treat GRPC status codes like `Unavailable`, `DeadlineExceeded`, `ResourceExhausted`, and custom `ServiceNotReady` as retryable.
  - Implement exponential backoff with full jitter (e.g., base 100-500ms, cap ~5-10s).
  - Add per-operation retry budget/timeout to avoid infinite retries.

- __[Lookup and redirect hint]__
  - On retry, perform topic lookup. If `ErrorMessage.redirect_to` is returned by the server, prefer it to avoid an extra lookup.
  - Ensure lookup caches are invalidated on retry to prevent stale assignments.

- __[Producer reconnect flow]__
  - On send failure due to connection errors or broker signaling reassign:
    - Close connection, redo lookup/redirect, recreate producer by name (idempotent) and resend.
  - On `create_producer` failure with “already exists” semantics, assume idempotency and reuse the existing state.

- __[Consumer reconnect flow]__
  - On stream termination with retryable errors:
    - Redo lookup/redirect and reattach to subscription by name.
  - Server-side will deliver from “now” in Phase 2; exact resume handled in Phase 3.

- __[Validate while active]__
  - Periodically (or on broker hint) validate current routing by issuing a lightweight ownership check or handling `ServiceNotReady` mid-flight by triggering re-lookup.
  - Add a guard to re-check routing if repeated `NotFound` on topic/producer/consumer operations occurs.

- __[Telemetry]__
  - Emit structured logs and counters: reconnect attempts, backoff delays, redirect usage, final success/failure.

## Testing Strategy
- Unit tests: backoff strategy, redirect hint usage, retry budget enforcement, idempotent create behaviors.
- Integration tests (mock or local): simulate broker disconnects and verify fast reconnect and message continuity (non-reliable).
- Chaos-style test (optional): intermittent disconnects to assert bounded recovery time and lack of herd effects.

## Acceptance Criteria
- Producers and consumers reconnect automatically under retryable failures with exponential backoff + jitter.
- Redirect hints (when present) are honored to reduce latency.
- Producer/consumer creation is idempotent by name during retries.
- Active connections recover when ownership changes mid-flight.
