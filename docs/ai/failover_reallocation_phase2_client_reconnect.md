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
  - Treat retryable failures: tonic `Status` codes such as `Unavailable`, `DeadlineExceeded`, `ResourceExhausted`, and server `ErrorType::ServiceNotReady` (`danube-core/src/proto/danube.rs`).
  - Implement exponential backoff with full jitter (base 100–500ms, cap ~5–10s) in client retry loops.
  - Add per-operation retry budget/timeout to bound retries for `create_producer`, `subscribe`, and send/receive paths.

- __[Lookup and redirect hint]__
  - On retry, perform topic lookup via `LookupService::handle_lookup()` in `danube-client/src/lookup_service.rs`.
  - If the server provides `ErrorMessage.redirect_to`, prefer it to avoid an extra lookup; otherwise use `TopicLookupResponse::LookupType::{Redirect, Connect}` to decide next hop.
  - Invalidate any cached routing in between retries (LookupService currently resolves fresh per call; keep this invariant).

- __[Producer reconnect flow]__
  - On `create_producer` failure with `SERVICE_NOT_READY`, `TopicProducer::create()` already retries with lookup and reconnect; harden with backoff + jitter and a retry budget.
  - On send failures classified as retryable, close/reset stream, redo lookup, recreate the producer by name (idempotent on the server), and resend.
  - For `ALREADY_EXISTS`, surface a clear error for true duplicates; for retries, ensure we reuse existing producer state after reconnect if the same name is accepted.

- __[Consumer reconnect flow]__
  - On subscribe or receive termination with retryable errors, redo lookup via `LookupService::handle_lookup()`, reconnect, and reattach by `subscription` + `consumer_name` using `TopicConsumer::subscribe()`.
  - Delivery from “now” in Phase 2; exact resume is deferred to Phase 3.

- __[Validate while active]__
  - Use `HealthCheckService::start_health_check()` (`danube-client/src/health_check.rs`) to periodically validate routing; if `ClientStatus::Close` is returned, trigger stop-and-reconnect.
  - If repeated `TOPIC_NOT_FOUND` / `SUBSCRIPTION_NOT_FOUND` occur mid-flight, trigger re-lookup to detect ownership changes.

- __[Telemetry]__
  - Emit structured logs and counters around: lookup result types, redirect usage, retry attempts, backoff delays, reconnect outcomes, and health-check closures.
  - Candidate sites: `TopicProducer::create()`, `TopicProducer::send()`, `TopicConsumer::subscribe()`, `TopicConsumer::receive()`, `LookupService::handle_lookup()`.

## Testing Strategy
- Unit tests (client crate): backoff strategy, redirect hint usage, retry budget enforcement, idempotent create behaviors.
  - Target modules: `lookup_service.rs`, `topic_producer.rs`, `topic_consumer.rs`.
- Integration tests:
  - Reuse/extend `danube-broker/tests/producer_reconnect.rs` and add client-focused tests to simulate broker disconnects, asserting fast reconnect and continuity (non-reliable).
- Chaos-style (optional): intermittent disconnects; assert bounded recovery time and absence of thundering herd effects.

## Acceptance Criteria
- Producers and consumers reconnect automatically under retryable failures with exponential backoff + jitter.
- Redirect hints (when present) are honored to reduce latency.
- Producer/consumer creation is idempotent by name during retries.
- Active connections recover when ownership changes mid-flight.

## Code Mapping to Current Implementation

- __Lookup and Redirect__
  - `danube-client/src/lookup_service.rs` — `LookupService::handle_lookup()` decides between `Redirect` and `Connect` based on `TopicLookupResponse` and errors with `ErrorType::ServiceNotReady`.

- __Producer paths__
  - `danube-client/src/topic_producer.rs` — `TopicProducer::create()` implements retry-on-`SERVICE_NOT_READY` with a follow-up lookup; `send()` surfaces errors (site to add retry-with-backoff for transient transport failures).

- __Consumer paths__
  - `danube-client/src/topic_consumer.rs` — `TopicConsumer::subscribe()` performs lookup+connect and starts health check; `receive()` establishes the stream (site to add reconnect on stream termination with retryable errors).

- __Active validation__
  - `danube-client/src/health_check.rs` — `HealthCheckService::start_health_check()` periodically checks status; `ClientStatus::Close` signals client to close and reconnect.

- __Error model__
  - `danube-client/src/errors.rs` — `DanubeError`, `decode_error_details()` to pull `ErrorMessage` (with `redirect_to`) from gRPC metadata.
  - `danube-core/src/proto/danube.rs` — `ErrorType`, `ErrorMessage.redirect_to`, `TopicLookupResponse::LookupType`.

## Operational Flows (Phase 2)

- __Producer create__
  - create -> if OK: start health check
  - if `SERVICE_NOT_READY`: backoff+jitter -> `handle_lookup()` -> connect new addr -> retry (budgeted)

- __Producer send__
  - send -> if transport retryable or `SERVICE_NOT_READY`: close stream -> backoff+jitter -> `handle_lookup()` -> recreate by name -> resend

- __Consumer subscribe__
  - lookup -> connect -> subscribe -> start health check
  - if `ALREADY_EXISTS` on legitimate retry, ensure idempotent semantics at server and reuse client state

- __Consumer receive__
  - streaming -> if stream ends with retryable error: backoff+jitter -> `handle_lookup()` -> resubscribe
