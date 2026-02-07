# Danube Client Review Plan (Merged)

This plan merges the original client review plan (info/04_danube_client_improvements.md) with the review plan I proposed (info/05_danube_client_review_plan.md). The goal is to keep the most impactful refactors while adding small polish items that improve readability and idiomatic Rust style.

## Goals
- Idiomatic Rust (avoid panics, clearer state, consistent logging)
- Clear, teachable structure (easy to port to other languages)
- Obvious performance wins only

## Phase 0 — API clarity + idiomatic baseline ✅
1. ~~**Builders return `Result` instead of panicking**~~ ✅
   - Changed `ConsumerBuilder::build()` and `ProducerBuilder::build()` to `Result<T>`. Updated all 112 callers.

2. ~~**Replace `eprintln!` with `tracing`**~~ ✅
   - Replaced all `eprintln!` with `tracing::{warn, error, info}` in `consumer.rs`, `producer.rs`, and `errors.rs`.

3. ~~**Idiomatic defaults for `Option` values**~~ ✅
   - Replaced verbose `if let Some` with `unwrap_or()` / `unwrap_or_default()` in `consumer.rs`, `topic_consumer.rs`, `producer.rs`.

4. ~~**Imports to top of file**~~ ✅
   - Moved mid-file and function-body `use` statements to top in `producer.rs` and `topic_producer.rs`.

## Phase 1 — Cross-cutting structural cleanup ✅
5. ~~**Centralize retry/backoff logic**~~ ✅
   - Extracted `try_create()`/`lookup_new_broker()` in `TopicProducer`, `try_subscribe()`/`lookup_new_broker()` in `TopicConsumer`.
   - Extracted `select_partition()`/`recreate_producer()`/`lookup_and_recreate()` in `Producer`, `resubscribe()` in `Consumer`.
   - Removed redundant max_retries re-defaulting everywhere (RetryManager already handles it).

6. ~~**Unify auth token insertion**~~ ✅
   - Single implementation: `AuthService::insert_token_if_needed()`. Removed duplicates from `LookupService`, `HealthCheckService`. `RetryManager` delegates to it.

7. ~~**Fix `MessageRouter` round‑robin race**~~ ✅
   - Replaced non-atomic `load`+`store` with single `fetch_add(1, Relaxed) % partitions`.

8. ~~**Replace magic numbers with named constants/enums**~~ ✅
   - `ClientType::Producer`/`Consumer` enum for health check (from proto).
   - `DEFAULT_MAX_RETRIES`, `DEFAULT_BASE_BACKOFF_MS`, `DEFAULT_MAX_BACKOFF_MS` in `RetryManager`.
   - `RECEIVE_CHANNEL_BUFFER`, `GRACEFUL_CLOSE_DELAY_MS` in `Consumer`.
   - `HEALTH_CHECK_INTERVAL_SECS` in `HealthCheckService`.
   - `TOKEN_EXPIRY_SECS` in `AuthService`.

## Phase 2 — Consumer/Producer refactor for readability
9. **Refactor `Consumer::receive` into smaller functions**
   - Extract per-partition loop and use structured shutdown signaling.
   - Consider a `Stream` return type (only if acceptable API change).

10. **Make `TopicConsumer` and `TopicProducer` state machines**
    - Replace `Option` + `unwrap()` with explicit states (Disconnected/Connected/Registered).

11. **Stop mutating `DanubeClient.uri` inside topic actors**
    - Store per-topic broker URI inside topic actors instead of mutating the shared client.

12. **Reduce clone-heavy send paths**
    - Consider `Bytes` or `Arc<Vec<u8>>` for payload to reduce repeated cloning.

## Phase 3 — Schema registry + structural polish
13. **Schema registry error ergonomics**
    - Use typed errors instead of stringly `Unrecoverable` for schema resolution.

14. **Reduce schema registry boilerplate**
    - Extract internal helper for connect/auth/unwrap/error mapping.

15. **Clean up `ProducerOptions` / `ConsumerOptions`**
    - Remove `others` or mark structs `#[non_exhaustive]` for forward compatibility.

16. **Fix `DanubeClientBuilder::build()` double ConnectionManager creation**
    - Create `ConnectionManager` once and pass through.

17. **Minor cleanups**
    - Avoid `unwrap()` in `decode_error_details`.
    - Validate `proxy` flag in `ConnectionManager`.
    - Consider `Display`/`FromStr` for schema enums.

## Phase 4 — Broker alignment (only if needed)
18. **Broker error metadata alignment**
    - Ensure `error-message-bin` consistently encodes retryable errors if client retry classification is unreliable.

## Recommended Execution Order
1 → 2 → 4 → 3 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13 → 14 → 15 → 16 → 17 → 18

## Notes
- Focus first on readability/idioms before structural refactors.
- If any public API changes are needed (e.g., returning `Stream`), document and confirm before implementation.
