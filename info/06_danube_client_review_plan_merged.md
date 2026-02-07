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

## Phase 2 — Consumer/Producer refactor for readability ✅
9. ~~**Refactor `Consumer::receive` into smaller functions**~~ ✅
   - Extracted `partition_receive_loop()` standalone async function from the `tokio::spawn` closure. `receive()` is now a short method that spawns tasks.

10. ~~**Make `TopicConsumer` and `TopicProducer` state machines**~~ ✅
    - Added `ProducerState` enum (`Disconnected` / `Ready { stream_client, producer_id }`) in `TopicProducer`.
    - Added `ConsumerState` enum (`Disconnected` / `Ready { stream_client, consumer_id }`) in `TopicConsumer`.
    - Eliminated all `Option::unwrap()` calls — methods now match on state and return proper errors.
    - `connect()` returns the client instead of storing it; state transitions are atomic in `try_create()`/`try_subscribe()`.

11. ~~**Stop mutating `DanubeClient.uri` inside topic actors**~~ ✅
    - Added `broker_addr: Uri` field to both `TopicProducer` and `TopicConsumer`, initialized from `client.uri`.
    - All methods use `self.broker_addr` instead of `self.client.uri`.
    - `lookup_new_broker()` updates `self.broker_addr` instead of `self.client.uri`.
    - `Producer::lookup_and_recreate()` updated to use `producer.broker_addr`.

12. ~~**Reduce clone-heavy send paths**~~ ✅
    - Changed `TopicProducer::send` signature to borrow `&[u8]` + `Option<&HashMap<String, String>>` instead of taking ownership.
    - `Producer::send` passes `&data` and `attributes.as_ref()` — no clones in the retry loop.
    - Full zero-copy would require `Bytes` in `danube-core::StreamMessage` (deferred — cross-crate API change).

## Phase 3 — Schema registry + structural polish ✅
13. ~~**Schema registry error ergonomics**~~ ✅
    - Added `SchemaError(String)` variant to `DanubeError` for schema-specific errors.
    - Changed gRPC status wrapping from `Unrecoverable(format!(...))` to `FromStatus(status, None)` — preserves structured error info.
    - Schema validation errors (pinned version, min version) now use `SchemaError`.
    - Removed double-wrapping in `try_create()` — schema errors propagate directly with `?`.

14. ~~**Reduce schema registry boilerplate**~~ ✅
    - Extracted `prepare_request<T>()` helper in `SchemaRegistryClient` — handles connect, auth token, and client unwrap in one call.
    - All 7 public methods simplified from ~15 lines to ~6 lines each.

15. ~~**Clean up `ProducerOptions` / `ConsumerOptions`**~~ ✅
    - Removed unused `pub others: String` field from both structs.
    - Added `#[non_exhaustive]` for forward compatibility.

16. ~~**Fix `DanubeClientBuilder::build()` double ConnectionManager creation**~~ ✅
    - Set `api_key` on `connection_options` before creating the single `ConnectionManager`.
    - Build `DanubeClient` directly in `build()` — removed `new_client()`.
    - Removed dead `jwt_token` field from `ConnectionOptions`.

17. ~~**Minor cleanups**~~ ✅
    - Replaced `unwrap()` in `decode_error_details` with graceful `match` + `warn!` log.
    - Fixed inverted `proxy` flag: `proxy = broker_url != connect_url` (was backwards).
    - Added `Display` and `FromStr` for `CompatibilityMode` and `SchemaType` enums.

## Phase 4 — Broker alignment (only if needed)
18. **Broker error metadata alignment**
    - Ensure `error-message-bin` consistently encodes retryable errors if client retry classification is unreliable.

## Recommended Execution Order
1 → 2 → 4 → 3 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13 → 14 → 15 → 16 → 17 → 18

## Notes
- Focus first on readability/idioms before structural refactors.
- If any public API changes are needed (e.g., returning `Stream`), document and confirm before implementation.
