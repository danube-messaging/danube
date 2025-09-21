# Durable Subscription Progress: Design & Implementation Plan

## Goals
- Persist per-subscription consumption progress (cursor) for Reliable subscriptions to enable correct resume behavior across client reconnects and broker restarts.
- Batch updates to the metadata store (etcd) to avoid write amplification by flushing at a configurable interval (default 5 seconds).
- Preserve current defaults: brand-new subscriptions start at `StartPosition::Latest` unless a specific offset is requested by the client.

## Scope
- Broker-only changes (no client API changes required).
- Applies to Reliable subscriptions only. Non-reliable remains fire-and-forget (no persistence).

## Data Model
- Key: `/danube/subscriptions/<namespace>/<topic>/<subscription>/cursor`
- Value (JSON):
  ```json
  {
    "offset": 12345,
    "updated_at": 1726937845
  }
  ```
- Stored in etcd via `danube-metadata-store` (`EtcdMetadata`).

## Configuration
Extend `config/danube_broker.yml` under the `wal_cloud.metadata` section:
```yaml
wal_cloud:
  metadata:
    etcd_endpoint: "127.0.0.1:2379"
    in_memory: false
    subscription_progress_flush_secs: 5
```
- `subscription_progress_flush_secs` controls the periodic flush interval. Default: 5 seconds.

## Components & Changes
- `danube-broker/src/dispatcher/subscription_engine.rs`
  - Add a `TopicResources`-backed progress integration (no extra traits):
    - `progress_resources: Option<tokio::sync::Mutex<TopicResources>>`
    - `last_acked: Option<u64 /*offset*/>`, `dirty: bool`, `last_flush_at: Instant`, `flush_interval: Duration`
  - Constructors:
    - `new(subscription_name: String, topic_store: Arc<dyn TopicStoreLike>)`
    - `new_with_progress(subscription_name: String, topic_name: String, topic_store: Arc<dyn TopicStoreLike>, progress_resources: TopicResources, flush_interval: Duration)`
  - Init methods:
    - `init_stream_latest()` (existing)
    - `init_stream_from_progress_or_latest()`
      - If cursor found in `TopicResources`: start with `StartPosition::Offset(cursor + 1)`
      - Else: `StartPosition::Latest`
  - Ack handling:
    - `on_acked(request_id, msg_id)` sets `last_acked = Some(msg_id.segment_offset)` and `dirty = true`
    - Debounced flush: if `now - last_flush_at >= flush_interval` and `dirty`, write `offset` via `TopicResources::set_subscription_cursor(...)`, then clear `dirty`

- `danube-broker/src/resources/topic.rs`
  - Cursor helpers added:
    - `set_subscription_cursor(topic, subscription, offset)`
    - `get_subscription_cursor(topic, subscription) -> Option<u64>`
  - Cursor stored at `/topics/<topic>/subscriptions/<subscription>/cursor` as a JSON number (offset)

- `danube-broker/src/subscription.rs` and dispatchers
  - `Subscription::create_new_dispatcher(...)` now accepts optional `TopicResources` and `flush_interval`
  - Reliable dispatchers (`UnifiedSingleDispatcher`, `UnifiedMultipleDispatcher`) are created with `SubscriptionEngine::new_with_progress(...)` when resources are available
  - Reliable dispatchers call `init_stream_from_progress_or_latest()` on startup

- `danube-persistent-storage`
  - `wal.rs`: `tail_reader()` now injects the assigned WAL offset into outgoing messages:
    - For replayed items and live tail, set `msg.msg_id.segment_offset = wal_offset` before yielding
  - This guarantees `SubscriptionEngine::on_acked()` receives the durable WAL offset

## Resume Scenarios
- Client disconnect/reconnect (broker stays up):
  - Re-use in-memory `SubscriptionEngine`; no read from etcd required
- Broker restart (fresh process) or subscription migration:
  - On new engine creation, `init_stream_from_progress_or_latest()` consults etcd and resumes at `Offset(cursor + 1)`
  - If no cursor: start at `Latest`

## Testing
- Unit tests (colocated):
  - `subscription_engine_test.rs`
    - Persist-on-ack: after ack, use a short test `flush_interval` (e.g., 50ms), verify cursor updated via `TopicResources`
    - Resume-from-progress: pre-seed cursor; verify reader starts at expected offset by observing delivered messages
- Integration tests (`danube-broker/tests/`):
  - `reliable_dispatch_resume.rs`:
    - Produce N messages; consume and ack K; wait flusher tick; recreate engine (simulated restart); verify resume from K+1

## Operational Notes
- The default policy for brand-new subscriptions remains `StartPosition::Latest` unless the client explicitly requests an offset
- The flush interval is configurable and defaults to 5 seconds to limit etcd writes
- Cursor format is minimal and forward-compatible

## Rollout Plan
1. Implement `TopicResources` cursor helpers (done)
2. Wire `SubscriptionEngine` progress state and `init_stream_from_progress_or_latest()` (done)
3. Update dispatchers to initialize from progress or Latest (done)
4. Inject WAL offsets into outgoing `StreamMessage.msg_id.segment_offset` (done)
5. Parse `subscription_progress_flush_secs` from config and pass to engine (next)
6. Add unit and integration tests for persist/resume (next)
7. Verify `tests/reliable_dispatch_basic.rs` remain green; add `reliable_dispatch_resume.rs` (next)
8. Update documentation to reference the new config flag and resume semantics (done here)
