# Metrics Implementation Plan (Broker + WAL + Cloud)

## Goals
- **Expand coverage** across reliable and non-reliable paths.
- **Low-cardinality labels** (namespace, topic, partition, subscription, broker_id, reliable).
- **Expose Prometheus /metrics** with counters, gauges, histograms.
- **Trace key paths** (publish → WAL → dispatch → ack) with OTEL spans.

## Taxonomy
- Counters: `danube_<area>_<thing>_total`
- Gauges: `danube_<area>_<thing>`
- Histograms: `danube_<area>_<thing>_{ms|bytes}`

## Cluster & RPC
Files: `danube-broker/src/danube_service/load_manager.rs`, RPC servers

- `danube_broker_topics_owned` (Gauge) — broker_id
- `danube_broker_assignments_total` (Counter) — broker_id, action
- `danube_leader_election_state` (Gauge) — state (per broker: 0 follower/NoLeader, 1 leader)
- `danube_broker_rpc_total` (Counter) — service, method, result
- `danube_client_redirects_total` (Counter) — reason

Notes:
- rpc_errors_total and retry_attempts_total are intentionally not implemented.

## Phase 1 — Broker Core (Topics, Producers, Consumers)
Files: `danube-broker/src/topic.rs`, `danube-broker/src/subscription.rs`, `danube-broker/src/dispatcher.rs`, `danube-broker/src/topic_worker.rs`, RPC handlers

- **Topic**
  - `danube_topic_messages_in_total` (Counter)
    - Labels: topic
    - Hook: `Topic.publish_message_async()`
  - `danube_topic_bytes_in_total` (Counter)
    - Labels: topic
    - Hook: `Topic.publish_message_async()`
  - `danube_topic_active_producers` (Gauge)
    - Labels: topic
    - Hook: producer create/close
  - `danube_topic_active_subscriptions` (Gauge)
    - Labels: topic
    - Hook: subscription create/remove; bulk decrement on topic close
  - `danube_topic_message_size_bytes` (Histogram)
    - Labels: topic
    - Hook: entry of `publish_message_async()` (observe payload length)

- **Producer (broker-side behavior)**
  - `danube_producer_send_total` (Counter)
    - Labels: topic, result, error_code
    - Hook: Producer RPC Send handler
  - `danube_producer_send_latency_ms` (Histogram)
    - Labels: topic
    - Hook: around send (append/dispatch)

- **Consumer / Subscription**
  - `danube_subscription_active_consumers` (Gauge)
    - Labels: topic, subscription
    - Hook: `Subscription.add_consumer()` (++) and on remove/disconnect (--)
  - `danube_consumer_messages_out_total` / `danube_consumer_bytes_out_total` (Counters)
    - Labels: topic, subscription
    - Hook: when a message is delivered to consumer (Consumer::send_message)

### Policy and Rate-Limiter Metrics (deferred wiring; quick wins)

These complement the core metrics and are easy to add when we enable metrics work. They align with the "Metrics to Add" section below; this subsection records exact hook points in code.

- `POLICY_VIOLATION_COUNTER` (Counter: `danube_policy_violation_total`)
  - Labels: policy_type, topic_name, violation_reason
  - Hooks:
    - `topic.rs`: on `create_producer`/`subscribe`/`publish_message_async` when policy checks fail (producer/subscription/consumers/message_size).
    - `broker_server/producer_handler.rs`, `broker_server/consumer_handler.rs`: mirror early rejects for fast-fail visibility.

- `RATE_LIMIT_THROTTLE_COUNTER` (Counter: `danube_rate_limit_throttle_total`)
  - Labels: rate_type (publish|nonreliable_dispatch|reliable_dispatch), topic_name[, subscription_name]
  - Hooks:
    - `topic.rs`: `publish_message_async` when publish limiter denies (warn-only path).
    - `subscription.rs`: `send_message_to_dispatcher` when per-sub limiter denies (warn-only for NonReliable).
    - `dispatcher/subscription_engine.rs`: `poll_next()` when pacing (Reliable) backs off due to limiter (increment once per backoff cycle).

- `MESSAGE_SIZE_HISTOGRAM` (Histogram: `danube_topic_message_size_bytes`)
  - Labels: topic
  - Hook: `topic.rs` at entry of `publish_message_async` (observe `payload.len()` before checks/persist).

- `PRODUCER_COUNT_GAUGE` (Gauge: `danube_topic_active_producers`)
  - Labels: topic
  - Hooks: `topic.rs` after successful `create_producer` (++) and on producer close/cleanup (--).

- `CONSUMER_COUNT_GAUGE` (Gauge: `danube_subscription_active_consumers`)
  - Labels: topic, subscription
  - Hooks: `subscription.rs` after successful `add_consumer` (++) and on `remove_consumer`/drop (--).

- `RATE_LIMITER_UTILIZATION_GAUGE` (Gauge: `danube_rate_limiter_utilization`)
  - Labels: rate_type, topic_name
  - Note: optional follow-up; requires exposing utilization (e.g., tokens remaining vs capacity or rolling acquire rate).
    - Hook: Subscribe RPC handler
  - `danube_consumer_active` (Gauge)
    - Labels: namespace, topic, subscription, sub_type
    - Hook: `Subscription.add_consumer()` / disconnect
  - `danube_consumer_receive_total` (Counter)
    - Labels: namespace, topic, subscription
    - Hook: upon dispatch to consumer
  - `danube_consumer_ack_total` (Counter)
    - Labels: namespace, topic, subscription, result
    - Hook: `Topic.ack_message()`
  - `danube_consumer_lag_messages` (Gauge)
    - Labels: namespace, topic, subscription, reliable
    - Hook: see Backlog calc above

- **Dispatcher / Queues**
  - `danube_dispatcher_queue_depth` (Gauge)
    - Labels: namespace, topic, subscription, reliable
    - Hook: queue length per subscription
  - `danube_dispatcher_enqueue_total` / `danube_dispatcher_dequeue_total` (Counters)
    - Labels: namespace, topic, subscription
  - `danube_dispatcher_commit_latency_ms` (Histogram)
    - Labels: namespace, topic, subscription, reliable
    - Hook: store→ack complete time (reliable), enqueue→deliver (non-reliable)

## Phase 2 — Reliable Storage (WAL)
Files: `danube-persistent-storage/src/wal_storage.rs`, `wal.rs`, `wal/writer.rs`, `wal/stateful_reader.rs`, `wal/cache.rs`, `wal/deleter.rs`

- Implemented (topic-labeled):
  - `danube_wal_append_total` (Counter)
    - Labels: topic
    - Hook: `WalStorage::append_message` on success
  - `danube_wal_append_bytes_total` (Counter)
    - Labels: topic
    - Hook: `WalStorage::append_message` on success
  - `danube_wal_flush_latency_ms` (Histogram)
    - Labels: topic
    - Hook: `writer::process_flush` around write+flush
  - `danube_wal_fsync_total` (Counter)
    - Labels: topic
    - Hook: `writer::process_flush` after flush
  - `danube_wal_file_rotate_total` (Counter)
    - Labels: topic, reason in {size,time}
    - Hook: `writer::rotate_if_needed`
  - `danube_wal_reader_create_total` (Counter)
    - Labels: topic, mode in {wal_only,cloud_then_wal}
    - Hook: `WalStorage::create_reader`
  - `danube_wal_delete_total` (Counter)
    - Labels: topic, reason="retention"
    - Hook: `wal/deleter.rs::run_cycle` after deletions

- Skipped by design:
  - `danube_wal_fsync_batch_bytes`
  - `danube_wal_checkpoint_write_total`
  - `danube_wal_reader_phase_transitions_total`
  - `danube_wal_delete_bytes_total`

Next Steps:
- Implement Phase 3 Cloud metrics and handoff.
- Implement Phase 4 Subscription progress metrics and dashboards.

## Phase 3 — Cloud Tier (OpenDAL)
Files: `danube-persistent-storage/src/cloud/storage.rs`, `cloud/uploader_stream.rs`, `cloud/uploader.rs`

- **Upload / List / Download**
  - `danube_cloud_upload_objects_total` (Counter)
    - Labels: namespace, topic, partition, provider, result
  - `danube_cloud_upload_bytes_total` (Counter)
    - Labels: namespace, topic, partition, provider
  - `danube_cloud_upload_object_size_bytes` (Histogram)
    - Labels: namespace, topic, partition, provider
  - `danube_cloud_upload_latency_ms` (Histogram)
    - Labels: provider
  - `danube_cloud_list_total` / `danube_cloud_list_latency_ms` (Counters/Histogram)
    - Labels: provider
  - `danube_cloud_download_bytes_total` (Counter) [if read from cloud]
    - Labels: namespace, topic, partition, provider
  - `danube_handoff_cloud_to_wal_total` (Counter)
    - Labels: namespace, topic, partition, path={cloud→wal-tail}

## Phase 4 — Subscription Progress (Reliable)
Files: dispatcher engines / progress persistence (if present)

- `danube_sub_progress_offset` (Gauge)
  - Labels: namespace, topic, subscription, partition
- `danube_sub_pending_messages` (Gauge)
  - Labels: namespace, topic, subscription
- `danube_sub_redelivery_total` (Counter)
  - Labels: namespace, topic, subscription, reason={timeout,nack}


## Export & Dashboards
- Add Prometheus `/metrics` HTTP endpoint in broker process.
- Provide example Grafana dashboards:
  - Topic Overview: rates, bytes, backlog, lag, active producers/consumers
  - Reliable Storage: WAL append/fsync/rotate, backlog, sub progress
  - Cloud Upload: objects/bytes/latency, object size distribution, error rate
  - Broker/Cluster: ownership, assignments, leader state

## Labeling & Cardinality Guidelines
- Prefer: namespace, topic, subscription, partition, broker_id, provider, reliable.
- Avoid: unbounded IDs (producer_id, consumer_id) as labels; include them only in logs/traces.

## Implementation Notes (Hooks)
- `topic.rs`: counters/gauges and dispatch/ack latency.
- `subscription.rs` & `dispatcher/*`: consumer gauges, enqueue/dequeue, redeliveries, commit latency.
- `topic_worker.rs`: optional internal queue depth.
- `wal/writer.rs`: append/fsync/rotate/checkpoint.
- `wal/stateful_reader.rs`: reader create/read and latency.
- `wal/cache.rs`: hits/misses.
- `wal/deleter.rs`: deletions with reason.
- `cloud/*`: upload/list/download, size/latency, errors.

## Rollout Plan
- Phase 1: Broker core metrics and `/metrics` endpoint.
- Phase 2: WAL metrics and reliable backlog/lag.
- Phase 3: Cloud metrics and handoff.
- Phase 4: Subscription progress metrics and dashboards.
