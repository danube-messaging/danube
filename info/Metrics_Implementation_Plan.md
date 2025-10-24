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

## Phase 1 — Broker Core (Topics, Producers, Consumers)
Files: `danube-broker/src/topic.rs`, `danube-broker/src/subscription.rs`, `danube-broker/src/dispatcher.rs`, `danube-broker/src/topic_worker.rs`, RPC handlers

- **Topic**
  - `danube_topic_messages_in_total` (Counter)
    - Labels: namespace, topic, partition, producer_name
    - Hook: `Topic.publish_message_async()`
  - `danube_topic_bytes_in_total` (Counter)
    - Labels: namespace, topic, partition, producer_name
    - Hook: `Topic.publish_message_async()`
  - `danube_topic_active_producers` (Gauge)
    - Labels: namespace, topic
    - Hook: producer create/close
  - `danube_topic_active_subscriptions` (Gauge)
    - Labels: namespace, topic
    - Hook: subscription create/remove
  - `danube_topic_dispatch_latency_ms` (Histogram)
    - Labels: namespace, topic, subscription, sub_type, reliable
    - Hook: before `send_message_to_dispatcher()` to post-enqueue/ack-gate
  - `danube_topic_backlog_messages` (Gauge)
    - Labels: namespace, topic, subscription, reliable
    - Hook: non-reliable: dispatcher queue depth; reliable: WAL end − sub progress

- **Producer (broker-side behavior)**
  - `danube_producer_create_total` (Counter)
    - Labels: namespace, topic, result, error_type
    - Hook: Producer RPC Create handler
  - `danube_producer_send_total` (Counter)
    - Labels: namespace, topic, partition, result, error_type, reliable
    - Hook: Producer RPC Send handler
  - `danube_producer_send_latency_ms` (Histogram)
    - Labels: namespace, topic, partition, reliable
    - Hook: around send (append/dispatch)

- **Consumer / Subscription**
  - `danube_consumer_subscribe_total` (Counter)
    - Labels: namespace, topic, subscription, sub_type, result
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

- **Append/Flush/Rotate**
  - `danube_wal_append_total` (Counter)
    - Labels: namespace, topic, partition, file_seq
    - Hook: successful frame append
  - `danube_wal_append_bytes_total` (Counter)
    - Labels: namespace, topic, partition
  - `danube_wal_append_latency_ms` (Histogram)
    - Labels: namespace, topic, partition
    - Hook: enqueue→fsync, or fsync batch latency
  - `danube_wal_fsync_total` (Counter)
    - Labels: namespace, topic, partition
  - `danube_wal_fsync_batch_bytes` (Histogram)
    - Labels: namespace, topic, partition
  - `danube_wal_file_rotate_total` (Counter)
    - Labels: namespace, topic, partition, reason={size,time}
  - `danube_wal_checkpoint_write_total` (Counter)
    - Labels: namespace, topic, partition

- **Readers / Cache / Deleter**
  - `danube_wal_reader_create_total` (Counter)
    - Labels: namespace, topic, partition, start_pos
  - `danube_wal_reader_read_total` (Counter)
    - Labels: namespace, topic, partition
  - `danube_wal_reader_read_latency_ms` (Histogram)
    - Labels: namespace, topic, partition
  - `danube_wal_cache_hits_total` / `danube_wal_cache_misses_total` (Counters)
    - Labels: namespace, topic, partition
  - `danube_wal_delete_total` (Counter)
    - Labels: namespace, topic, partition, reason={retention,compaction}

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

## Cluster & RPC
Files: `danube-broker/src/danube_service/load_manager.rs`, RPC servers

- `danube_broker_topics_owned` (Gauge) — broker_id
- `danube_broker_assignments_total` (Counter) — broker_id, action
- `danube_leader_election_state` (Gauge) — state
- `danube_broker_rpc_total` / `danube_admin_rpc_total` (Counter) — method, result
- `danube_rpc_errors_total` (Counter) — service, code
- `danube_client_redirects_total` (Counter) — service, reason
- `danube_retry_attempts_total` (Counter) — component, reason

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
