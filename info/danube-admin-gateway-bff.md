# Danube Admin Gateway - BFF Endpoints (POC)

This document defines the Backend-for-Frontend (BFF) endpoints for the initial Admin UI POC. The goal is to provide page-ready payloads with a single request per page. The gateway aggregates Admin gRPC data and Prometheus metrics, applies small TTL caches, and returns JSON tailored for the UI.

## Principles

- Single-call-per-page BFF endpoints under `/ui/v1/*`.
- Compose existing Admin gRPC responses with broker/topic metrics.
- Short TTL caches (default 3s) for page payloads.
- Reasonable timeouts per upstream call (default 800ms) and per-page.
- Error-tolerant: partial results with an `errors` array when some scrapes fail.
- No leader selection required; brokers expose consistent cluster view.

## Configuration

- gRPC: uses existing CLI/env flags (TLS/mTLS supported).
- Metrics: brokers expose Prometheus on port `9040` (default), path typically `/metrics`.
  - Default target: `http://{broker_host}:9040/metrics`.
  - Make this configurable via CLI later if needed.

---

## Endpoint: Cluster page

- Method/Path: `GET /ui/v1/cluster`
- Purpose: List all brokers with role and summary stats; provide cluster totals.
- Response (example):
```json
{
  "timestamp": "2025-11-07T10:20:30Z",
  "brokers": [
    {
      "broker_id": "node-1",
      "broker_addr": "10.0.0.1:50051",
      "broker_role": "Leader",
      "stats": {
        "topics_owned": 120,
        "rpc_total": 42513,
        "rpc_rate_1m": 72.3,
        "active_connections": 54,
        "errors_5xx_total": 12
      }
    }
  ],
  "totals": {
    "broker_count": 3,
    "topics_total": 350,
    "rpc_total": 118234,
    "active_connections": 180
  },
  "errors": []
}
```
- Data sources:
  - gRPC: `BrokerAdmin.ListBrokers`
  - Metrics (per broker): `topics_owned`, `rpc_total`, `rpc_rate`, `active_connections`, `errors_5xx_total`
- Caching: 3s TTL for the whole payload.
- Timeouts: 800ms per metrics scrape; page budget ~2s.

---

## Endpoint: Broker page

- Method/Path: `GET /ui/v1/brokers/{broker_id}`
- Purpose: Detailed broker info including topics assigned to the broker and their producer/consumer counts.
- Response (example):
```json
{
  "timestamp": "2025-11-07T10:20:30Z",
  "broker": {
    "broker_id": "node-1",
    "broker_addr": "10.0.0.1:50051",
    "broker_role": "Leader"
  },
  "metrics": {
    "rpc_total": 42513,
    "rpc_rate_1m": 72.3,
    "topics_owned": 120,
    "producers_connected": 210,
    "consumers_connected": 180,
    "inbound_bytes_total": 123456789,
    "outbound_bytes_total": 987654321,
    "errors_5xx_total": 12
  },
  "topics": [
    {
      "name": "/public/my-topic",
      "producers_connected": 5,
      "consumers_connected": 9
    }
  ],
  "errors": []
}
```
- Data sources:
  - gRPC: `BrokerAdmin.ListBrokers` (resolve address/role for broker_id)
  - Metrics (target broker):
    - Broker-level metrics (see `broker_metrics.rs`).
    - Topic-level metrics filtered to those owned by this broker.
- Topic ownership determination:
  - Preferred: metrics include `owner_broker_id` label on topic series; filter by `{broker_id=...}`.
  - Alternative (if metrics lack labels): add Admin RPC for ownership in future; for POC, we may scan metrics exported by the broker to list topics it reports.
- Caching: 3s TTL for the broker payload.
- Timeouts: 800ms for scrape; page budget ~1.5s.

---

## Endpoint: Topic page

- Method/Path: `GET /ui/v1/topics/{topic}`
  - `topic` must be URL-encoded when it contains `/`.
- Purpose: Topic details (schema + subscriptions) with runtime & persistent metrics.
- Response (example):
```json
{
  "timestamp": "2025-11-07T10:20:30Z",
  "topic": {
    "name": "/public/my-topic",
    "type_schema": 0,
    "schema_data": "<base64>",
    "subscriptions": ["sub-1", "sub-2"]
  },
  "metrics": {
    "msg_in_total": 123456,
    "msg_out_total": 120111,
    "msg_backlog": 42,
    "storage_bytes": 9876543,
    "producers": 5,
    "consumers": 9,
    "publish_rate_1m": 31.2,
    "dispatch_rate_1m": 29.9
  },
  "errors": []
}
```
- Data sources:
  - gRPC: `TopicAdmin.DescribeTopic`, `TopicAdmin.ListSubscriptions`.
  - Metrics:
    - Runtime topic metrics from the owning broker’s Prometheus (`broker_metrics.rs`).
    - Persistent metrics from `persistent_metrics.rs` via the appropriate Prometheus endpoint(s).
- Owning broker:
  - Preferred: metrics indexed by topic with labels including broker_id.
  - Alternative: resolve via Admin metadata if available; else scrape all brokers and merge topic series (POC acceptable with timeout/partial fallback).
- Caching: 3s TTL; description/subscriptions cached; metrics cached separately or together.
- Timeouts: 800ms per upstream call; page budget ~2s.

---

## Error handling

- Map gRPC errors to HTTP as implemented (400/401/403/404/502/504).
- Metrics scrape failures should yield partial responses with `errors` array, not hard fail.

## Tracing & observability

- Add span fields per request: `page`, `cache_hit`, `grpc_codes`, `scrape_count`, `duration_ms`.
- Log upstream timings and error summaries.

## Code organization (refactor)

- `src/ui/cluster.rs` — handler for `/ui/v1/cluster`.
- `src/ui/broker.rs` — handler for `/ui/v1/brokers/{broker_id}`.
- `src/ui/topic.rs` — handler for `/ui/v1/topics/{topic}`.
- `src/metrics.rs` — minimal Prometheus scrape client and text parser helpers.
- `src/dto.rs` — BFF response structs (serde-serializable) shared by handlers.
- Wire routes in `main.rs`. Keep `grpc_client.rs` as-is for Admin gRPC.

## Open considerations

- Metrics endpoint configuration (port/path, TLS) — add CLI flags if needed.
- Ownership discovery without metrics labels — may require a lightweight Admin RPC in future.
- Pagination for large topic lists — defer until needed.

## Next steps

1) Implement `src/metrics.rs` with small scrape/parse helpers and timeouts.
2) Scaffold `src/ui/*` handlers and `src/dto.rs` with the above contracts.
3) Add routes `/ui/v1/cluster`, `/ui/v1/brokers/{broker_id}`, `/ui/v1/topics/{topic}` and 3s TTL caches.
4) Add tracing fields and log cache hits/misses.
