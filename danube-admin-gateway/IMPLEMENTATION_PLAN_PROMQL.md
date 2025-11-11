# Danube Admin Gateway: PromQL Migration Implementation Plan

This plan migrates the gateway from scraping per-broker /metrics text and manually parsing to using Prometheus as the metrics backend and querying via PromQL over the HTTP API.

## Goals
- Remove brittle text scraping and custom parser.
- Centralize metrics collection in Prometheus.
- Use PromQL to compute values (instant and rates) and receive structured JSON.
- Maintain current API shapes and caching behavior.

## High-Level Architecture Changes
- Brokers export metrics at /metrics (unchanged).
- Prometheus server scrapes brokers and stores TSDB (new external dependency).
- Gateway becomes a Prometheus API client for queries.

## Config Changes
- Remove:
  - `metrics_scheme`, `metrics_port`, `metrics_path`.
- Add:
  - `prometheus_base_url` (e.g., http://localhost:9090).
  - Keep `metrics_timeout_ms` (request timeout to Prometheus).
- Optional:
  - Auth support if Prometheus is secured (Basic/Bearer env vars).

## Dependencies
- DIY approach (recommended for control/minimal deps):
  - reqwest = { version = "0.12", features = ["json", "rustls-tls"] }
  - serde = { version = "1", features = ["derive"] }
  - serde_json = "1"
  - url = "2.5"
  - tokio = { version = "1", features = ["full"] }
- Alternative: prometheus-http-query = "0.7" (reduces boilerplate).

## Code Changes by File

- main.rs
  - Replace CLI args for metrics with `--prometheus-base-url` and `--metrics-timeout-ms`.
  - Construct `MetricsClient { base_url, timeout }`.

- metrics.rs
  - Replace `MetricsClient` internals:
    - Fields: `base_url: String`, `http: reqwest::Client`.
    - Methods:
      - `query(&self, promql: &str) -> Result<PromResponse>` (instant vector).
      - `query_range(&self, promql: &str, start, end, step) -> Result<PromResponseRange>` (optional for charts).
    - Remove: `scrape_host_port`, `base_port`, `parse_prometheus`.
  - Define serde structs for Prometheus API responses (instant and range).

- ui/shared.rs
  - Remove `resolve_metrics_endpoint` (no direct scraping).
  - Keep `fetch_brokers` for identity/labels and UI display.

- ui/cluster.rs
  - Replace `scrape_broker_metrics` with PromQL-based fetch:
    - Prefer batched queries returning vectors for all brokers.
    - Map results by `broker` label in Rust.
  - Queries:
    - topics_owned per broker: `danube_broker_topics_owned`
    - rpc_total per broker: `sum by (broker) (danube_broker_rpc_total)`
    - rpc_rate_1m per broker: `sum by (broker) (rate(danube_broker_rpc_total[1m]))`

- ui/broker.rs
  - Replace `scrape_metrics_and_topics` with queries filtered by `broker`:
    - topics_owned: `danube_broker_topics_owned{broker="<id>"}`
    - rpc_total: `sum(danube_broker_rpc_total{broker="<id>"})`
    - rpc_rate_1m: `sum(rate(danube_broker_rpc_total{broker="<id>"}[1m]))`
    - topic lists (vectors):
      - producers: `danube_topic_active_producers{broker="<id>"}`
      - consumers: `danube_topic_active_consumers{broker="<id>"}`
    - Merge producer/consumer series by `topic` label to build rows.

- ui/topic.rs
  - Replace host-targeted scrape with queries filtered by `topic`:
    - msg_in_total: `sum(danube_topic_messages_in_total{topic="<topic>"})`
    - producers: `sum(danube_topic_active_producers{topic="<topic>"})`
    - consumers: `sum(danube_topic_active_consumers{topic="<topic>"})`
    - Optional rates:
      - publish_rate_1m: `sum(rate(danube_topic_messages_in_total{topic="<topic>"}[1m]))`

## Data Model Assumptions
- Metrics expose labels:
  - `broker="<broker_id>"`
  - `topic="<topic_name>"`
- If topics span multiple brokers, use `sum by (...)` aggregations.

## Error Handling & Timeouts
- Return partial data with `errors: Vec<String>` populated on query failures.
- Respect `metrics_timeout_ms` in the reqwest client.
- Handle empty results gracefully (treat as zero).

## Caching
- Keep existing DTO caching unchanged.
- Cache after successful Prometheus queries.

## Testing Plan
- Run Prometheus locally scraping brokers.
- Verify:
  - /ui/v1/cluster shows expected totals per broker.
  - /ui/v1/brokers/{id} shows topics and counts.
  - /ui/v1/topics/{topic} matches sums across brokers.
- Induce missing metric and verify zeros + error entry.
- Optional: latency test to ensure TTL works well.

## Rollout Plan
- Feature flag behind presence of `PROMETHEUS_BASE_URL` (if absent, keep current behavior until fully removed).
- After verification, remove legacy scraping code and flags.

## Acceptance Criteria
- All three endpoints function using only Prometheus queries.
- No dependency on broker /metrics endpoints at runtime.
- Parser and scraper code removed.
- CI/tests (manual or automated) pass basic validations above.

---

## Implementation Checklist

- [ ] Update Cargo.toml dependencies (reqwest, serde, url, tokio).
- [ ] Add `--prometheus-base-url` CLI arg and plumb into `MetricsClient`.
- [ ] Create new `MetricsClient::query` (+ optional `query_range`).
- [ ] Remove `parse_prometheus`, `scrape_host_port`, `base_port`.
- [ ] Remove `resolve_metrics_endpoint` and its usages.
- [ ] cluster.rs: implement batched queries and map by `broker`.
- [ ] broker.rs: implement per-broker queries and topic vector joins.
- [ ] topic.rs: implement per-topic queries with sums.
- [ ] Error handling: bubble up errors as strings in `errors` fields.
- [ ] Validate results against running Prometheus.
- [ ] Remove legacy flags/env and dead code.
- [ ] Update README/notes with new Prometheus requirement and labels.
