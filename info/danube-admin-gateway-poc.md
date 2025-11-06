# Danube Admin Gateway (BFF) – POC Implementation Plan

This plan defines a minimal Backend-for-Frontend (BFF) service named `danube-admin-gateway` that exposes a REST/JSON API for an external Admin UI and talks to Danube brokers over gRPC using the existing Admin services defined in `danube-core/proto/DanubeAdmin.proto`. The POC focuses on server implementation, security, and first read-only endpoints. The UI is out of scope here.


## Goals (POC scope)

- Expose a small, stable REST surface for the Admin UI without modifying brokers.
- Use current Broker Admin gRPC services for data: `BrokerAdmin`, `NamespaceAdmin`, `TopicAdmin`.
- Centralize security, CORS, rate limiting, and request logging.
- Keep the broker focused on messaging; BFF runs as a separate service in-cluster.
- Defer Prometheus/time-series aggregation to Phase 2 (not required for the initial POC).


## Service name and placement

- Crate/Bin: `danube-admin-gateway`
- Runs in the same cluster as brokers (Deployment/Service), horizontally scalable.
- External traffic terminates at the gateway (HTTPS), gateway talks to brokers via mTLS gRPC.


## Dependencies and stack (aligned with workspace)

- Runtime: `tokio` (workspace)
- gRPC client: `tonic` (workspace)
- HTTP server: `axum` (new dependency) over `hyper`
- TLS: `rustls` (workspace) via `axum-server` or `hyper-rustls`
- JSON: `serde`, `serde_json` (workspace)
- Observability: `tracing`, `tracing-subscriber` (workspace)
- Auth: `openidconnect` or `oauth2` crate (new dependency) for JWT verification (POC can accept HS256/RS256 configured JWKs)
- Rate limiting/middleware: `tower` (comes with axum)

Rationale: axum integrates cleanly with tokio/tower/rustls already in workspace and keeps the server minimal.


## Security model (POC)

- External → Gateway
  - HTTPS (TLS via rustls). Provide `--tls-cert` and `--tls-key` flags.
  - Optional OIDC/JWT validation (POC can support disabled mode first, then enable with issuer/JWKS URL, audience).
  - Strict CORS to the Admin UI origin.
  - Per-IP (or per-subject) rate limit.

- Gateway → Brokers
  - mTLS gRPC using broker-issued or platform-issued certs.
  - Broker addresses provided via config (static list or seed + DNS).

- Authorization (POC)
  - Role mapping from JWT claims (e.g., `role: viewer|operator|admin`).
  - POC endpoints are read-only; require `viewer` role or above.


## Configuration (CLI/env)

- `--listen-addr` (default: 0.0.0.0:8080)
- `--tls-cert`, `--tls-key` (PEM)
- `--broker-endpoints` (comma-separated `host:port` for Admin gRPC)
- `--grpc-ca-cert`, `--grpc-client-cert`, `--grpc-client-key` (mTLS)
- `--oidc-issuer`, `--oidc-audience` (optional; enable JWT validation if set)
- `--cors-allow-origin`
- `--request-timeout-ms` (default 800)
- `--per-endpoint-cache-ms` (default 1000)


## Broker discovery (POC)

- POC uses a static list from `--broker-endpoints`.
- Future: call `BrokerAdmin.ListBrokers` to refresh the pool periodically.


## Endpoints (POC, read-only)

All endpoints return JSON. These map directly to existing gRPC without needing proto changes.

- GET `/api/v1/health`
  - Returns gateway status and a quick broker reachability check (best-effort ping `GetLeaderBroker`).

- GET `/api/v1/brokers`
  - Maps to: `BrokerAdmin.ListBrokers`
  - Response: `[ { broker_id, broker_addr, broker_role } ]`

- GET `/api/v1/leader`
  - Maps to: `BrokerAdmin.GetLeaderBroker`
  - Response: `{ leader: string }`

- GET `/api/v1/namespaces`
  - Maps to: `BrokerAdmin.ListNamespaces`
  - Response: `{ namespaces: string[] }`

- GET `/api/v1/topics?namespace=<ns>`
  - Maps to: `TopicAdmin.ListTopics` or `NamespaceAdmin.GetNamespaceTopics`
  - Response: `{ topics: string[] }`

- GET `/api/v1/topics/{topic}/subscriptions`
  - Maps to: `TopicAdmin.ListSubscriptions`
  - Response: `{ subscriptions: string[] }`

- GET `/api/v1/topics/{topic}`
  - Maps to: `TopicAdmin.DescribeTopic`
  - Response: `{ name, type_schema, schema_data(base64), subscriptions[] }`

Notes:
- Aggregation across brokers: for these specific RPCs, the admin service should already serve cluster-scoped answers (leader node or internal cache). If a broker returns redirect/not-leader, the gateway retries with the leader.
- Partial/degraded behavior and caching: brief TTL (e.g., 1s) for `/brokers` and `/namespaces` to smooth UI refresh.


## Mapping to proto (from danube-core/proto/DanubeAdmin.proto)

- BrokerAdmin
  - `ListBrokers(Empty) -> BrokerListResponse`
  - `GetLeaderBroker(Empty) -> BrokerResponse`
  - `ListNamespaces(Empty) -> NamespaceListResponse`
- NamespaceAdmin
  - `GetNamespaceTopics(NamespaceRequest) -> TopicListResponse`
- TopicAdmin
  - `ListTopics(NamespaceRequest) -> TopicListResponse`
  - `ListSubscriptions(TopicRequest) -> SubscriptionListResponse`
  - `DescribeTopic(DescribeTopicRequest) -> DescribeTopicResponse`

POC excludes mutations (Create/Delete/Unload/Activate/Unsubscribe).


## Error handling and timeouts

- Per-request timeout (default 800ms) for gRPC calls.
- If multiple brokers exist, try leader endpoint or hedge to an alternate broker if not-leader or unavailable.
- Return meaningful HTTP status codes:
  - 200 OK on success
  - 206 Partial Content when serving cached/partial broker data (if applicable later)
  - 400 for invalid params
  - 401/403 for auth failures
  - 502/504 when upstream (brokers) fail/timeout


## Tracing and metrics

- Structured logs with request IDs, latency, upstream broker target, and outcome.
- Expose gateway Prometheus metrics at `/metrics` (Phase 2 optional).
- Propagate trace headers if present.


## Directory layout (new crate)

- `danube-admin-gateway/`
  - `src/main.rs` (CLI, config, server bootstrap)
  - `src/http.rs` (routes, request/response models)
  - `src/grpc.rs` (tonic clients, leader selection/hedging)
  - `src/auth.rs` (JWT/OIDC middleware)
  - `src/errors.rs`
  - `Cargo.toml`


## Data models (REST)

Define REST DTOs to mirror/reshape gRPC responses with serde:
- `BrokerDto { broker_id, broker_addr, broker_role }`
- `LeaderDto { leader }`
- `NamespacesDto { namespaces: Vec<String> }`
- `TopicsDto { topics: Vec<String> }`
- `SubscriptionsDto { subscriptions: Vec<String> }`
- `DescribeTopicDto { name, type_schema, schema_data, subscriptions }`
- `HealthDto { status, leader_reachable, generated_at }`


## Implementation steps

1) Bootstrap crate
- Create `danube-admin-gateway` crate in workspace.
- Add deps: `axum`, `tower`, `axum-server` (or `hyper-rustls`), `openidconnect` (optional POC), plus workspace deps (`tokio`, `tonic`, `serde`, `serde_json`, `tracing`).
- Wire CLI/config and TLS listener.

2) gRPC clients
- Generate/use tonic stubs from `DanubeAdmin.proto` via `danube-core` (reuse generated types if exported; else add a local build step referencing the same proto file to avoid duplication).
- Implement client pool with per-call timeout and simple leader retry.

3) REST routes (POC)
- Implement endpoints:
  - GET `/api/v1/health`
  - GET `/api/v1/brokers`
  - GET `/api/v1/leader`
  - GET `/api/v1/namespaces`
  - GET `/api/v1/topics`
  - GET `/api/v1/topics/:topic`
  - GET `/api/v1/topics/:topic/subscriptions`
- Add CORS, logging, and optional JWT middleware.

4) Caching & resilience
- In-memory TTL cache for `/brokers`, `/namespaces` (1s default).
- Request-level timeout; translate gRPC status to HTTP.

5) Packaging & runbooks
- Provide example config and TLS notes.
- Kubernetes manifest snippet (optional POC).


## Phase 2 (not in POC, for later)

- Add Prometheus integration: scrape per-broker `/metrics`, or query Prometheus API; add UI endpoints for topic rates and consumer lag.
- Add mutations with RBAC enforcement (`CreateTopic`, `DeleteTopic`, `ActivateBroker`, `UnloadBroker`, `Unsubscribe`).
- Dynamic broker discovery via `ListBrokers` and background refresh.
- Pagination/filters for large topic sets.
- OpenAPI spec generation for the REST API.


## Notes on Local Cache and Metrics

- Local cache in each broker provides a cluster view sourced from etcd. The Admin gRPC already uses this; the gateway can rely on Admin RPCs to get cluster-wide data without querying etcd directly.
- Broker Prometheus metrics are available per-node; aggregation will be done in the gateway in Phase 2 or delegated to Prometheus with API queries.
