# danube-admin-gateway

A Backend-for-Frontend (BFF) service that provides a unified HTTP/JSON API for the Danube Admin UI. It aggregates data from multiple sources—broker administrative endpoints and Prometheus metrics—and delivers page-ready payloads optimized for the frontend.

## What it does

- **Single-call page endpoints**: Each UI page gets all its data in one request (cluster overview, broker details, topic metrics).
- **Aggregated data**: Combines administrative metadata (brokers, topics, subscriptions) with real-time metrics (message counts, producer/consumer stats).
- **Automatic discovery**: Finds all brokers in the cluster on-demand; no manual configuration required.
- **Built-in caching**: Short TTL cache (default 3s) reduces load on brokers and speeds up responses.
- **Prometheus client (PromQL)**: Queries a Prometheus server via HTTP API (instant and range queries) instead of scraping broker text directly.
- **Error tolerance**: Returns partial results with error details when some brokers are unreachable.

## Build & run

From workspace root:

```bash
cargo run -p danube-admin-gateway -- \
  --broker-endpoint 127.0.0.1:50051 \
  --listen-addr 0.0.0.0:8080 \
  --request-timeout-ms 800 \
  --per-endpoint-cache-ms 3000 \
  --prometheus-base-url http://localhost:9090 \
  --metrics-timeout-ms 800
```

With HTTPS for the HTTP listener:

```bash
cargo run -p danube-admin-gateway -- \
  --broker-endpoint 127.0.0.1:50051 \
  --listen-addr 0.0.0.0:8443 \
  --tls-cert ./server.crt \
  --tls-key ./server.key
```

With gRPC TLS/mTLS to brokers (CLI overrides env vars):

```bash
cargo run -p danube-admin-gateway -- \
  --broker-endpoint https://broker.example.com:50051 \
  --grpc-enable-tls true \
  --grpc-domain broker.example.com \
  --grpc-ca ./ca.pem \
  --grpc-cert ./client.crt \
  --grpc-key ./client.key
```

## Configuration options

### Admin gRPC connection
- `--broker-endpoint` (required): Seed broker address (e.g., `127.0.0.1:50051`)
- `--request-timeout-ms` (default: 800): Timeout for gRPC calls to brokers
- `--grpc-enable-tls`, `--grpc-domain`, `--grpc-ca`, `--grpc-cert`, `--grpc-key`: TLS/mTLS config for broker connection

### Prometheus metrics
- `--prometheus-base-url` (default: http://localhost:9090): Prometheus HTTP API base URL
- `--metrics-timeout-ms` (default: 800): Timeout per Prometheus HTTP call

### Server & caching
- `--listen-addr` (default: 0.0.0.0:8080): Gateway HTTP listen address
- `--tls-cert`, `--tls-key`: Enable HTTPS for the gateway server
- `--per-endpoint-cache-ms` (default: 3000): TTL for page-level caches

### Environment variables (fallbacks for gRPC TLS)
- `DANUBE_ADMIN_TLS=true`: Enable TLS for broker connections
- `DANUBE_ADMIN_DOMAIN`: TLS SNI/verification domain
- `DANUBE_ADMIN_CA`: Path to PEM root CA
- `DANUBE_ADMIN_CERT` / `DANUBE_ADMIN_KEY`: mTLS client cert/key paths

## API endpoints

Base URL defaults to `http://localhost:8080`.

### GET /ui/v1/health
Health check with broker leader reachability.

### GET /ui/v1/cluster
Cluster overview: all brokers with roles and aggregated metrics (topics owned, RPC totals, etc.).

### GET /ui/v1/topics
Cluster-wide topics summary aggregated from Prometheus. Returns a list of topics with totals of producers, consumers, and subscriptions across the cluster, plus broker identities.

### GET /ui/v1/brokers/{broker_id}
Detailed broker view: identity, metrics, and list of topics assigned to the broker with producer/consumer counts.

### GET /ui/v1/topics/{topic}
Topic details: schema, subscriptions, and aggregated metrics (messages in/out, active producers/consumers). Topic name must be URL-encoded (e.g., `/default/my-topic` becomes `%2Fdefault%2Fmy-topic`).

### GET /ui/v1/topics/{topic}/series
Chart-ready time series for the topic using Prometheus range queries. Query params:
- `from`: unix seconds
- `to`: unix seconds
- `step`: Prometheus step (e.g, `15s`, `30s`, `1m`)

Returns an array of named series with points: `(ts_ms, value)`.

### GET /ui/v1/namespaces
Lists all namespaces, their topics, and current policies (as a JSON string returned by the admin service). Deduplicates namespaces.

### POST /ui/v1/topics/actions
Execute topic actions via Admin gRPC. Single endpoint supporting multiple actions.

Request JSON:

```json
{ "action": "create|delete|unload", "topic": "/ns/topic" | "topic", "namespace": "ns?", "partitions": 3?, "schema_type": "String|Bytes|Int64|Json?", "schema_data": "{}"?, "dispatch_strategy": "non_reliable|reliable?" }
```

Notes:
- When `topic` lacks a namespace, provide `namespace` and the service will normalize to `/ns/topic`.
- `create` with `partitions` creates a partitioned topic; otherwise creates a non-partitioned topic.
- `dispatch_strategy` defaults to `non_reliable`. `schema_type` defaults to `String`. `schema_data` defaults to `{}`.

Response JSON:

```json
{ "success": true, "message": "ok" }
```

### POST /ui/v1/cluster/actions
Execute broker actions via Admin gRPC. Single endpoint supporting multiple actions.

Request JSON:

```json
{
  "action": "unload|activate",
  "broker_id": "...",
  // unload (optional)
  "max_parallel": 1,
  "namespaces_include": ["ns-a"],
  "namespaces_exclude": ["ns-b"],
  "dry_run": false,
  "timeout_seconds": 60,
  // activate (optional)
  "reason": "admin_activate"
}
```

Notes:
- `broker_id` is required for all actions.
- `unload` accepts tuning fields; sensible defaults are applied if omitted.
- `activate` accepts an optional `reason` for auditability (default: `admin_activate`).

Response JSON:

```json
{ "success": true, "message": "started=true total=10 succeeded=0 failed=0 pending=10" }
```

## curl examples

Assuming the gateway runs on `http://localhost:8080`:

```bash
# Health check
curl -s http://localhost:8080/ui/v1/health | jq

# Cluster overview (all brokers + metrics)
curl -s http://localhost:8080/ui/v1/cluster | jq

# Cluster-wide topics summary
curl -s http://localhost:8080/ui/v1/topics | jq

# Broker details (replace broker_id with actual ID from cluster response)
curl -s http://localhost:8080/ui/v1/brokers/63161296830406433 | jq

# Topic details (topic name /default/test_topic must be URL-encoded)
curl -s http://localhost:8080/ui/v1/topics/%2Fdefault%2Ftest_topic | jq

# Another topic example: /default/partitioned_topic-part-0
curl -s http://localhost:8080/ui/v1/topics/%2Fdefault%2Fpartitioned_topic-part-0 | jq

# Namespaces with topics and policies
curl -s http://localhost:8080/ui/v1/namespaces | jq

# Topic actions
# Create non-partitioned topic
curl -s -X POST http://localhost:8080/ui/v1/topics/actions \
  -H 'content-type: application/json' \
  -d '{
    "action":"create",
    "topic":"mytopic",
    "namespace":"default",
    "schema_type":"String",
    "dispatch_strategy":"reliable"
  }' | jq

# Create partitioned topic
curl -s -X POST http://localhost:8080/ui/v1/topics/actions \
  -H 'content-type: application/json' \
  -d '{
    "action":"create",
    "topic":"/default/partitioned_topic",
    "partitions":3,
    "schema_type":"Json",
    "schema_data":"{\"a\":1}",
    "dispatch_strategy":"non_reliable"
  }' | jq

# Delete a topic
curl -s -X POST http://localhost:8080/ui/v1/topics/actions \
  -H 'content-type: application/json' \
  -d '{"action":"delete","topic":"/default/mytopic"}' | jq

# Unload a topic
curl -s -X POST http://localhost:8080/ui/v1/topics/actions \
  -H 'content-type: application/json' \
  -d '{"action":"unload","topic":"mytopic","namespace":"default"}' | jq

# Cluster actions
# Unload a broker
curl -s -X POST http://localhost:8080/ui/v1/cluster/actions \
  -H 'content-type: application/json' \
  -d '{
    "action": "unload",
    "broker_id": "63161296830406433",
    "max_parallel": 2,
    "namespaces_include": ["default"],
    "dry_run": false,
    "timeout_seconds": 60
  }' | jq

# Activate a broker
curl -s -X POST http://localhost:8080/ui/v1/cluster/actions \
  -H 'content-type: application/json' \
  -d '{
    "action": "activate",
    "broker_id": "63161296830406433",
    "reason": "admin_activate"
  }' | jq
```

### Notes
- **URL encoding**: Topic names include namespace prefix with slashes (e.g., `/default/my-topic`). Encode slashes as `%2F` in URLs.
- **HTTPS**: For HTTPS listener, use `https://` and add `-k` if testing with self-signed certificates.
- **jq**: Install `jq` for pretty JSON output, or omit `| jq` to see raw JSON.
