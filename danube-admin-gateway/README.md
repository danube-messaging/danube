# danube-admin-gateway

A Backend-for-Frontend (BFF) service that provides a unified HTTP/JSON API for the Danube Admin UI. It aggregates data from multiple sources—broker administrative endpoints and Prometheus metrics—and delivers page-ready payloads optimized for the frontend.

## What it does

- **Single-call page endpoints**: Each UI page gets all its data in one request (cluster overview, broker details, topic metrics).
- **Aggregated data**: Combines administrative metadata (brokers, topics, subscriptions) with real-time metrics (message counts, producer/consumer stats).
- **Automatic discovery**: Finds all brokers in the cluster on-demand; no manual configuration required.
- **Built-in caching**: Short TTL cache (default 3s) reduces load on brokers and speeds up responses.
- **Configurable metrics scraping**: Pulls Prometheus metrics from brokers with configurable timeouts and endpoints.
- **Error tolerance**: Returns partial results with error details when some brokers are unreachable.

## Build & run

From workspace root:

```bash
cargo run -p danube-admin-gateway -- \
  --broker-endpoint 127.0.0.1:50051 \
  --listen-addr 0.0.0.0:8080 \
  --request-timeout-ms 800 \
  --per-endpoint-cache-ms 3000 \
  --metrics-port 9040
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

### Metrics scraping
- `--metrics-scheme` (default: http): Scheme for Prometheus endpoints
- `--metrics-port` (default: 9040): Port where brokers expose Prometheus metrics
- `--metrics-path` (default: /metrics): Path for Prometheus scrape
- `--metrics-timeout-ms` (default: 800): Timeout per metrics scrape

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

### GET /ui/v1/brokers/{broker_id}
Detailed broker view: identity, metrics, and list of topics assigned to the broker with producer/consumer counts.

### GET /ui/v1/topics/{topic}
Topic details: schema, subscriptions, and aggregated metrics (messages in/out, active producers/consumers). Topic name must be URL-encoded (e.g., `/default/my-topic` becomes `%2Fdefault%2Fmy-topic`).

## curl examples

Assuming the gateway runs on `http://localhost:8080`:

```bash
# Health check
curl -s http://localhost:8080/ui/v1/health | jq

# Cluster overview (all brokers + metrics)
curl -s http://localhost:8080/ui/v1/cluster | jq

# Broker details (replace broker_id with actual ID from cluster response)
curl -s http://localhost:8080/ui/v1/brokers/63161296830406433 | jq

# Topic details (topic name /default/test_topic must be URL-encoded)
curl -s http://localhost:8080/ui/v1/topics/%2Fdefault%2Ftest_topic | jq

# Another topic example: /default/partitioned_topic-part-0
curl -s http://localhost:8080/ui/v1/topics/%2Fdefault%2Fpartitioned_topic-part-0 | jq
```

### Notes
- **URL encoding**: Topic names include namespace prefix with slashes (e.g., `/default/my-topic`). Encode slashes as `%2F` in URLs.
- **HTTPS**: For HTTPS listener, use `https://` and add `-k` if testing with self-signed certificates.
- **jq**: Install `jq` for pretty JSON output, or omit `| jq` to see raw JSON.
