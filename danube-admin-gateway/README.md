# danube-admin-gateway

A Backend-for-Frontend (BFF) service that provides a unified HTTP/JSON API for the Danube Admin UI. 

It aggregates data from multiple sources, like broker administrative endpoints and Prometheus metrics, and delivers page-ready payloads optimized for the frontend.

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

**Examples**

```bash
curl -s http://localhost:8080/ui/v1/cluster | jq
{
  "timestamp": "2025-11-21T04:59:41.467789717+00:00",
  "brokers": [
    {
      "broker_id": "16769495701206859101",
      "broker_addr": "http://0.0.0.0:6650",
      "broker_role": "Cluster_Leader",
      "broker_status": "active",
      "stats": {
        "topics_owned": 5,
        "rpc_total": 14686,
        "active_connections": 24,
        "errors_5xx_total": 0
      }
    },
    {
      "broker_id": "3823634821110504384",
      "broker_addr": "http://0.0.0.0:6651",
      "broker_role": "Cluster_Follower",
      "broker_status": "active",
      "stats": {
        "topics_owned": 6,
        "rpc_total": 3967,
        "active_connections": 9,
        "errors_5xx_total": 0
      }
    },
    {
      "broker_id": "9235619178526211712",
      "broker_addr": "http://0.0.0.0:6652",
      "broker_role": "Cluster_Follower",
      "broker_status": "active",
      "stats": {
        "topics_owned": 5,
        "rpc_total": 18232,
        "active_connections": 17,
        "errors_5xx_total": 0
      }
    }
  ],
  "totals": {
    "broker_count": 3,
    "topics_total": 16,
    "rpc_total": 36885,
    "active_connections": 50
  },
  "errors": []
}
```

### Notes
- **URL encoding**: Topic names include namespace prefix with slashes (e.g., `/default/my-topic`). Encode slashes as `%2F` in URLs.
- **HTTPS**: For HTTPS listener, use `https://` and add `-k` if testing with self-signed certificates.
- **jq**: Install `jq` for pretty JSON output, or omit `| jq` to see raw JSON.
