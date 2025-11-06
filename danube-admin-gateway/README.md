# danube-admin-gateway

A minimal Backend-for-Frontend (BFF) service for the Danube Admin UI. It exposes a simple REST/JSON API and calls Danube brokers over gRPC using the existing Admin services defined in `danube-core/proto/DanubeAdmin.proto`.

## What it does (POC)

- Provides read-only endpoints for cluster status, brokers, namespaces, topics, and subscriptions.
- Reuses generated gRPC stubs from `danube-core/src/proto/danube_admin.rs`.
- On-demand discovery: calls `BrokerAdmin.ListBrokers` when requested; no background refresh.
- Request timeouts and HTTP error mapping.
- Short TTL cache (default 3s) for `/brokers`, `/namespaces`, and per-namespace `/topics`.

## Build & run

From workspace root:

```
cargo run -p danube-admin-gateway -- \
  --broker-endpoint 127.0.0.1:50051 \
  --listen-addr 0.0.0.0:8080 \
  --request-timeout-ms 800 \
  --per-endpoint-cache-ms 3000
```

With HTTPS for the HTTP listener:

```
cargo run -p danube-admin-gateway -- \
  --broker-endpoint 127.0.0.1:50051 \
  --listen-addr 0.0.0.0:8443 \
  --tls-cert ./server.crt \
  --tls-key ./server.key
```

With gRPC TLS/mTLS to brokers (CLI overrides env vars):

```
cargo run -p danube-admin-gateway -- \
  --broker-endpoint https://broker.example.com:50051 \
  --grpc-enable-tls true \
  --grpc-domain broker.example.com \
  --grpc-ca ./ca.pem \
  --grpc-cert ./client.crt \
  --grpc-key ./client.key
```

Environment variables (fallbacks):
- `DANUBE_ADMIN_TLS=true` (enable TLS)
- `DANUBE_ADMIN_DOMAIN` (TLS SNI/verification)
- `DANUBE_ADMIN_CA` (PEM root CA)
- `DANUBE_ADMIN_CERT` / `DANUBE_ADMIN_KEY` (mTLS client cert/key)

## REST API (POC)

Base URL defaults to `http://localhost:8080`.

- Health
  - `GET /api/v1/health`
  - Response:
    ```json
    { "status": "ok", "leader_reachable": true }
    ```

- Brokers
  - `GET /api/v1/brokers`
  - Response (from `BrokerListResponse`):
    ```json
    [
      { "broker_id": "...", "broker_addr": "host:port", "broker_role": "Leader|Follower" }
    ]
    ```

- Leader
  - `GET /api/v1/leader`
  - Response (from `BrokerResponse`):
    ```json
    { "leader": "broker-id" }
    ```

- Namespaces
  - `GET /api/v1/namespaces`
  - Response (from `NamespaceListResponse`):
    ```json
    { "namespaces": ["/ns1", "/ns2"] }
    ```

- Topics in a namespace
  - `GET /api/v1/topics?namespace=/ns1`
  - Response (from `TopicListResponse`):
    ```json
    { "topics": ["/ns1/topic-a", "/ns1/topic-b"] }
    ```

- Topic description
  - `GET /api/v1/topics/:topic`
  - Response (from `DescribeTopicResponse`):
    ```json
    {
      "name": "/ns1/topic-a",
      "type_schema": 0,
      "schema_data": "<base64>",
      "subscriptions": ["sub-1", "sub-2"]
    }
    ```

- Topic subscriptions
  - `GET /api/v1/topics/:topic/subscriptions`
  - Response (from `SubscriptionListResponse`):
    ```json
    { "subscriptions": ["sub-1", "sub-2"] }
    ```

## curl examples

Assuming the gateway runs on `http://localhost:8080` and a namespace `/public` with topic `/public/my-topic`:

```
# Health
curl -s http://localhost:8080/api/v1/health | jq

# Brokers
curl -s http://localhost:8080/api/v1/brokers | jq

# Leader broker id
curl -s http://localhost:8080/api/v1/leader | jq

# Namespaces
curl -s http://localhost:8080/api/v1/namespaces | jq

# Topics in a namespace
curl -s "http://localhost:8080/api/v1/topics?namespace=/public" | jq

# Describe topic
curl -s http://localhost:8080/api/v1/topics/%2Fpublic%2Fmy-topic | jq

# Topic subscriptions
curl -s http://localhost:8080/api/v1/topics/%2Fpublic%2Fmy-topic/subscriptions | jq
```

Notes:
- URL-encode the `:topic` parameter if it contains slashes.
- For HTTPS listener, use `https://` and `-k` if testing with self-signed certs.
