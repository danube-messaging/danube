# Danube Dev Stack with UI

This compose setup is tailored for local development.

- Builds broker, CLI, and admin-gateway from your local workspace via the root Dockerfile targets (`broker`, `cli`, `admin-gateway`).
- Uses Prometheus for metrics and the published Admin UI image from GHCR.
- Best for iterating on Rust backend code (broker/gateway) while testing the UI and metrics in one stack.

## What runs

- etcd (metadata)
- minio + mc (S3-compatible storage and bucket bootstrap)
- broker1, broker2 (built from local code)
- danube-cli (built from local code; includes danube-cli and danube-admin-cli)
- prometheus (scrapes broker metrics)
- admin-gateway (built from local code; HTTP/JSON BFF)
- admin-ui (external image: ghcr.io/danube-messaging/danube-admin-ui:latest)

## Ports

- Admin UI: http://localhost:8081
- Admin Gateway: http://localhost:8080
- Prometheus: http://localhost:9090
- Broker1 gRPC: localhost:6650, Admin API: localhost:50051, Metrics: http://localhost:9040/metrics
- Broker2 gRPC: localhost:6651, Admin API: localhost:50052, Metrics: http://localhost:9041/metrics
- MinIO: http://localhost:9000 (API), http://localhost:9001 (Console)

## Start the stack

From the repo root:

```bash
# Start (build local targets and run)
docker-compose -f docker/dev_with_ui/docker-compose.yml up -d --build
```

Open the Admin UI: http://localhost:8081

## Stop the stack

```bash
docker-compose -f docker/dev_with_ui/docker-compose.yml down
```

To stop and remove volumes (fresh start):

```bash
docker compose -f docker/dev_with_ui/docker-compose.yml down -v
```

## Rebuild after code changes

Because this stack builds images from the local workspace, rebuild to pick up changes:

```bash
# Rebuild everything and recreate
docker compose -f docker/dev_with_ui/docker-compose.yml up -d --build

# Or rebuild specific services and recreate them
docker compose -f docker/dev_with_ui/docker-compose.yml build broker1 broker2 admin-gateway danube-cli
docker compose -f docker/dev_with_ui/docker-compose.yml up -d --no-deps broker1 broker2 admin-gateway danube-cli
```

## Logs

```bash
# All services
docker compose -f docker/dev_with_ui/docker-compose.yml logs -f

# Specific services
docker compose -f docker/dev_with_ui/docker-compose.yml logs -f admin-gateway
docker compose -f docker/dev_with_ui/docker-compose.yml logs -f broker1
```

## Prometheus config

This stack mounts `scripts/prometheus.yml` from the repo:

```yaml
volumes:
  - ../../scripts/prometheus.yml:/etc/prometheus/prometheus.yml:ro
```

It scrapes the brokers and powers Admin Gateway metrics queries.

## Notes

- Admin Gateway CORS is configured in the compose to allow `http://localhost:8081` (the Admin UI).
- If you prefer running the UI in pure dev mode (Vite on port 5173), we can add a `admin-ui-dev` service based on the UI's Dockerfile.dev; ask and we can wire it.
