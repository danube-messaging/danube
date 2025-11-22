# Danube Dev Stack with UI

This compose setup is tailored for local development.

- Builds broker, CLI from your local workspace via the root Dockerfile targets (`broker`, `cli`).
- Uses Prometheus for metrics.
- Best for iterating on Rust backend code (broker/gateway) while testing the UI and metrics in one stack.

## What runs

- etcd (metadata)
- minio + mc (S3-compatible storage and bucket bootstrap)
- broker1, broker2 (built from local code)
- danube-cli (built from local code; includes danube-cli and danube-admin-cli)

## Ports

- Broker1 gRPC: localhost:6650, Admin API: localhost:50051, Metrics: http://localhost:9040/metrics
- Broker2 gRPC: localhost:6651, Admin API: localhost:50052, Metrics: http://localhost:9041/metrics
- MinIO: http://localhost:9000 (API), http://localhost:9001 (Console)

## Start the stack

From the repo root:

```bash
# Start (build local targets and run)
docker-compose -f docker/dev/docker-compose.yml up -d --build
```

## Stop the stack

```bash
docker-compose -f docker/dev/docker-compose.yml down
```

To stop and remove volumes (fresh start):

```bash
docker compose -f docker/dev/docker-compose.yml down -v
```

## Rebuild after code changes

Because this stack builds images from the local workspace, rebuild to pick up changes:

```bash
# Rebuild everything and recreate
docker compose -f docker/dev/docker-compose.yml up -d --build

# Or rebuild specific services and recreate them
docker compose -f docker/dev/docker-compose.yml build broker1 broker2 danube-cli
docker compose -f docker/dev/docker-compose.yml up -d --no-deps broker1 broker2 danube-cli
```

## Logs

```bash
# All services
docker compose -f docker/dev/docker-compose.yml logs -f

# Specific services
docker compose -f docker/dev/docker-compose.yml logs -f broker1
```

## Notes

- If you prefer running the UI in pure dev mode (Vite on port 5173), we can add a `admin-ui-dev` service based on the UI's Dockerfile.dev; ask and we can wire it.
