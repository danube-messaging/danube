# Danube Docker Setup (v2)

Danube v2 runs from a single binary (`/usr/local/bin/danube`) with **zero mounted YAML config files**.

The consolidated [`docker/docker-compose.yml`](docker-compose.yml) builds the [`Dockerfile`](../Dockerfile) and boots a 3-node Raft cluster (`broker1`, `broker2`, `broker3`).

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+

## Quick Start

```bash
cd docker/
docker compose up -d --build
```

Check container status:

```bash
docker compose ps
```

Exposed host ports:
- **Brokers (gRPC data + admin)**: `6650` (`broker1`), `6651` (`broker2`), `6652` (`broker3`)
- **Prometheus metrics (`/metrics`)**: `9040`, `9041`, `9042`

## Test Messaging & Admin from inside a broker container

No separate CLI or admin container is needed: every broker container ships the `danube` binary. Client commands default to `http://127.0.0.1:6650`; pass `--endpoint` to target another broker.

```bash
# Consume messages from broker1
docker exec -it danube-broker1 danube consume /default/test -s my-sub

# Produce messages to broker1
docker exec danube-broker1 danube produce /default/test \
  --count 100 \
  --message "Hello Danube v2!"
```

### Cluster Administration & Declarative Apply

```bash
# Inspect brokers and Raft status
docker exec danube-broker1 danube broker list
docker exec danube-broker1 danube cluster status

# Create a topic with a stream intent
docker exec danube-broker1 danube topic create /default/agent-tasks --intent task_queue

# Zero-copy branch a topic
docker exec danube-broker1 danube topic branch /default/agent-tasks /default/agent-tasks-replay

# Apply a declarative YAML/JSON manifest (additive by default)
docker exec -i danube-broker1 danube apply -f - < cluster-state.yaml
```

## Single-Container Standalone Mode

```bash
docker build -t danube:local -f ../Dockerfile ..
docker run --rm -p 6650:6650 -p 9040:9040 \
  danube:local up standalone \
  --data-dir /danube-data \
  --listen 0.0.0.0:6650 \
  --metrics-listen 0.0.0.0:9040
```

## Security, Storage & Recovery Overrides

- **Startup TLS / Auto-TLS**: pass `--auth auto-tls`, or `--auth tls` with `--tls-cert`, `--tls-key`, `--ca-cert` (plus optional `--jwt-secret`, `--super-admin`), in the container `command`.
- **Break-glass recovery / test overrides**: if Raft quorum is lost or you need static local overrides (object store, shared filesystem, write buffer), mount a recovery JSON/YAML file and pass `--recovery-config /path/to/recovery.yaml`.
- **Dedicated admin listener**: pass `--admin-listen 0.0.0.0:50051` to firewall admin gRPC separately from port `6650`.
- **Preview the resolved configuration**: append `--dry-run` to any `danube up ...` command.

## Cleanup

```bash
docker compose down -v
```

## Troubleshooting

```bash
# Follow logs for a broker
docker compose logs -f broker1

# Inspect metrics endpoint
curl http://localhost:9040/metrics
```
