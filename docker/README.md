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

## Test Messaging & Admin via `danube ctl`

No separate `danube-cli` or `danube-admin` container is needed — `danube` ships both inside every broker container:

```bash
# Consume messages from broker1
docker exec -it danube-broker1 danube ctl --endpoint http://127.0.0.1:6650 \
  consume --service-addr http://127.0.0.1:6650 --topic /default/test --subscription my-sub

# Produce messages to broker1
docker exec danube-broker1 danube ctl produce \
  --service-addr http://127.0.0.1:6650 \
  --topic /default/test \
  --count 100 \
  --message "Hello Danube v2!"
```

### Cluster Administration & Declarative Apply

```bash
# Inspect cluster and Raft status
docker exec danube-broker1 danube ctl --endpoint http://127.0.0.1:6650 brokers list
docker exec danube-broker1 danube ctl --endpoint http://127.0.0.1:6650 cluster status

# Create a topic with a StreamIntent profile
docker exec danube-broker1 danube ctl --endpoint http://127.0.0.1:6650 \
  topics create /default/agent-tasks --intent agent_task_queue

# Zero-copy branch a topic
docker exec danube-broker1 danube ctl --endpoint http://127.0.0.1:6650 \
  topics branch /default/agent-tasks /default/agent-tasks-replay

# Apply a declarative YAML/JSON manifest (additive by default)
docker exec -i danube-broker1 danube ctl --endpoint http://127.0.0.1:6650 \
  apply -f - < cluster-state.yaml
```

## Single-Container Standalone Mode

```bash
docker build -t danube:local -f ../Dockerfile ..
docker run --rm -p 6650:6650 -p 9040:9040 \
  danube:local up \
  --mode standalone \
  --data-dir /danube-data \
  --broker-addr 0.0.0.0:6650 \
  --prom-exporter 0.0.0.0:9040
```

## Security, Storage & Recovery Overrides

- **Startup TLS / Auto-TLS**: Pass `--auth-mode auto-tls` (or `--auth-mode tls` with `--tls-cert`, `--tls-key`, `--ca-cert`, `--jwt-secret`, `--super-admins`) directly in container `command`.
- **Break-glass Recovery / Test Overrides**: If Raft quorum is lost or you need static local overrides (e.g., object store / shared filesystem / write buffer settings), mount a recovery JSON/YAML file and pass `--recovery-config /path/to/recovery.yaml`.
- **Dedicated Admin Listener**: Pass `--admin-addr 0.0.0.0:50051` when you want to firewall admin gRPC separately from port `6650`.

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
