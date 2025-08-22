# Danube Testing Guide

This document explains how to run the danube-broker tests reliably. The key is to start from a CLEAN infrastructure state before running tests.

## Prerequisites
- Docker Desktop running (required for ETCD)
- Rust toolchain installed (cargo, rustc)
- TLS certificates available (tests use `cert/ca-cert.pem`)

## Clean the infrastructure FIRST (required)
Tests may be flaky if old brokers or ETCD data are left running. Always clean first:

- Stop running brokers:
```bash
make brokers-clean
# or alternatively
pkill -f danube-broker || true
```

- Remove ETCD container and data:
```bash
docker rm -f etcd-danube || true
rm -rf ./etcd-data
```

## Start fresh infrastructure
- Start ETCD (tested with v3.5.9):
```bash
docker run -d --name etcd-danube -p 2379:2379 \
  -v "$(pwd)/etcd-data:/etcd-data" \
  quay.io/coreos/etcd:v3.5.9 \
  /usr/local/bin/etcd \
  --name etcd-danube \
  --data-dir /etcd-data \
  --advertise-client-urls http://0.0.0.0:2379 \
  --listen-client-urls http://0.0.0.0:2379
```

- Start 3 brokers (6650, 6651, 6652):
```bash
make brokers
```
Logs are written under `temp/broker_<port>.log`.

Notes:
- If you see a Prometheus "address already in use" error on 9040, it means an old broker is still running. Re-run the clean steps and start brokers again.

## Running tests
- Run a specific test (e.g., exclusive subscription):
```bash
cargo test -p danube-broker --test subscription_exclusive -- --nocapture
```

- Run the full integration test suite:
```bash
cargo test -p danube-broker --tests -- --nocapture
```

- Optional: run tests serially to minimize interference:
```bash
cargo test -p danube-broker --tests -- --test-threads=1 --nocapture
```

## Troubleshooting
- A test times out on first run but passes alone: this usually indicates leftover state or timing. Ensure the infrastructure is CLEAN and brokers have fully started before kicking tests.
- Verify brokers are running:
```bash
ps aux | grep danube-broker | grep -v grep
```
- Verify ETCD is running:
```bash
docker ps | grep etcd-danube
```

## Ports
- Brokers: 6650, 6651, 6652
- Admin: 50051, 50052, 50053
- Prometheus: 9040, 9041, 9042

Starting from a clean state is critical for consistent green test runs.
