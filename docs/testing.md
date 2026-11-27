# Testing Danube locally

How to run the test suites the same way CI does. Run them after a change and before you commit or open a PR.

```bash
# Every change: no broker needed (Tier 1)
cargo test --workspace --lib --bins

# Any change to broker, client, storage, schema or CLI: 3-broker cluster (Tier 2)
make brokers && cargo test --release -p danube-broker --tests -- --test-threads=1
```

## Which tiers to run

- **Every change:** Tier 1.
- **Code in `danube-broker`, `danube-client`, `danube-persistent-storage`, `danube-schema`, `danube-raft` or `danube-core`:** Tier 1 and Tier 2.
- **Code in `danube-admin` (CLI, MCP, console) or the admin proto:** Tier 1 and Tier 3.
- **A specific area (restart, failover, security, edge, ...):** also the matching workflow from Tier 4.
- **Docs-only changes:** nothing to run.

Report what you ran and the result. If a test fails, say so with the output; do not skip it silently.

## Prerequisites

- Rust stable and `protoc` (`sudo apt-get install -y protobuf-compiler`).
- `nc` (`netcat-openbsd`) to wait for ports.
- The tiers that start brokers use the fixed ports `6650-6652` (client and admin), `50051-50053` (admin), `7650-7652` (Raft). Run one tier at a time and stop the brokers before the next one.
- Broker data goes to `./danube-data/` and logs to `./temp/`. Both are git-ignored.

## Tier 1: unit and self-contained tests (no broker)

Same as the `unit-tests.yml` workflow, plus the integration tests that start their own in-process nodes.

```bash
cargo test --workspace --lib --bins                     # all unit tests
cargo check --workspace --features danube-broker/lakehouse
cargo test --workspace --no-run                         # every integration test compiles
cargo test -p danube-raft --tests                       # embedded Raft nodes in temp dirs
cargo test -p danube-persistent-storage --tests         # WAL, segments, recovery
cargo test -p danube-iceberg --tests                    # Parquet + Iceberg, local only
cargo test -p danube-broker --bin danube cli::          # CLI tree + `danube up` parsing
```

- `danube-raft` tests bind ports from `17650` upward, so they can run while a cluster is up.
- Tests marked `#[ignore]` need external services. They run in Tier 4, never in Tier 1.

## Tier 2: broker integration tests on a 3-broker cluster

Same as the `pull-request-tests.yml` workflow. The tests in `danube-broker/tests/` connect to `http://127.0.0.1:6650` (override with `DANUBE_BROKER_ENDPOINT`).

1. Start a fresh cluster (builds the release binary first):

   ```bash
   make brokers-clean            # stop old brokers, remove ./danube-data/broker-*
   make brokers                  # 3 brokers: client 6650-6652, admin 50051-50053, Raft 7650-7652
   ```

2. Wait until all three ports answer:

   ```bash
   for port in 6650 6651 6652; do
     until nc -z 127.0.0.1 $port; do sleep 2; done
   done
   make cluster-status           # should list 3 voters and a leader
   ```

3. Run the tests serially (several are timing-sensitive):

   ```bash
   cargo test --release -p danube-broker --tests -- --test-threads=1
   ```

4. Stop the cluster and check that nothing is left:

   ```bash
   make brokers-clean
   pgrep -a danube && pkill -9 -x danube    # see the note below
   ```

- `make brokers-clean` sends SIGTERM to all brokers at once. The last one runs its graceful drain without Raft quorum and can hang, so force-kill whatever is still running.

- Broker logs are in `temp/broker_6650.log`, `temp/broker_6651.log` and `temp/broker_6652.log`. Read them when a test fails.
- Consumers must start before producers in tests: a new subscription starts at the end of a reliable topic.
- Other Makefile targets: `make broker-kill ID=0` and `make broker-start ID=0` stop and start one broker; `make brokers-stop` stops all but keeps Raft data, for restart testing.

## Tier 3: admin CLI and MCP tests on a single node

Same as the first part of the `cli-e2e.yml` workflow. The tests in `danube-admin/tests/` run the `danube` binary (`target/release/danube`, then `target/debug/danube`, or `DANUBE_BIN`) against `DANUBE_ENDPOINT`.

```bash
cargo build --release -p danube-broker
mkdir -p temp
./target/release/danube up cluster --single-node \
  --listen 0.0.0.0:6650 --raft-port 7650 \
  --data-dir ./danube-data/cli-1 \
  > temp/cli_broker.log 2>&1 &
echo $! > temp/cli_broker.pid
until nc -z 127.0.0.1 6650; do sleep 2; done

DANUBE_ENDPOINT=http://127.0.0.1:6650 \
DANUBE_BROKER_ENDPOINT=http://127.0.0.1:6650 \
  cargo test -p danube-admin --tests -- --nocapture

kill $(cat temp/cli_broker.pid)
rm -rf ./danube-data/cli-1
```

- Stop brokers by PID, not with `pkill -f "<command line>"`: that pattern also matches the shell running your script and kills it.

- Port `6650` serves both client and admin gRPC when `--admin-listen` is not given.
- Use a new data dir for each run: every data dir records its mode in `danube-node.json` and refuses a different mode.

## Tier 4: end-to-end scenarios (by area)

These are the `.github/workflows/*-e2e.yml` workflows. Each one starts its own brokers and runs CLI scripts and `#[ignore]` tests. They are plain bash, so they run locally too:

1. Build once: `cargo build --release --workspace`.
2. Run the workflow's `run:` steps in order from the repo root, each with `bash -e`. Skip `Checkout`, `Install Rust`, `Cache cargo`, `Install dependencies` and `Upload` steps.
3. Stop every broker you started (see the cleanup checklist), even when a step fails.

Exceptions to plan for:

- `security-e2e.yml` passes tokens between steps with `$GITHUB_ENV` and `${{ env.NAME }}`. Locally, run its steps in one shell and replace `${{ env.NAME }}` with `$NAME`.
- `write-buffer-e2e.yml` gets Valkey from a GitHub `services:` container. Start it yourself first (below).
- `edge-replication-e2e.yml` has a Python MQTT step: `pip install -r scripts/edge-e2e/requirements.txt` first, or skip that step and say so.
- The workflows use fixed ports, so run them one at a time and never next to Tier 2 or 3.

- **Restart and recovery** (Raft snapshots, WAL resume, cursors): `standalone-restart-e2e.yml` (standalone, SIGTERM and `kill -9`) and `broker-restart-e2e.yml` (3-node cluster).
- **Failover and topic moves:** `broker-failover-e2e.yml`, `danube-rebalance-e2e.yml`, `broker-reliable-move-shared-fs-e2e.yml` (runs `reliable_topic_move_shared_fs` with `--ignored`, needs `DANUBE_BIN`).
- **Security** (TLS, JWT, RBAC): `security-e2e.yml`. Generates certs into `./cert/` and runs `security_e2e` with `--ignored`.
- **Write buffer and warm tier:** `write-buffer-e2e.yml`. Needs Valkey on `127.0.0.1:6379`, for example `docker run -d --name valkey -p 6379:6379 valkey/valkey:latest`. Runs `write_buffer_valkey` and `warm_tier_read` with `--ignored`.
- **Edge and MQTT:** `edge-replication-e2e.yml`, or locally `./scripts/edge-e2e/edge-e2e-local.sh` (same steps; `--skip-build` reuses the binary). A 3-broker cluster, the `edge1` namespace and the `telemetry-events` schema on the cluster, then `danube up edge` on port `6653`. Runs `edge_replication_basic` with `--ignored`, using `EDGE_BROKER_ENDPOINT=http://127.0.0.1:6653` and `CLOUD_BROKER_ENDPOINT=http://127.0.0.1:6650`.
- **CLI behaviour** (intents, `--wal-dir`, branching, single-port admin): the rest of `cli-e2e.yml` after the admin tests.
- **AI agent stack** (MCP + CLI): see [`agent_bench/AGENTS.md`](../agent_bench/AGENTS.md).

## Cleanup checklist

```bash
pkill -x danube || true       # SIGTERM every broker
sleep 5
pkill -9 -x danube || true    # a broker stuck in its drain (no quorum) needs SIGKILL
pgrep -a danube               # should print nothing
make data-clean               # removes ./danube-data/broker-*
```

When you add a test that needs a running broker, put it in the tier whose setup it uses, or mark it `#[ignore]` and run it from a workflow. Update this file and the matching workflow in the same change.
