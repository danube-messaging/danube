<p align="center">
  <img src="Danube_logo.png" alt="Danube Logo" width="400">
</p>

<h3 align="center">Messaging and event streaming that humans and AI agents can both run.</h3>

<p align="center">
  One Rust binary. No YAML files. No JVM. No ZooKeeper.
</p>

<p align="center">
  <a href="https://danube-messaging.com/"><img src="https://img.shields.io/badge/%F0%9F%93%91-Documentation-blue" alt="Documentation"></a>
  <a href="docker/README.md"><img src="https://img.shields.io/badge/%F0%9F%90%B3-Docker%20Ready-2496ED" alt="Docker"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/%F0%9F%A6%80-Rust-000000" alt="Rust"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/%F0%9F%93%9C-Apache%202.0-green" alt="License"></a>
</p>

---

Danube gives you pub/sub, durable streams, task queues and an AI-ready control plane in a single `danube` binary. Start it on your laptop in one command, grow it into a Raft cluster when you need to, and let your agents both **run on it** and **operate it**.

## Try it in 60 seconds

Grab the `danube` binary from [releases](https://github.com/danube-messaging/danube/releases) (or `cargo build --release -p danube-broker`), then:

```bash
# 1. Start a node
danube up standalone

# 2. Create a topic by describing what it is for
danube topic create /default/orders --intent event_stream

# 3. Stream some messages (two terminals)
danube consume /default/orders
danube produce /default/orders -m "hello danube"
```

That's it. Run `danube` on its own any time to see everything it can do.

Prefer containers?

```bash
docker run -p 6650:6650 ghcr.io/danube-messaging/danube:latest \
  up standalone --data-dir /danube-data --listen 0.0.0.0:6650
```

## Why Danube

### 🧑‍💻 Simple for humans

- **One binary, one port.** Broker, CLI, web console backend and MCP server all ship as `danube`. Clients, schemas and admin share port `6650`.
- **Say what a topic is for, not how to tune it.** `--intent realtime | event_stream | task_queue | audit_log` picks delivery, durability, retries and dead-letter handling for you.
- **Zero config files.** Start with flags, then change cluster settings live (`danube cluster ...`) or declaratively (`danube apply -f state.yaml --dry-run`).
- **Safe by default.** `--dry-run` shows what a node or manifest will do before it does it, and Danube refuses to start a data directory in the wrong mode.

### 🤖 Built for AI agents

- **Agents operate Danube out of the box:** `danube mcp` exposes 71 admin, schema, RBAC and diagnostic tools to Claude Code, Cursor, VS Code or Antigravity, with dry-run defaults, confirmation guardrails and secret redaction.
- **Add bootstrap and messaging skills:** run `npx skills add danube-messaging/danube-agent-skills` ([danube-agent-skills](https://github.com/danube-messaging/danube-agent-skills)) so agents can also start nodes and produce/consume messages.
- **Agent workloads run natively:** task queues with leases, progress checkpoints, retries and DLQs, request/reply, and cascading cancellation of task trees.
- **Replay and branch history:** fork any topic at an offset into a new topic (`danube topic branch`) to replay or evaluate agent runs.

### ⚙️ Serious streaming underneath

- Reliable (WAL-backed) and in-memory topics side by side.
- Exclusive, shared, failover and key-shared subscriptions.
- Tiered storage on local disk, shared filesystems or S3 / GCS / Azure, so topics move between brokers in milliseconds.
- Built-in schema registry (JSON Schema, Avro, Protobuf), edge MQTT gateway and Apache Iceberg export.

## Connect your AI agent

Add Danube to your MCP client config:

```json
{
  "mcpServers": {
    "danube": {
      "command": "danube",
      "args": ["mcp", "--endpoint", "http://127.0.0.1:6650"]
    }
  }
}
```

Then just ask: *"Create a task queue for my research agents and show me which consumers are stalled."*

To see the verified end-to-end agent test suite, check [agent_bench](agent_bench/README.md) and the [v0.17.0 report](agent_bench/reports/v0.17.0.md).

## Run it your way

```bash
# Laptop, CI, single box
danube up standalone

# Production cluster (run on each node)
danube up cluster --data-dir /var/lib/danube \
  --advertised-addr node1:6650 --seed-nodes node1:7650,node2:7650,node3:7650

# Edge gateway: MQTT in, survives WAN outages, replicates to the cloud
danube up edge --data-dir ./edge --name factory-1 \
  --cloud-endpoint https://danube.example.com:6650
```

Ready-made setups: [Docker Compose](docker/README.md) · [Kubernetes](https://danube-messaging.com/getting-started/kubernetes/)

## Client libraries

[Rust](danube-client/README.md) · [Go](https://github.com/danube-messaging/danube-go) · [Java](https://github.com/danube-messaging/danube-java) · [Python](https://github.com/danube-messaging/danube-py)

## Learn more

- 📖 [Documentation](https://danube-messaging.com/): quickstart, concepts and guides
- 🤖 [Danube for AI agents](https://danube-messaging.com/ai-agents/overview/)
- 🧰 [The danube CLI](https://danube-messaging.com/getting-started/the-danube-cli/)
- 🧭 [Positioning and architecture](danube_positioning.md)

## Build from source

```bash
cargo build --release -p danube-broker                        # ./target/release/danube
cargo build --release -p danube-broker --features lakehouse   # with Iceberg export
```

## Contributing

Issues and pull requests are welcome. Start with the [contribution guide](https://danube-messaging.com/contributing/), then [report a bug](https://github.com/danube-messaging/danube/issues) or [suggest a feature](https://github.com/danube-messaging/danube/issues/new).

Licensed under [Apache 2.0](LICENSE).
