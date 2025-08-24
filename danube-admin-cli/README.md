# Danube-admin-cli

The danube-admin-cli is a command-line interface designed for interacting with and managing the Danube cluster.

## TLS and mTLS

The CLI uses a TLS-aware client factory. Configure via environment variables or by using an HTTPS endpoint.

Environment variables:

- DANUBE_ADMIN_ENDPOINT: Admin endpoint (default: http://127.0.0.1:50051)
- DANUBE_ADMIN_TLS: "true" to force TLS even for http endpoints
- DANUBE_ADMIN_DOMAIN: TLS server name (SNI/verification)
- DANUBE_ADMIN_CA: Path to CA PEM file (optional; system roots used if omitted)
- DANUBE_ADMIN_CERT: Path to client certificate (PEM) for mTLS (optional)
- DANUBE_ADMIN_KEY: Path to client private key (PEM) for mTLS (optional)

Examples:

```bash
# TLS using HTTPS endpoint and custom domain/CA
export DANUBE_ADMIN_ENDPOINT=https://broker.example.com:50051
export DANUBE_ADMIN_DOMAIN=broker.example.com
export DANUBE_ADMIN_CA=./cert/ca-cert.pem

# optional mTLS
export DANUBE_ADMIN_CERT=./cert/client-cert.pem
export DANUBE_ADMIN_KEY=./cert/client-key.pem

danube-admin-cli brokers list
```

## Reliable dispatch options

The `topic create` and `topic create-partitioned` commands accept a `--dispatch-strategy` flag.

Values:

- `non_reliable` (default)
- `reliable`

When `reliable` is selected, the following flags can override defaults:

- `--segment-size-mb <u64>` (default: 64)
- `--retention-policy retain_until_ack|retain_until_expire` (default: retain_until_ack)
- `--retention-period-sec <u64>` (default: 86400)

## Command examples

### Brokers

```bash
# List active brokers
danube-admin-cli brokers list

# Show leader broker
danube-admin-cli brokers leader-broker

# List namespaces in cluster
danube-admin-cli brokers namespaces
```

### Namespaces

```bash
# List topics in a namespace
danube-admin-cli namespaces topics default

# Get namespace policies
danube-admin-cli namespaces policies default

# Create a namespace
danube-admin-cli namespaces create default

# Delete a namespace (must be empty)
danube-admin-cli namespaces delete default
```

### Topic management

```bash
# List topics in a namespace
danube-admin-cli topic list default
# JSON output
danube-admin-cli topic list default --output json

# Create non-partitioned topic (non-reliable)
danube-admin-cli topic create /default/mytopic --dispatch-strategy non_reliable

# Create non-partitioned topic (reliable with defaults)
danube-admin-cli topic create /default/mytopic --dispatch-strategy reliable

# Create non-partitioned topic (reliable with overrides)
danube-admin-cli topic create /default/mytopic \
  --dispatch-strategy reliable \
  --segment-size-mb 128 \
  --retention-policy retain_until_expire \
  --retention-period-sec 172800

# Create partitioned topic (server-side batch)
danube-admin-cli topic create-partitioned /default/mytopic \
  --partitions 3 \
  --dispatch-strategy reliable \
  --segment-size-mb 64 \
  --retention-policy retain_until_ack \
  --retention-period-sec 86400

# Delete a topic
danube-admin-cli topic delete /default/mytopic

# List subscriptions on a topic
danube-admin-cli topic subscriptions /default/mytopic
# JSON output
danube-admin-cli topic subscriptions /default/mytopic --output json

# Unsubscribe a subscription from a topic
danube-admin-cli topic unsubscribe --subscription sub1 /default/mytopic

# Describe a topic (schema + subscriptions)
danube-admin-cli topic describe /default/mytopic
# JSON output (uses DANUBE_BROKER_ENDPOINT for Discovery; default http://127.0.0.1:6650)
danube-admin-cli topic describe /default/mytopic --output json
```
