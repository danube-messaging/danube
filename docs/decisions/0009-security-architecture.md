# 0009: Security architecture

**Status:** Accepted, with known gaps listed below

## Decision

- **Transport and auth mode are chosen at startup** with `danube up ... --auth none|tls|auto-tls`. They are never hot-swapped: switching plaintext/TLS under live clients and Raft peers is unsafe.
  - `tls`: operator-provided `--tls-cert`, `--tls-key`, `--ca-cert`. Raft peers use mTLS with the same material.
  - `auto-tls`: the node generates a cluster CA, node certificate, JWT secret and a `bootstrap_admin.token` under the data dir. The CA key is stored **encrypted in Raft** (key from `DANUBE_CLUSTER_SECRET` or a local `cluster_ca.secret`) so other nodes can share it.
- **Authentication:** JWT (HMAC, `--jwt-secret`). Tokens are issued offline (`danube auth token create`) or by the cluster (`auth token issue`). Revocation is per subject at `/auth/revoked/{subject}` and is permanent.
- **Authorization:** RBAC with roles and bindings stored in Raft (`danube auth role|binding`). There are 10 permissions, including `ReadPayload`, which is separate so metadata admins and agents cannot read message contents by default. `--super-admin` subjects bypass RBAC.
- **Admin surfaces share the same checks:** the CLI and MCP go through admin gRPC with the caller's token. The console backend can require its own HTTP bearer token (`--require-auth`).
- **MCP guardrails:** destructive tools need explicit confirmation or default to dry run, and control-plane mutations are audit-logged (0014).

## Why

- One binary must be secure-by-flag for small teams (`auto-tls`) and integrate with real PKI for enterprises (`tls`).
- Keeping RBAC in Raft means every broker enforces the same policy without a config push.

## Known gaps (do not paper over them in docs)

- Consume/receive and some schema reads check permissions at **cluster** scope only, so topic- or namespace-scoped bindings are not enough for consumers.
- Multi-node `auto-tls`: each fresh node creates its own CA before syncing via Raft, so Raft mTLS may fail to form. The workaround is pre-seeding the first node's CA files, or using `--auth tls`.
- `danube cluster export-config` includes secrets (JWT key, object-store credentials). Treat its output as sensitive.
- `danube mcp --http` has no HTTP auth of its own; protect it with `danube console --require-auth` or a proxy.

## Rules for changes

- New gRPC handlers must call the authz layer with the narrowest resource scope.
- Never log tokens, secrets or payloads (payload access is the `ReadPayload` permission).

