# Task 06: Security, RBAC, and MCP Guardrails

## Goal

Verify the agent can bootstrap a secured node with `--auth auto-tls`, configure least-privilege RBAC and JWTs via MCP, verify allowed vs denied produce/consume operations, and confirm MCP destructive/secret guardrails work as designed.

## Preconditions

- `openssl` installed on the host.
- Clean directory `/tmp/danube-bench-06` and free port `127.0.0.1:6650` (or custom port).
- MCP configured with the TLS CA and bootstrap admin token (for example via `DANUBE_TLS_CA` and `DANUBE_TOKEN` or `--context`).

## Prompt for the agent

> 1. Start a fresh standalone node in `/tmp/danube-bench-06/data` with `--auth auto-tls`.
> 2. Connect `danube mcp` using `/tmp/danube-bench-06/data/tls/ca.pem` and `/tmp/danube-bench-06/data/bootstrap_admin.token`.
> 3. Using Danube MCP:
>    - Create namespace `secure` and topic `/secure/payments` (`intent: "audit_log"`).
>    - Call `cluster_export_config` and verify the JWT secret is `<redacted>` (never exposed).
>    - Attempt `delete_topic` on `/secure/payments` *without* `confirm_destructive: true` and verify MCP refuses the destructive call.
>    - Create role `pay-writer` (`Lookup,Produce`), role `pay-reader` (`Lookup,Consume`), bind them on scope `topic` / resource `/secure/payments` to service accounts `checkout-svc` and `ledger-svc`, and issue tokens for both.
> 4. Using Danube CLI with `--tls-ca` and each token:
>    - Confirm `ledger-svc` can consume and `checkout-svc` can produce on `/secure/payments`.
>    - Confirm `checkout-svc` is denied (`PERMISSION_DENIED`) when trying to consume `/secure/payments`.

## Expected surface usage

- **CLI (`danube-bootstrap`):** `danube up standalone --data-dir /tmp/danube-bench-06/data --auth auto-tls`.
- **MCP:** `create_namespace`, `create_topic`, `cluster_export_config`, `delete_topic` (guardrail check), `create_role`, `create_binding`, `create_token`.
- **CLI (`danube-messaging`):** `danube consume` / `danube produce` with `--tls-ca` and `--token`.

## Verification

Export the secured endpoint, CA, and admin token, then run:

```bash
DANUBE_ENDPOINT=https://127.0.0.1:6650 \
DANUBE_TLS_CA=/tmp/danube-bench-06/data/tls/ca.pem \
DANUBE_TOKEN="$(cat /tmp/danube-bench-06/data/bootstrap_admin.token)" \
bash agent_bench/verify/06_security_and_guardrails.sh
```

