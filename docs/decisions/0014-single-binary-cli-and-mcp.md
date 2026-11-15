# 0014: One binary, one CLI tree, guarded MCP

**Status:** Accepted

## Decision

- A single `danube` binary (built from `danube-broker`) contains the broker, the CLI, the web console backend (`danube console`) and the MCP server (`danube mcp`).
- The CLI is one clap 4 derive tree (`danube-broker/src/cli`), grouped Server / Messaging / Resources / Cluster operations / Utilities. Bare `danube` prints an overview and never starts a server. `danube up <mode>` has per-mode required flags.
- Client commands share `ClientArgs` (`--endpoint`, `--context`, `--token`, `--tls*`), resolved as flag > env > saved context > default (`danube-admin/src/cli/connection.rs`).
- Port 6650 multiplexes client gRPC, schema registry and admin gRPC unless `--admin-listen` is set.
- MCP exposes the admin API as tools (about 70, in `danube-admin/src/mcp/server.rs`), plus prompts and a `danube://metrics` resource. Destructive tools require confirmation or default to dry run. Payload access needs `ReadPayload`.

## Why

- One artifact to install, version and secure. Humans and agents use the same commands.
- A clap tree gives consistent help, suggestions, completions and validation.

## Rules for changes

- Pre-1.0: clean breaks are allowed, but update every caller (workflows, docker, Makefile, README, CHANGELOG, docs repo) in the same PR.
- New commands: singular noun, then verb (plural visible aliases are fine), an `about`, examples in `after_help`, and a parse test.
- New MCP tools that mutate state must support dry run or confirmation and be covered by RBAC.

