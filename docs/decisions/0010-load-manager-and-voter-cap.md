# 0010: Load manager placement and the Raft voter cap

**Status:** Accepted

## Load manager

- The Raft leader runs the load manager. Brokers publish small periodic load reports (about every 30s) into metadata.
- **Placement** of new topics uses an assignment strategy: `fair` (topic count), `balanced` (topic load, CPU and memory; the default) or `weighted_load` (adaptive bottleneck weighting).
- **Rebalancing** is **off by default**. When enabled (`danube cluster load-manager ...`), it uses profiles (conservative, balanced, aggressive) that bound the CV threshold, check interval, moves per hour and cooldowns. Topics younger than `min_topic_age_seconds` or blacklisted (exact topic or `/ns/*`) never move.
- Manual tools: `danube broker balance` (report), `danube broker rebalance [--dry-run] [--max-moves]`, `danube broker unload`, `danube topic unload`.
- `--role lakehouse` nodes register as drained, so client topics are never placed on them.

Rebalancing is only safe because topic moves are metadata operations (0002). Keep it conservative by default: moves are cheap but not free (clients reconnect).

## Voter cap

- Raft voters are capped (`/cluster/config/target_voter_count`, default 5). Extra brokers join as **non-voting learners**: they serve topics and replicate metadata but do not slow commits or affect quorum.
- `danube cluster node promote` refuses to exceed the cap.
- SIGTERM drains a broker's topics instead of changing Raft membership, so rolling restarts do not churn quorum.

## Why

- Serving capacity (brokers) and consensus size (voters) scale independently: 3 or 5 voters with 50 brokers is fine.
- Scale-in and restarts do not risk quorum.

## Rules for changes

- New placement inputs must come from load reports, not from per-message hooks.
- Any automatic action must respect the cooldowns and support `--dry-run`.

