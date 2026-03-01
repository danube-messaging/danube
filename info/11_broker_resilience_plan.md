# Broker Resilience: Restart & Partition Safety

This document describes three problems with the current broker lifecycle, the
failure scenarios they affect, and the implementation plan to fix them.

---

## Problems

### Problem 1: TTL Renewal Loop Breaks on First Error

**File:** `danube-broker/src/danube_service/broker_register.rs:55-73`

The background task that renews broker registration via `put_with_ttl` does
`break` on the first error. A transient Raft write failure (network blip,
leader election in progress) permanently kills the renewal loop. The
registration key then expires at the next TTL boundary even though the broker
process is healthy.

```rust
// Current (fragile):
Err(e) => {
    error!(...);
    break;  // ← permanent death of renewal loop
}
```

### Problem 2: No Startup Topic Reconciliation

**File:** `danube-broker/src/danube_service.rs` (DanubeService::start)

When a broker restarts, its in-memory `TopicManager` is empty. Topics assigned
to it in metadata (`/cluster/brokers/{id}/{ns}/{topic}`) still exist, but the
`broker_watcher` only fires on **new** watch events — pre-existing keys don't
generate Put events. Result: metadata says the broker owns topics, but it can't
serve them. Clients get "topic not found."

### Problem 3: No Stale-State Detection After TTL Expiry

**Files:** `danube-broker/src/danube_service.rs`, `danube-broker/src/danube_service/broker_register.rs`

When a broker's registration expires (it was unavailable > TTL), its topics get
reassigned to other brokers. If the broker comes back (restart or network
heals), it currently either:

- **On restart:** re-registers as `active` — immediately eligible for new topics
  even though it was just declared dead. No investigation by the admin.
- **On network heal (process still running):** becomes a zombie — no
  registration, no renewal loop, no topics, but process still running.

In both cases, the broker should enter `drained` state and require admin to
explicitly `activate` it after investigation.

---

## Failure Scenarios

### Scenario 1: Network Glitch < TTL (< ~32s)

**Example:** Broker C loses network for 15 seconds.

| Time | Event |
|------|-------|
| T=0 | Network partition starts |
| T=0–15 | Broker C isolated. Can't write to Raft. TTL renewal fails → **renewal loop breaks** (Problem 1) |
| T=15 | Network heals. Raft reconnects, Broker C catches up via log replication |
| T=15 | TTL key still valid (last renewal + 32s > T=15). Topics still assigned. Local TopicManager intact |
| T=~42 | **Without fix:** TTL expires because renewal loop is dead → broker declared dead → topics reassigned |

**Current outcome:** 15-second glitch escalates to full broker failure at T=42.

**After Fix 1 (renewal resilience):** Renewal loop retries at T=15, succeeds,
pushes TTL to T=47. Normal operation resumes. **Self-healing, no admin
intervention.**

---

### Scenario 2: Broker Unavailable > TTL (> 32s)

**Example:** Broker C has a hardware issue, network down for 3 minutes.

| Time | Event |
|------|-------|
| T=0 | Network partition starts |
| T=~11 | TTL renewal fails → renewal loop breaks (Problem 1) |
| T=32 | TTL expires. Leader's TTL worker deletes `/cluster/register/{C}` |
| T=32 | LoadManager: "broker C no longer alive" → `delete_topic_allocation(C)` → topics to `/cluster/unassigned/` |
| T=33 | Leader assigns topics to Brokers A and B |
| T=180 | Network heals. Broker C reconnects to Raft, catches up |
| T=180 | State machine replays missed entries. `broker_watcher` fires Delete events for C's topics → cleans up local TopicManager |
| T=180 | **Current:** Broker C is a zombie — no registration, no renewal, no topics, process still running |

**After Fixes 1+3 (renewal resilience + stale-state detection):**

The renewal loop tracks cumulative failure time. After exceeding TTL:
1. Sets internal `registration_lost` flag
2. When next renewal succeeds (network healed), checks if registration exists
3. Re-registers with TTL but sets state to `drained` with reason `"registration_expired"`
4. Keeps renewal loop alive (process stays up)
5. Logs: *"Broker registration expired. Registered as drained. Use `danube-admin brokers activate` to resume."*

**Admin intervention required:** inspect → fix root cause → `activate` → `rebalance`.

---

### Scenario 3a: Broker Restart < TTL (e.g., crash, systemd restarts in 5s)

| Time | Event |
|------|-------|
| T=0 | Broker C process dies. Renewal loop dies with it |
| T=5 | Broker C restarts. Reads same `node_id` from file. Opens persisted `raft-log.redb` |
| T=5 | `bootstrap_cluster()`: "already initialized" → returns early. Raft reconnects |
| T=5 | `/cluster/register/{C}` still exists (TTL not expired) → `register_broker()` renews it |
| T=5 | `set_broker_state("active")` |
| T=5 | **Problem:** TopicManager is empty. Topics assigned in metadata but broker can't serve them (Problem 2) |

**After Fix 2 (startup reconciliation):**

Before accepting client connections, broker scans
`/cluster/brokers/{my_id}/` and calls `ensure_local()` for each assigned topic.
Topics recreated in TopicManager. Normal operation resumes.

**Self-healing, no admin intervention.**

---

### Scenario 3b: Broker Restart > TTL (e.g., host down for 5 minutes)

| Time | Event |
|------|-------|
| T=0 | Broker C process dies |
| T=32 | TTL expires → registration deleted → topics reassigned to A, B |
| T=300 | Broker C restarts. Same `node_id`. Raft catches up from persisted log |
| T=300 | State machine now shows: no registration for C, no topic assignments for C |
| T=300 | Startup reconciliation: scans `/cluster/brokers/{C}/` → empty → nothing to reconcile |
| T=300 | **Current:** `register_broker()` creates registration, `set_broker_state("active")` → broker immediately eligible for new topics |

**After Fix 3 (stale-state detection at startup):**

On startup, before `register_broker()`:
1. Check if `/cluster/register/{my_id}` exists
2. It doesn't exist AND we have persisted Raft state (this is a restart, not first boot)
3. Register as `drained` with reason `"stale_restart"`
4. Log: *"Broker registration was expired (unavailable > TTL). Registered as drained."*

**Admin intervention required:** inspect → `activate` → `rebalance`.

---

### Scenario 4: Full Cluster Restart (all brokers, e.g., `make brokers`)

| Time | Event |
|------|-------|
| T=0 | All 3 brokers killed |
| T=0+ | No Raft leader → TTL worker not running → TTL keys frozen in state machine with absolute timestamps |
| T=? | All 3 restart. Raft reforms, elects leader |
| T=? | TTL worker starts. `now_ms()` > all `expires_at` → **immediately expires ALL registrations** |
| T=? | LoadManager fires `delete_topic_allocation` for ALL brokers → all topics to `/cluster/unassigned/` |
| T=? | Each broker calls `register_broker()` → new registrations created |
| T=? | LoadManager assigns topics from `/cluster/unassigned/` to available brokers |

This actually works end-to-end but is messy. The fixes improve it:
- **Fix 2:** Each broker reconciles its newly-assigned topics immediately
- **Fix 3:** If restart > TTL, all brokers register as `drained`. But this is
  wrong for a planned full restart — all brokers would need manual activation.

**Special case for full cluster restart:** When ALL registrations are expired
(not just ours), it's a full cluster restart, not a single broker failure. In
this case, registering as `active` is correct — there's no "healthy cluster" to
be cautious about rejoining.

Detection: at startup, check if **any** broker is registered. If zero brokers
registered → full restart → register as `active`.

---

## Implementation Plan

### Fix 1: TTL Renewal Resilience

**File:** `danube-broker/src/danube_service/broker_register.rs`

**Changes:**
1. Replace `break` with retry + exponential backoff (cap at 5s)
2. Track `consecutive_failure_duration` (sum of sleep intervals during failures)
3. When `consecutive_failure_duration > ttl`: set `registration_lost = true`
4. When a renewal succeeds after `registration_lost`:
   - Re-register with TTL (already done by the successful `put_with_ttl`)
   - Call `set_broker_state("drained", "registration_expired")` via metadata store
   - Log warning message for operator
   - Reset `registration_lost` and `consecutive_failure_duration`
   - Keep loop running (Option B)

**Signature change:** `register_broker` needs access to the broker state path to
set state to `drained`. Pass `broker_id` (already present) and the `MetadataStorage`
(already present). The state write path is:
`/cluster/brokers/{broker_id}/state` → `{"mode": "drained", "reason": "registration_expired"}`

```rust
// Pseudocode for the revised renewal loop:
let mut consecutive_failure_ms: u64 = 0;
let mut registration_lost = false;
let backoff_base = Duration::from_secs(1);
let backoff_max = Duration::from_secs(5);
let mut current_backoff = backoff_base;

loop {
    sleep(renew_interval).await;

    match store.put_with_ttl(&path, payload.clone(), ttl_duration).await {
        Ok(_) => {
            if registration_lost {
                // We recovered after being presumed dead.
                // Re-registration succeeded, but set state to drained.
                warn!(
                    broker_id = %broker_id,
                    "Registration recovered after expiry. Setting broker to drained."
                );
                let state_path = format!("/cluster/brokers/{}/state", broker_id);
                let drained = json!({"mode": "drained", "reason": "registration_expired"});
                let _ = store.put(&state_path, drained, MetaOptions::None).await;
                registration_lost = false;
            }
            consecutive_failure_ms = 0;
            current_backoff = backoff_base;
        }
        Err(e) => {
            consecutive_failure_ms += renew_interval.as_millis() as u64;
            error!(
                broker_id = %broker_id,
                consecutive_failure_ms,
                error = %e,
                "Failed to renew broker registration"
            );
            if consecutive_failure_ms > ttl_duration.as_millis() as u64 {
                if !registration_lost {
                    warn!(
                        broker_id = %broker_id,
                        "Registration likely expired (failures > TTL). Will register as drained on recovery."
                    );
                    registration_lost = true;
                }
            }
            sleep(current_backoff).await;
            current_backoff = (current_backoff * 2).min(backoff_max);
            continue;
        }
    }
}
```

**Resolves:** Scenario 1 (fully), Scenario 2 running-process variant.

---

### Fix 2: Startup Topic Reconciliation

**File:** `danube-broker/src/danube_service.rs` (in `DanubeService::start()`)

**Where:** After Raft is ready and before starting the client-facing gRPC
server. Specifically, after `register_broker()` and `set_broker_state()`, before
the `broker_watcher` and server handles.

**Changes:**
1. Scan `/cluster/brokers/{broker_id}/` using `get_childrens()`
2. For each child path (format: `/cluster/brokers/{id}/{ns}/{topic}`):
   - Extract namespace and topic name
   - Call `ensure_local()` on the `TopicManager` to recreate in memory
3. Log count of reconciled topics

```rust
// Pseudocode — in DanubeService::start(), after register_broker():
let broker_path = format!("/cluster/brokers/{}", self.broker_id);
match self.meta_store.get_childrens(&broker_path).await {
    Ok(children) => {
        let mut reconciled = 0;
        for full_path in &children {
            let parts: Vec<&str> = full_path.split('/').collect();
            // /cluster/brokers/{id}/{ns}/{topic} → parts[4]=ns, parts[5]=topic
            if parts.len() >= 6 {
                // Skip the "state" key
                if parts[5] == "state" || parts[4] == "state" {
                    continue;
                }
                let topic_name = format!("/{}/{}", parts[4], parts[5]);
                match self.broker.topic_manager.ensure_local(&topic_name).await {
                    Ok(_) => {
                        reconciled += 1;
                        info!(topic = %topic_name, "reconciled topic on startup");
                    }
                    Err(e) => {
                        warn!(topic = %topic_name, error = %e, "failed to reconcile topic");
                    }
                }
            }
        }
        if reconciled > 0 {
            info!(count = reconciled, "startup reconciliation complete");
        }
    }
    Err(e) => {
        warn!(error = %e, "failed to scan broker assignments for reconciliation");
    }
}
```

**Note:** The `state` key (`/cluster/brokers/{id}/state`) is also a child — we
must skip it. Filter by path depth or by checking if the path matches the
`{ns}/{topic}` pattern.

**Resolves:** Scenario 3a (fully), Scenario 4 (topic loading after reassignment).

---

### Fix 3: Stale-State Detection at Startup

**File:** `danube-broker/src/danube_service.rs` (in `DanubeService::start()`)

**Where:** After Raft is ready, **before** `register_broker()`.

**Changes:**
1. Check if `/cluster/register/{my_id}` exists in metadata
2. Check if this is a restart (persisted Raft state) vs first boot
3. If registration missing AND restart → set initial state to `drained`
4. Special case: if **zero** brokers registered → full cluster restart → `active`

**How to detect restart vs first boot:**

`bootstrap_cluster()` already knows this — it returns early when it finds
persisted membership. We change it to return a boolean:

```rust
// In danube-raft/src/node.rs:
pub async fn bootstrap_cluster(...) -> anyhow::Result<bool> {
    // Returns true if cluster was already initialized (restart)
    // Returns false if this is a fresh initialization
}
```

Then in `main.rs`:

```rust
let was_restart = raft_node.bootstrap_cluster(...).await?;
```

**Startup decision logic:**

```rust
// In DanubeService::start(), before register_broker():
let my_reg_path = format!("/cluster/register/{}", self.broker_id);
let my_reg_exists = self.meta_store
    .get(&my_reg_path, MetaOptions::None).await
    .ok().flatten().is_some();

let initial_state = if self.join_cluster {
    // --join mode: always drained (existing behavior)
    "drained"
} else if was_restart && !my_reg_exists {
    // Restart after TTL expired. But check: is this a full cluster restart?
    let all_regs = self.meta_store
        .get_childrens("/cluster/register").await
        .unwrap_or_default();
    if all_regs.is_empty() {
        // No brokers registered at all → full cluster restart → active
        info!("Full cluster restart detected (no brokers registered). Registering as active.");
        "active"
    } else {
        // Other brokers are alive but we were declared dead → drained
        warn!(
            "Broker registration expired (unavailable > TTL). \
             Registering as drained. Use `danube-admin brokers activate` to resume."
        );
        "drained"
    }
} else {
    // First boot OR restart within TTL → active
    "active"
};
```

Then pass `initial_state` to `set_broker_state()` instead of the current
if/else on `self.join_cluster`.

**Resolves:** Scenario 2 (restart variant), Scenario 3b, Scenario 4 (special case).

---

## Implementation Order

| Phase | Fix | Effort | Scenarios Resolved |
|-------|-----|--------|--------------------|
| **Phase 1** | Fix 2: Startup Reconciliation | ~30 lines | 3a, 4 (topic loading) |
| **Phase 2** | Fix 1: Renewal Resilience | ~40 lines | 1, 2 (running process) |
| **Phase 3** | Fix 3: Stale-State Detection | ~30 lines, small change to `bootstrap_cluster` return type | 2 (restart), 3b, 4 (full restart) |

**Rationale for order:**
- Fix 2 is the most impactful for day-to-day development (`make brokers` restart)
  and has zero risk of side effects.
- Fix 1 is a clear improvement with no behavior change in the happy path.
- Fix 3 changes startup state logic, which affects all scenarios — do it last
  after the simpler fixes are proven.

---

## Testing Strategy

### Fix 1 Tests
- **Unit:** Mock `put_with_ttl` to fail N times then succeed. Verify loop
  doesn't break. Verify `registration_lost` flag is set after TTL duration.
  Verify state set to `drained` on recovery.
- **E2E:** Start 3 brokers. Block one broker's Raft port with iptables for 15s
  (< TTL). Verify it recovers without topic reassignment.

### Fix 2 Tests
- **Integration:** Start broker, create topics, kill and restart (same
  data-dir). Verify topics are servable after restart.
- **E2E:** `make brokers`, create topics, `make brokers-clean`, re-run `make
  brokers` (without `data-clean`). Verify topics are assigned and serving.

### Fix 3 Tests
- **Integration:** Start broker, create topics, kill broker, wait > TTL (or
  manually expire registration), restart. Verify broker registers as `drained`.
  Verify `brokers activate` transitions to `active`.
- **E2E:** 3-node cluster, kill one broker, wait 40s, restart. Verify it comes
  back as `drained`. Verify the other 2 brokers have the topics.

---

## Files Modified

| File | Fix | Change |
|------|-----|--------|
| `danube-broker/src/danube_service/broker_register.rs` | 1 | Retry loop, backoff, registration_lost tracking, drained on recovery |
| `danube-broker/src/danube_service.rs` | 2, 3 | Startup reconciliation scan, stale-state detection before registration |
| `danube-raft/src/node.rs` | 3 | `bootstrap_cluster()` returns `bool` (was_restart) |
| `danube-broker/src/main.rs` | 3 | Capture `was_restart` from bootstrap, pass to DanubeService |

---

## Appendix: Metadata Paths Reference

| Path | Purpose | TTL? |
|------|---------|------|
| `/cluster/register/{broker_id}` | Broker liveness registration (URL, admin addr) | Yes (32s) |
| `/cluster/brokers/{broker_id}/state` | Broker operational state (active/drained/draining) | No |
| `/cluster/brokers/{broker_id}/{ns}/{topic}` | Topic assignment to broker | No |
| `/cluster/unassigned/{ns}/{topic}` | Topics awaiting assignment | No |
| `/topics/{ns}/{topic}` | Topic metadata (dispatch strategy, schema, etc.) | No |

No new metadata fields or paths are introduced by these fixes.
