# Topic Unload Implementation Plan

## Executive Summary

This document outlines the implementation plan for the `unload_topic` admin command, enabling graceful topic migration between brokers. The implementation differs significantly between **non-reliable** and **reliable** topics.

**Key Requirements:**
- Preserve schema, subscriptions, and cursors for reliable topics
- Ensure reassignment to a **different** broker
- Minimize downtime during migration
- Prevent data loss for reliable topics

---

## Architecture Analysis

### Current State (ETCD Paths)

**Preserved for Reliable Topics (Essential Data Only):**
```
/topics/{namespace}/{topic}/delivery                        - "Reliable" (KEEP)
/topics/{namespace}/{topic}/schema                          - Schema definition (KEEP)
/topics/{namespace}/{topic}/subscriptions/{sub_name}        - Subscription metadata (KEEP)
/topics/{namespace}/{topic}/subscriptions/{sub_name}/cursor - Last acked offset (KEEP)
/danube-data/storage/topics/{namespace}/{topic}/**          - Storage objects (KEEP)
```

**Deleted During Unload (All Topics):**
```
/cluster/brokers/{broker_id}/{namespace}/{topic}            - Broker assignment
/topics/{namespace}/{topic}/producers/**                     - Producer metadata (DELETE)
```

**Deleted for Non-Reliable Topics Only:**
```
/topics/{namespace}/{topic}/subscriptions/**                - All subscriptions (optional)
```

**Created for Reassignment:**
```
/cluster/unassigned/{namespace}/{topic}                     - Reassignment marker
```

### Key Differences: Delete vs Unload

| Operation | Non-Reliable | Reliable |
|-----------|-------------|----------|
| **Delete** | Remove all metadata + storage | Remove all metadata + storage |
| **Unload** | Remove all metadata (recreate on new broker) | Keep only: schema, delivery, subscriptions, cursors, storage |

### Metadata Strategy

**Non-Reliable Topics:**
- Preserve minimal metadata needed for immediate reassignment hosting:
  - Keep: delivery (dispatch strategy), schema, and namespace topic entry
  - Delete: producer metadata; optionally delete subscriptions metadata
  - Rationale: ensures new broker can materialize topic instantly while producers/consumers reattach cleanly

**Reliable Topics:**
- Keep ONLY essential data for continuity:
  - **Schema**: So producers can validate messages against topic schema
  - **Delivery Strategy**: Topic configuration
  - **Subscriptions**: Metadata about subscription types
  - **Cursors**: Last acknowledged offsets for resumption
  - **Storage Data**: Historical messages in cloud storage
- Delete producer metadata (producers must rejoin and recreate on new broker)

---

## Implementation Phases

### Phase 1: Admin CLI Command
**File:** `danube-admin-cli/src/topics.rs`

Add `Unload` variant to `TopicsCommands` enum:
```rust
Unload {
    topic: String,
    namespace: Option<String>,
}
```

Add handler that calls `client.unload_topic(request).await`.

---

### Phase 2: Proto Definition
**File:** `danube-core/proto/DanubeAdmin.proto`

```proto
service TopicAdmin {
  rpc UnloadTopic(TopicRequest) returns (TopicResponse);
}
```

---

### Phase 3: Topic Manager - Core Unload Logic
**File:** `danube-broker/src/topic_control.rs`

#### 3.1 Main Unload Entry Point
```rust
pub(crate) async fn unload_topic(&self, topic_name: &str) -> Result<()> {
    let is_reliable = /* check dispatch strategy */;
    
    if is_reliable {
        self.unload_reliable_topic(topic_name).await
    } else {
        self.unload_non_reliable_topic(topic_name).await
    }
}
```

#### 3.2 Non-Reliable Unload
```rust
async fn unload_non_reliable_topic(&self, topic_name: &str) -> Result<()> {
    // 1. Mark as draining (reject new publishes)
    if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
        topic.unavailable_topic().await;
    }
    
    // 2. Close connections
    let topic = self
        .topic_worker_pool
        .remove_topic_from_worker(topic_name)
        .ok_or_else(|| anyhow!("Failed to remove topic from worker pool"))?;
    
    let (producers, consumers) = topic.close().await?;
    
    // 3. Clean up local registries
    for producer_id in producers {
        self.producers.remove(&producer_id);
    }
    for consumer_id in consumers {
        self.consumers.remove(&consumer_id);
    }
    
    // 4. DELETE metadata (for clean recreation on new broker)
    let mut resources = self.resources.lock().await;
    let _ = resources.namespace.delete_topic(topic_name).await;
    let _ = resources.topic.delete_all_producers(topic_name).await;
    let _ = resources.topic.delete_all_subscriptions(topic_name).await;
    let _ = resources.topic.delete_topic_delivery(topic_name).await;
    let _ = resources.topic.delete_topic_schema(topic_name).await;
    let _ = resources.topic.delete_topic_root(topic_name).await;
    
    gauge!(BROKER_TOPICS.name, "broker" => self.broker_id.to_string()).decrement(1);
    
    info!(
        "Non-reliable topic {} unloaded from broker {} (metadata deleted for clean recreation)",
        topic_name, self.broker_id
    );
    
    Ok(())
}
```

#### 3.3 Reliable Unload
```rust
async fn unload_reliable_topic(&self, topic_name: &str) -> Result<()> {
    // 1. Mark as draining (reject new publishes during unload)
    if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
        topic.unavailable_topic().await;
    }
    
    // 2. Flush subscription cursors (bypass debounce, best-effort)
    if let Err(e) = self.flush_subscription_cursors(topic_name).await {
        warn!(
            "Some subscription cursors failed to flush for {}: {}. Continuing with unload.",
            topic_name, e
        );
    }
    
    // 3. Flush and seal WAL storage
    if let Err(e) = self.flush_and_seal(topic_name).await {
        error!(
            "flush_and_seal failed during unload for {}: {}",
            topic_name, e
        );
        return Err(e);
    }
    
    // 4. Close local connections
    let topic = self
        .topic_worker_pool
        .remove_topic_from_worker(topic_name)
        .ok_or_else(|| anyhow!("Failed to remove topic from worker pool"))?;
    
    let (producers, consumers) = topic.close().await?;
    
    // 5. Clean up local registries
    for producer_id in producers {
        self.producers.remove(&producer_id);
    }
    for consumer_id in consumers {
        self.consumers.remove(&consumer_id);
    }
    
    // 6. DELETE producer metadata (producers must rejoin on new broker)
    {
        let mut resources = self.resources.lock().await;
        if let Err(e) = resources.topic.delete_all_producers(topic_name).await {
            warn!("Failed to delete producer metadata for {}: {}", topic_name, e);
        }
    }
    
    // 7. KEEP essential metadata:
    //    - Schema (for producer validation)
    //    - Delivery strategy
    //    - Subscriptions (metadata + cursors)
    //    - Storage data (already sealed in cloud)
    
    gauge!(BROKER_TOPICS.name, "broker" => self.broker_id.to_string()).decrement(1);
    
    info!(
        "Reliable topic {} unloaded from broker {} (schema, subscriptions, cursors, and storage preserved)",
        topic_name, self.broker_id
    );
    
    Ok(())
}
```

#### 3.4 Cursor Flush Helper (Best-Effort)
```rust
async fn flush_subscription_cursors(&self, topic_name: &str) -> Result<()> {
    if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
        let subscriptions = topic.subscriptions.lock().await;
        let mut failed_subs = Vec::new();
        
        for (sub_name, subscription) in subscriptions.iter() {
            if let Some(dispatcher) = &subscription.dispatcher {
                if let Err(e) = dispatcher.flush_progress_now().await {
                    warn!(
                        "Failed to flush cursor for subscription {} on topic {}: {}",
                        sub_name, topic_name, e
                    );
                    failed_subs.push(sub_name.clone());
                }
            }
        }
        
        // Return error only if ALL subscriptions failed
        // Partial failures are acceptable (continue with unload)
        if !failed_subs.is_empty() && failed_subs.len() == subscriptions.len() {
            return Err(anyhow!(
                "All subscription cursor flushes failed for topic {}",
                topic_name
            ));
        }
    }
    Ok(())
}
```

---

### Phase 4: Load Manager - Exclude Current Broker
**File:** `danube-broker/src/danube_service/load_manager.rs`

#### 4.1 Detect Unload Requests

Modify `handle_unassigned_topic()` to check for unload marker:

```rust
async fn handle_unassigned_topic(...) -> Result<()> {
    // Parse value JSON
    let unload_info = parse_unload_marker(value)?;
    
    if let Some(old_broker_id) = unload_info.from_broker {
        // Unload: exclude old broker
        self.assign_topic_to_different_broker(event, old_broker_id).await;
    } else {
        // New topic: any broker OK
        self.assign_topic_to_broker(event).await;
    }
}
```

#### 4.2 Assignment with Exclusion

```rust
async fn get_next_broker_excluding(&self, exclude_id: u64) -> Result<u64> {
    let rankings = self.rankings.lock().await;
    
    // Return first broker that is not excluded
    for (broker_id, _) in rankings.iter() {
        if *broker_id != exclude_id {
            return Ok(*broker_id);
        }
    }
    
    // If only one broker exists (single broker cluster), unload is not allowed
    Err(anyhow!(
        "Cannot unload topic: no alternative broker available. \
         Unload requires at least 2 brokers in the cluster."
    ))
}
```

#### 4.3 Modified assign_topic_to_different_broker

```rust
async fn assign_topic_to_different_broker(
    &mut self,
    event: WatchEvent,
    exclude_broker_id: u64,
) {
    match event {
        WatchEvent::Put { key, .. } => {
            self.calculate_rankings_simple().await;
            
            let key_str = match std::str::from_utf8(&key) {
                Ok(s) => s,
                Err(e) => {
                    error!("Invalid UTF-8 in key: {}", e);
                    return;
                }
            };
            
            let parts: Vec<_> = key_str.split(BASE_UNASSIGNED_PATH).collect();
            let topic_name = parts[1];
            
            // Get next broker, excluding the current one
            let broker_id = match self.get_next_broker_excluding(exclude_broker_id).await {
                Ok(id) => id,
                Err(e) => {
                    error!(
                        "Cannot reassign topic {} for unload: {}",
                        topic_name, e
                    );
                    // Keep unassigned marker so reassignment can occur later
                    return;
                }
            };
            
            let path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string(), topic_name]);
            
            match self
                .meta_store
                .put(&path, serde_json::Value::Null, MetaOptions::None)
                .await
            {
                Ok(_) => info!(
                    "Topic {} reassigned to broker {} (excluded broker {})",
                    topic_name, broker_id, exclude_broker_id
                ),
                Err(err) => warn!(
                    "Unable to reassign topic {} to broker {}: {}",
                    topic_name, broker_id, err
                ),
            }
            
            // Delete unassigned entry
            if let Err(err) = self.meta_store.delete(key_str).await {
                warn!(
                    "Failed to delete unassigned entry {} after reassignment: {}",
                    key_str, err
                );
            }
            
            // Update internal state
            let mut brokers_usage = self.brokers_usage.lock().await;
            if let Some(load_report) = brokers_usage.get_mut(&broker_id) {
                load_report.topics_len += 1;
                load_report.topic_list.push(topic_name.to_string());
            }
        }
        WatchEvent::Delete { .. } => (),
    }
}
```

---

### Phase 5: Unload Marker Format
**File:** `danube-broker/src/resources/cluster.rs`

```rust
pub(crate) async fn mark_topic_for_unload(
    &mut self,
    topic_name: &str,
    from_broker_id: u64,
) -> Result<()> {
    let marker = serde_json::json!({
        "reason": "unload",
        "from_broker": from_broker_id
    });
    let path = join_path(&[BASE_UNASSIGNED_PATH, topic_name]);
    self.create(&path, marker).await
}
```

---

### Phase 6: Admin RPC Handler
**File:** `danube-broker/src/danube_admin_services/topic_admin.rs`

```rust
async fn unload_topic(&self, request: Request<TopicRequest>) 
    -> Result<Response<TopicResponse>, Status> 
{
    let topic_name = request.into_inner().name;
    
    // 1. Verify topic is local
    let (is_local, _) = self.broker.lookup_topic(&topic_name).await
        .ok_or_else(|| Status::not_found("Topic not found"))?;
    
    if !is_local {
        return Err(Status::failed_precondition(
            "Topic not hosted on this broker"
        ));
    }
    
    // 2. Check if there are other brokers available
    let broker_count = self.broker.resources.lock().await
        .cluster
        .get_brokers()
        .await
        .len();
    
    if broker_count < 2 {
        return Err(Status::failed_precondition(
            "Cannot unload topic: single broker cluster detected. \
             Unload requires at least 2 brokers. Use delete if you want to remove the topic."
        ));
    }
    
    // 3. Unload locally
    self.broker.unload_topic(&topic_name).await
        .map_err(|e| Status::internal(format!("Unload failed: {}", e)))?;
    
    // 4. Create unload marker in unassigned path
    self.broker.resources.lock().await
        .cluster
        .mark_topic_for_unload(&topic_name, self.broker.broker_id)
        .await
        .map_err(|e| Status::internal(format!("Failed to mark for unload: {}", e)))?;
    
    // 5. Delete broker assignment
    let path = format!("/cluster/brokers/{}{}", self.broker.broker_id, topic_name);
    self.broker.resources.lock().await
        .cluster
        .delete(&path)
        .await
        .map_err(|e| Status::internal(format!("Failed to delete assignment: {}", e)))?;
    
    Ok(Response::new(TopicResponse { success: true }))
}
```

---

### Phase 7: Dispatcher Flush Method
**File:** `danube-broker/src/dispatcher/unified_single.rs`

```rust
pub(crate) async fn flush_progress_now(&self) -> Result<()> {
    let mut engine = self.engine.lock().await;
    engine.flush_progress_now().await
}
```

Note: `SubscriptionEngine::flush_progress_now()` already exists (line 136-148).

---

## Testing Strategy

### Test 1: Non-Reliable Topic
1. Create topic on broker-1
2. Publish 100 messages
3. Unload topic
4. Verify reassignment to broker-2 or broker-3 (not broker-1)
5. Producers/consumers reconnect automatically

### Test 2: Reliable Topic with Cursors
1. Create reliable topic on broker-1
2. Publish 1000 messages
3. Subscribe with "sub1", consume 500 messages
4. Unload topic
5. Verify:
   - Schema preserved in ETCD
   - Subscription "sub1" preserved
   - Cursor at offset 499
   - Storage data intact
6. Reconnect consumer to new broker
7. Verify consumption resumes from message 501

### Test 3: Multiple Subscriptions
1. Reliable topic with 3 subscriptions at different offsets
2. Unload topic
3. Verify all 3 cursors preserved correctly

### Test 4: Single Broker Cluster (Failure Case)
1. Create single broker cluster
2. Create topic on broker-1
3. Attempt to unload topic
4. Verify:
   - RPC returns `Status::failed_precondition`
   - Error message: "Cannot unload topic: single broker cluster detected..."
   - Topic remains on broker-1 (unchanged)

### Test 5: Partial Cursor Flush Failure
1. Create reliable topic with 3 subscriptions
2. Simulate cursor flush failure for 1 subscription (e.g., ETCD unavailable temporarily)
3. Unload topic
4. Verify:
   - Warning logged for failed subscription
   - Unload continues successfully
   - Other 2 subscriptions have persisted cursors
   - Failed subscription may lose some progress (acceptable)

---

## Files to Modify

| File | Changes |
|------|---------|
| `danube-admin-cli/src/topics.rs` | Add Unload command variant + handler |
| `danube-core/proto/DanubeAdmin.proto` | Add UnloadTopic RPC |
| `danube-broker/src/topic_control.rs` | Add 3 unload methods + cursor flush |
| `danube-broker/src/danube_service/load_manager.rs` | Add exclusion logic |
| `danube-broker/src/resources/cluster.rs` | Add mark_topic_for_unload |
| `danube-broker/src/danube_admin_services/topic_admin.rs` | Implement RPC handler |
| `danube-broker/src/dispatcher/unified_single.rs` | Add flush wrapper |
| `danube-broker/src/broker_service.rs` | Add unload_topic wrapper |
| `danube-broker/src/topic_cluster.rs` | Add post_unload_topic |

**Estimated Lines:** ~450-500 new lines

---

## Implementation Order

1. Proto definition (ensures compilation)
2. Topic Manager unload methods (core logic)
3. Load Manager exclusion logic
4. Resources helper (unload marker)
5. Admin RPC handler
6. CLI command
7. Integration tests

---

## Error Handling

| Error | Response | Behavior |
|-------|----------|----------|
| Topic not found | `Status::not_found` | Abort |
| Wrong broker | `Status::failed_precondition` | Abort |
| Single broker cluster | `Status::failed_precondition` | Abort - unload not allowed |
| WAL flush fails | `Status::internal` | Abort - data integrity critical |
| Cursor flush partial failure | Log warning | Continue - best effort |
| Cursor flush total failure | Log error | Continue - acceptable data loss |
| Load Manager can't find alternative broker | Log error, keep unassigned marker | Abort reassignment (marker retained for future assignment) |

---

## Performance Impact

- **Non-reliable**: ~100-500ms downtime
- **Reliable**: ~1-3s downtime (WAL flush + seal)
- **No message loss** for reliable topics
- **Brief message loss** for non-reliable (expected behavior)

---

## Implementation Status

Phases implemented:
- **Phase 1: Admin CLI Command**
  - Added `topics unload` subcommand in `danube-admin-cli/src/topics.rs` calling `UnloadTopic`.
- **Phase 2: Proto Definition**
  - Added `rpc UnloadTopic(TopicRequest) returns (TopicResponse)` to `TopicAdmin`.
- **Phase 3: Broker Admin Handler**
  - `unload_topic` implemented in `danube-broker/src/admin/topics_admin.rs`.
  - Delegates to cluster via `TopicCluster::post_unload_topic`; checks single-broker precondition.
- **Phase 4: Dispatcher wrapper**
  - Added non-disruptive `flush_progress_now()` to both `unified_single.rs` and `unified_multiple.rs` and delegator in `dispatcher.rs`.
- **Phase 5: Load Manager**
  - `assign_topic_to_broker` parses unload markers and excludes `from_broker` via `get_next_broker_excluding`.
  - Leaves unassigned marker intact if reassignment fails.
- **Phase 6: TopicManager unload flow**
  - Implemented `unload_topic`, `unload_non_reliable_topic`, `unload_reliable_topic`, and `flush_subscription_cursors` in `topic_control.rs`.
  - Broker watcher updated to call `unload_topic` on unload-driven deletions.
- **Cluster resources helper**
  - Added `mark_topic_for_unload` in `resources/cluster.rs`.
- **TopicCluster orchestration**
  - Added `post_unload_topic` to create unload marker and schedule assignment deletion for the hosting broker.

Remaining work:
- **E2E tests** for unload scenarios (non-reliable and reliable), including cursors and WAL sealing verification.
- **Telemetry/logging polish** around unload flows and error cases.
- **Edge cases**: partitioned topics handling (if applicable) and retries/backoff tuning.

---

## Design Decisions (Clarified)

### ✅ Single Broker Cluster
**Decision:** Unload MUST FAIL  
**Rationale:** Unload is for migration, not deletion. Single broker clusters have no migration target. Users should use `delete_topic` instead.

### ✅ Partial Cursor Flush Failures
**Decision:** Continue with unload (best-effort)  
**Rationale:** 
- If some cursors flush successfully, those subscriptions can resume correctly
- Failed subscriptions will re-consume some messages (acceptable for high availability)
- Total failure still logged as error for monitoring

### ✅ WAL Flush Timeout
**Decision:** Not configurable in initial implementation  
**Rationale:** Can be addressed later based on production usage patterns

### ✅ Metadata Preservation Strategy

**Non-Reliable Topics:**
- **DELETE** all metadata (schema, subscriptions, producers, delivery strategy)
- **Rationale:** Clean slate prevents misalignment when producers/consumers recreate on new broker

**Reliable Topics:**
- **KEEP:** Schema, delivery strategy, subscriptions, cursors, storage data
- **DELETE:** Producer metadata
- **Rationale:** 
  - Schema needed for producer validation on new broker
  - Subscriptions + cursors enable resumption from last acked offset
  - Storage data contains historical messages
  - Producers must rejoin and recreate (fresh registration on new broker)

---

## Summary of Key Design Points

### Metadata Management
1. **Non-Reliable:** Delete ALL metadata for clean recreation
2. **Reliable:** Keep only essential data (schema, subscriptions, cursors, storage), delete producers

### Single Broker Protection
- Unload fails with clear error message in single broker clusters
- Forces users to use `delete_topic` for true removal

### Fault Tolerance
- Partial cursor flush failures: log warning, continue
- WAL flush failures: abort (data integrity critical)
- Load Manager assignment failures: abort with cleanup

### Load Manager Enhancement
- New marker format distinguishes unload from new topics
- `get_next_broker_excluding()` ensures different broker selection
- Graceful error handling when no alternative broker exists

---

## Next Steps

1. ✅ Plan reviewed and clarified
2. Begin implementation in suggested order:
   - Phase 1: Proto definition
   - Phase 2: Topic Manager (core unload logic)
   - Phase 3: Load Manager (exclusion logic)
   - Phase 4: Admin RPC handler
   - Phase 5: CLI command
3. Create integration tests for each scenario
4. Document behavior in user-facing docs
5. Consider adding metrics for unload operations (success/failure rates)
