# Phase 6: Broker Cleanup Progress

**Date:** December 29, 2025
**Status:** 🔄 IN PROGRESS

---

## ✅ Completed

### 1. Admin Tools Schema Removal
**Files Modified:**
- `danube-broker/src/admin/topics_admin.rs`
  - ✅ Commented out old `Schema` and `SchemaType` imports
  - ✅ Commented out schema creation logic in `create_topic()`
  - ✅ Commented out schema creation logic in `create_partitioned_topic()`
  - ✅ Commented out schema retrieval logic in `describe_topic()`
  - ✅ Updated both methods to pass `None` for schema parameter

### 2. Synchronizer Schema Removal  
**Files Modified:**
- `danube-broker/src/danube_service/syncronizer.rs`
  - ✅ Commented out `SchemaType` import
  - ✅ Commented out `.with_schema()` call in producer creation

### 3. Resources Schema Import Removal
**Files Modified:**
- `danube-broker/src/resources/topic.rs`
  - ✅ Commented out `Schema` import

---

## 🔄 In Progress / Remaining

### 4. Topic Resources Schema Methods
**File:** `danube-broker/src/resources/topic.rs`
**Status:** ⏳ NEEDS COMMENTING OUT
**Methods to handle:**
- `add_topic_schema()` - Line 52-62
- `get_schema()` - Line 189-196

**Action:** Comment out these methods as they use old Schema type

### 5. Topic Schema Import
**File:** `danube-broker/src/topic.rs`
**Status:** ⏳ NEEDS FIX
**Error:** `unresolved import crate::schema::Schema`
**Action:** Comment out the Schema import

### 6. SchemaRegistryService Trait Bound
**File:** `danube-broker/src/broker_server.rs`
**Status:** ❌ CRITICAL ERROR
**Error:** `the trait bound &SchemaRegistryService: SchemaRegistry is not satisfied`
**Lines:** 93, 104
**Action:** Change from `&schema_registry_service` to `schema_registry_service.clone()` or wrap in Arc

### 7. JSON Schema Validator Calls
**Files:**
- `danube-broker/src/schema/formats/json_schema.rs` - Lines 21, 55
- `danube-broker/src/schema/validator.rs` - Lines 122, 131
**Status:** ❌ ERROR
**Error:** `cannot find function validator_for in crate jsonschema`
**Cause:** jsonschema crate API changed
**Action:** Update to use new API: `JSONSchema::compile()` instead of `validator_for()`

###8. Broker Tests (~20 files)
**Status:** ⏳ DEFERRED
**Error Pattern:** `unresolved import danube_client::SchemaType` and `no method named with_schema`
**Files Affected:**
- reliable_dispatch_heartbeat.rs
- subscription_basic.rs
- partitioning_validation.rs
- partitioned_variants.rs
- consumer_churn.rs
- reliable_dispatch_reconnection.rs
- queuing_shared_round_robin.rs
- pubsub_fanout_exclusive.rs
- producer_reconnect.rs
- reliable_dispatch_basic.rs
- partitioned_basic.rs
- ... and others

**Action:** Will be migrated to new schema API in Phase 7 when we re-enable tests

---

## 📋 Next Steps (Priority Order)

1. **SchemaRegistryService trait bound** - BLOCKING
2. **Comment out remaining Schema usages in resources/topic.rs**
3. **Remove Schema import from topic.rs**
4. **Fix jsonschema validator calls** (if time permits)
5. **Document test migration strategy for Phase 7**

---

## 🎯 Success Criteria

Phase 6 cleanup is complete when:
- ✅ No more old `Schema`/`SchemaType` imports in broker src
- ✅ SchemaRegistryService trait bound resolved
- ✅ danube-broker compiles (ignoring test failures)
- ⚠️ jsonschema validator calls fixed (optional - can be Phase 7)
- ⏳ Tests deferred to Phase 7

---

## 📝 Notes

- **CLI tools disabled** in Cargo.toml - will be re-enabled and migrated in Phase 7
- **Test migration** deferred - all ~20 broker tests need to use new schema registry API
- **Old schema module** (`danube-broker/src/schema`) is partially unused but kept for reference
- **Warnings** about unused imports in `schema/mod.rs` are expected and safe to ignore
