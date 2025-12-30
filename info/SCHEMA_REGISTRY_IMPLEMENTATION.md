# Danube Schema Registry Implementation

**Status:** Production-Ready for JSON Schema  
**Date:** December 2025  
**Version:** Phase 1-5 Complete

---

## Executive Summary

This document describes the complete transformation of Danube's schema system from a basic, topic-coupled schema attachment mechanism to a production-grade, standalone Schema Registry inspired by Confluent Schema Registry and Apache Pulsar Schema Registry.

**What Changed:**
- **Before:** Schemas were simple metadata attached to topics with no versioning or validation
- **After:** Full-featured Schema Registry with versioning, compatibility checking, multi-layer validation, and type-safe client APIs

**Result:** Danube now supports enterprise-grade schema evolution with backward/forward/full compatibility modes, enabling safe schema changes across distributed systems.

---

## The Old System: How It Used to Work

### Architecture Limitations

**Schema Attachment Model:**
- Schemas were attached directly to topics during topic creation
- Stored as part of topic metadata in ETCD at `/topics/{topic_name}/schema`
- One schema per topic, immutable after creation
- No way to evolve schemas without recreating the topic

**Limited Schema Types:**
- Only 4 primitive types: `Bytes`, `String`, `Int64`, `JSON`
- JSON schemas stored as raw strings with no validation
- No support for structured formats like Avro or Protobuf
- No schema parsing or validation at registration time

**No Versioning:**
- Single schema version per topic
- No version history or ability to query old versions
- Breaking changes required topic recreation
- No compatibility checking between schema changes

**No Validation:**
- Producers could send any data regardless of declared schema
- No enforcement at producer, broker, or consumer level
- Schema was documentation only, not enforced
- Silent data corruption possible

**API Integration:**
- Schema defined in `ProducerRequest` protobuf message
- Producer sent schema with every connection (inefficient)
- No centralized schema management or discovery
- No client-side caching

### Why It Needed to Change

1. **No Schema Evolution:** Unable to add fields or change schemas without breaking consumers
2. **No Validation:** Schema declaration provided no guarantees about message content
3. **Inefficient:** Schema transmitted with every producer connection
4. **No Reuse:** Each topic needed its own schema copy, even if identical
5. **No Governance:** No way to track who created schemas or when they changed
6. **Limited Types:** Only primitive types, no structured formats

---

## The New System: What Was Implemented

### 1. Standalone Schema Registry

**Independent Service:**
- Schemas now managed separately from topics in dedicated Schema Registry service
- New gRPC service `SchemaRegistry` with 7 operations:
  - `RegisterSchema` - Register new schemas or versions
  - `GetSchema` - Retrieve schema by ID
  - `GetLatestSchema` - Get most recent version by subject
  - `ListVersions` - Get all versions for a subject
  - `CheckCompatibility` - Validate schema changes before registration
  - `DeleteSchemaVersion` - Remove specific versions
  - `SetCompatibilityMode` - Configure compatibility rules per subject

**Storage Architecture:**
- Schemas stored in ETCD at `/schemas/{subject}/metadata` and `/schemas/{subject}/versions/{n}`
- Separate from topic metadata for independent lifecycle management
- Subject-based naming (industry standard from Kafka/Confluent)
- Each subject can have multiple versions with full history

**Implementation Files:**
- `danube-broker/src/broker_server/schema_registry_handler.rs` - gRPC service handler
- `danube-broker/src/schema/registry.rs` - Core registry logic
- `danube-broker/src/resources/schema.rs` - ETCD storage layer
- `danube-core/proto/SchemaRegistry.proto` - Service definitions

### 2. Schema Versioning System

**Automatic Version Management:**
- Each schema registration increments version number automatically
- Versions are immutable once created
- Full version history retained indefinitely
- Fingerprint-based deduplication (SHA-256) prevents duplicate registrations

**Version References:**
- Topics reference schemas via `SchemaReference` containing subject name and version
- Three reference types supported:
  - **UseLatest:** Always use the newest version (dynamic)
  - **PinnedVersion:** Lock to specific version number (stable)
  - **MinVersion:** Require at least version N (rolling upgrade)

**Rich Metadata:**
- Every schema version includes:
  - Timestamp (created_at)
  - Creator (created_by)
  - Description (human-readable docs)
  - Tags (categorization)
  - Fingerprint (content hash for deduplication)
  - Compatibility mode (Backward/Forward/Full/None)

**Implementation Files:**
- `danube-broker/src/schema/metadata.rs` - Schema metadata structures
- `danube-broker/src/schema/types.rs` - Version types and enums

### 3. Multi-Layer Validation

**Producer-Side Validation:**
- Client applications handle serialization explicitly using `serde_json`
- Producers send serialized data with schema reference
- Schema ID automatically included in message metadata
- Compile-time type checking via Rust's type system

**Broker-Side Validation:**
- Topics configured with validation policy: None, Warn, or Enforce
- Schema ID validation ensures message matches topic schema
- Configurable per-topic via `ValidationPolicy` enum
- Metrics tracking validation attempts and failures
- Invalid messages rejected before entering topic

**Consumer-Side Validation:**
- Client applications handle deserialization explicitly using `serde_json`
- Option to validate struct definitions against registered schemas at startup
- Schema fetched from registry for validation
- Prevents runtime deserialization errors through startup validation

**Validation Policies:**
- **None:** Skip validation entirely (testing/development)
- **Warn:** Validate and log warnings but allow message (monitoring)
- **Enforce:** Validate and reject invalid messages (production)

**Implementation Files:**
- `danube-broker/src/schema/validator.rs` - Validation framework
- `danube-broker/src/topic.rs` - Topic validation integration
- `danube-client/src/producer.rs` - Producer-side validation
- `danube-client/src/consumer.rs` - Consumer-side validation

### 4. Extended Schema Type System

**Six Schema Types:**
- **Bytes:** Raw binary data (no validation)
- **String:** UTF-8 text validation
- **Number:** Numeric types (int32/int64/float/double)
- **JSON Schema:** Full JSON Schema validation with `jsonschema` crate
- **Avro:** Apache Avro support with schema storage (validation framework in place)
- **Protobuf:** Protocol Buffers support with schema storage (validation framework in place)

**JSON Schema Support (Fully Working):**
- Schema parsing and validation at registration
- Producer-side serialization with `serde`
- Consumer-side deserialization with type safety
- Schema validation before each message send
- Example implementations in `json_producer.rs` and `json_consumer.rs`

**Avro & Protobuf (Storage Ready):**
- Schemas can be registered and retrieved
- Storage and versioning fully functional
- Format-specific validation to be implemented in future phases
- Compatibility checking framework in place

**Implementation Files:**
- `danube-broker/src/schema/formats/` - Format-specific handlers
- `danube-broker/src/schema/compatibility.rs` - Compatibility checking
- `danube-client/src/schema_types.rs` - Type-safe enums

### 5. Compatibility Checking

**Four Compatibility Modes:**
- **Backward:** New schema can read old data (most common, default)
  - Use case: Consumers upgrade before producers
  - Allows: Add optional fields, remove fields
- **Forward:** Old schema can read new data
  - Use case: Producers upgrade before consumers
  - Allows: Add required fields, remove optional fields
- **Full:** Both backward and forward (strictest)
  - Use case: Critical schemas requiring both directions
  - Allows: Only safe changes (add optional fields)
- **None:** No validation (development/testing only)

**Per-Subject Configuration:**
- Each schema subject has its own compatibility mode
- Configurable via `set_compatibility_mode()` API
- Defaults to Backward (industry standard)
- Compatibility checked before new version registration

**Simplified Design:**
- Checks only against latest version (not transitive)
- Reduced complexity while covering 99% of use cases
- Can be extended to transitive modes in future if needed

**Implementation Files:**
- `danube-broker/src/schema/compatibility.rs` - Compatibility logic
- `danube-client/src/schema_types.rs` - CompatibilityMode enum

### 6. Type-Safe Client API

**SchemaRegistryClient:**
- New client for all schema operations
- Separate from message producer/consumer
- Methods for registration, retrieval, versioning, compatibility

**Type-Safe Enums:**
- `SchemaType` enum replaces string-based type specification
- `CompatibilityMode` enum for type-safe mode configuration
- IDE auto-completion shows all available options
- Compile-time validation prevents typos

**Producer Integration:**
- `with_schema_subject()` - Link producer to schema subject
- Serialization delegated to client applications
- Schema ID automatically included in message metadata
- Producers use standard `send()` method with pre-serialized data

**Consumer Integration:**
- Deserialization delegated to client applications
- Schema fetching from registry at startup for validation
- Optional struct validation against registered schema
- Consumers use standard `receive()` method and deserialize manually

**Example Usage Pattern:**
```
// Producer registers schema
schema_client.register_schema("events")
    .with_type(SchemaType::JsonSchema)
    .execute().await?;

// Producer references schema
producer.with_schema_subject("events").build();

// Consumer validates struct at startup
validate_struct_against_registry(&mut schema_client, "events", &sample).await?;
```

**Implementation Files:**
- `danube-client/src/schema_registry_client.rs` - Client API
- `danube-client/src/schema_types.rs` - Type enums
- `danube-client/examples/json_consumer_validated.rs` - Validation example

### 7. Caching Architecture

**Danube's Distributed Pattern:**
- **Writes:** Go to SchemaResources → ETCD (source of truth)
- **Reads:** Come from LocalCache (fast, eventually consistent)
- **Updates:** ETCD watch events automatically update LocalCache

**Why This Pattern:**
- Consistency with Danube's existing topic/subscription architecture
- Proper separation of write vs. read paths
- No cache invalidation complexity (watch-based updates)
- Cluster-wide consistency with local performance

**Schema Resolution:**
- Topics cache resolved schema IDs in `resolved_schema_id` field
- Fast validation using cached ID (no registry lookup per message)
- Cache automatically refreshed on schema updates via ETCD watch

**Implementation Files:**
- `danube-broker/src/danube_service/local_cache.rs` - Cache layer
- `danube-broker/src/resources/schema.rs` - Resource layer

### 8. Metrics and Monitoring

**Schema Validation Metrics:**
- `SCHEMA_VALIDATION_TOTAL` - Total validation attempts
- `SCHEMA_VALIDATION_FAILURES_TOTAL` - Failed validations

**Labels:**
- `topic` - Which topic validation occurred on
- `policy` - Validation policy (Warn/Enforce)
- `reason` - Failure reason (missing_schema_id, schema_mismatch)

**Use Cases:**
- Monitor schema enforcement adoption across topics
- Track validation failure rates for debugging
- Alert on high failure rates indicating schema issues
- Measure impact of schema changes on system

**Implementation Files:**
- `danube-broker/src/broker_metrics.rs` - Metrics definitions
- `danube-broker/src/topic.rs` - Metrics recording

---

## Architecture Changes

### Message Flow Transformation

**Old Flow:**
```
Producer → (with full schema) → Broker → Topic → Consumer
```

**New Flow:**
```
Producer → Schema Registry (register/get schema_id)
         ↓
Producer → (with schema_id only) → Broker → Topic Validation → Consumer
         ↑                                     ↓
         └──────── Schema Registry ←──────────┘
              (resolve schema_id)
```

### Protocol Changes

**ProducerRequest:**
- Removed: `schema` field (old Schema message)
- Added: `schema_ref` field (SchemaReference with subject + version)
- Schema sent once during registration, not per connection

**StreamMessage:**
- Added: `schema_id` field (u64, global identifier)
- Added: `schema_version` field (u32, version number)
- Only 12 bytes overhead per message instead of full schema

**New Service:**
- Added: `SchemaRegistry` gRPC service (7 operations)
- Removed: `Discovery.GetSchema` RPC (functionality moved to SchemaRegistry)

### Storage Organization

**Before:**
```
/topics/{topic}/schema            # Single schema per topic
```

**After:**
```
/schemas/{subject}/metadata       # Subject metadata with all versions
/schemas/{subject}/versions/{n}   # Individual version data
/topics/{topic}/schema_ref        # Reference to schema subject
```

### Breaking Changes

**Complete API Redesign:**
- Old `Schema` struct removed from client API
- Old `SchemaType` enum removed
- Old schema attachment methods removed
- New `SchemaReference` model requires migration

**Migration Path:**
- Old topics with attached schemas continue to work (legacy support in broker)
- New topics must use SchemaReference model
- Client code must update to use SchemaRegistryClient
- Examples provided showing migration patterns

---

## What's Working Now

### Production-Ready Features

✅ **Schema Registration & Retrieval**
- Register schemas with automatic versioning
- Retrieve by subject name or schema ID
- List all versions for a subject
- Fingerprint-based deduplication

✅ **JSON Schema Support**
- Full validation at producer and consumer
- Type-safe serialization/deserialization
- Working examples with real data
- Consumer struct validation at startup

✅ **Multi-Layer Validation**
- Producer-side via explicit serialization with schema validation
- Broker-side with ValidationPolicy
- Consumer-side via explicit deserialization with optional struct validation
- Configurable enforcement levels

✅ **Schema Versioning**
- Automatic version management
- Version pinning and latest-version tracking
- Rich metadata (timestamps, creators, descriptions, tags)
- Full version history

✅ **Type-Safe Client API**
- Enum-based schema types (no string guessing)
- Enum-based compatibility modes
- IDE auto-completion support
- Compile-time validation

✅ **Compatibility Modes**
- Four modes: None, Backward, Forward, Full
- Per-subject configuration
- Default to Backward (industry standard)
- API to set and query modes

✅ **Working Examples**
- `json_producer.rs` - Schema registration and typed sending
- `json_consumer.rs` - Typed receiving with deserialization
- `json_consumer_validated.rs` - Struct validation at startup
- `avro_producer.rs` / `avro_consumer.rs` - Avro schema registration
- `schema_evolution.rs` - Compatibility checking demonstration

### Framework in Place (Implementation Pending)

⏳ **Avro Validation**
- Schema storage and retrieval working
- Compatibility checking framework exists
- Format-specific validation logic to be added

⏳ **Protobuf Validation**
- Schema storage and retrieval working
- Compatibility checking framework exists
- Format-specific validation logic to be added

⏳ **Advanced Discovery**
- Full-text search across schemas
- Dependency tracking between schemas
- Deprecation markers

---

## File References

### Core Implementation

**Broker Schema Module:**
- `danube-broker/src/schema/mod.rs` - Module exports
- `danube-broker/src/schema/types.rs` - Core type definitions
- `danube-broker/src/schema/metadata.rs` - Schema metadata structures
- `danube-broker/src/schema/registry.rs` - Registry core logic
- `danube-broker/src/schema/validator.rs` - Validation framework
- `danube-broker/src/schema/compatibility.rs` - Compatibility checking
- `danube-broker/src/schema/formats/` - Format-specific handlers

**Broker Integration:**
- `danube-broker/src/broker_server/schema_registry_handler.rs` - gRPC handler
- `danube-broker/src/resources/schema.rs` - ETCD storage layer
- `danube-broker/src/danube_service/local_cache.rs` - Caching layer
- `danube-broker/src/topic.rs` - Topic validation integration
- `danube-broker/src/broker_metrics.rs` - Metrics definitions

**Protocol Definitions:**
- `danube-core/proto/SchemaRegistry.proto` - Schema Registry service
- `danube-core/proto/DanubeApi.proto` - Updated with schema fields

**Client SDK:**
- `danube-client/src/schema_registry_client.rs` - Schema client API
- `danube-client/src/schema_types.rs` - Type-safe enums
- `danube-client/src/producer.rs` - Producer integration
- `danube-client/src/consumer.rs` - Consumer integration

**Examples:**
- `danube-client/examples/json_producer.rs` - JSON schema producer
- `danube-client/examples/json_consumer.rs` - JSON schema consumer
- `danube-client/examples/json_consumer_validated.rs` - Validated consumer
- `danube-client/examples/avro_producer.rs` - Avro producer
- `danube-client/examples/avro_consumer.rs` - Avro consumer
- `danube-client/examples/schema_evolution.rs` - Compatibility demo
- `danube-client/examples/README.md` - Comprehensive examples guide

---

## Design Decisions

### Why Subject-Based Naming?

**Chose:** Subject-based model (like Kafka/Confluent)  
**Instead of:** Direct schema naming

**Rationale:**
- Industry standard terminology
- Clearer separation between schema identity (subject) and versions
- Enables multiple topics to share same subject
- Better governance and discovery

### Why Separate Schema Registry Service?

**Chose:** Dedicated gRPC service  
**Instead of:** Schema as topic metadata

**Rationale:**
- Schemas live independently of topics
- Single schema reusable across multiple topics
- Centralized governance and discovery
- Efficient: schema_id transmitted instead of full schema
- Enables schema evolution without topic recreation

### Why SchemaResources + LocalCache Pattern?

**Chose:** Danube's standard Resources pattern  
**Instead of:** Internal cache in SchemaRegistry

**Rationale:**
- Consistency with existing Danube architecture
- Proper separation of write (ETCD) vs. read (cache) paths
- Watch-based updates eliminate cache invalidation complexity
- Shared infrastructure with topics and subscriptions
- Cluster-wide consistency with local performance

### Why Remove send_typed/receive_typed from Core API?

**Chose:** Delegate serialization to client applications  
**Instead of:** Built-in typed send/receive

**Rationale:**
- Cleaner API separation of concerns
- Clients control their own serialization strategy
- Schema registry provides validation, not serialization
- More flexible for different serialization libraries
- Validation example shows recommended pattern

### Why Simplified Compatibility Modes?

**Chose:** 4 modes (None, Backward, Forward, Full)  
**Instead of:** 7 modes with transitive variants

**Rationale:**
- Covers 99% of real-world use cases
- Simpler implementation and testing
- Faster compatibility checks (only vs. latest)
- Can add transitive modes later if users need them
- Reduces cognitive load for users

### Why Type-Safe Enums in Client?

**Chose:** Rust enums for SchemaType and CompatibilityMode  
**Instead of:** String-based API

**Rationale:**
- IDE auto-completion shows all options
- Compile-time validation prevents typos
- Self-documenting API
- Industry best practice (like Pulsar's Java client)
- Better developer experience

---

## Impact on Existing Code

### Client Migration Required

**Applications using old Schema API must update to:**
1. Use `SchemaRegistryClient` to register schemas
2. Use `with_schema_subject()` instead of `with_schema()`
3. Handle serialization/deserialization explicitly
4. Update `StreamMessage` initializations to include `schema_id`/`schema_version` fields

**Examples provided for all common patterns**

### Broker Changes

**Topic Structure:**
- Added `schema_ref` field (SchemaReference)
- Added `resolved_schema_id` field (cached ID)
- Added `validation_policy` field
- Legacy `schema` field kept for backward compatibility

**Message Structure:**
- Added `schema_id` field (Option<u64>)
- Added `schema_version` field (Option<u32>)
- Backward compatible (fields are optional)

### Test Updates

**All test files creating StreamMessage updated:**
- Added `schema_id: None`
- Added `schema_version: None`
- Approximately 15 test files updated across persistent storage and client modules

---


## Future Enhancements

### Planned (Not Yet Implemented)

⏳ **Avro-Specific Validation**
- Parse Avro schemas using `apache-avro` crate
- Field-level compatibility checking
- Reader/writer schema resolution

⏳ **Protobuf-Specific Validation**
- Parse protobuf descriptors
- Field number compatibility checking
- Message compatibility validation

⏳ **Advanced Compatibility**
- Compatibility against all historical versions (transitive)
- Custom compatibility rules per subject
- Breaking change detection and warnings

⏳ **Discovery Enhancements**
- Full-text search across schemas and metadata
- Schema dependency graph visualization
- Deprecation markers and migration guides
- Auto-generated documentation

⏳ **Admin Tooling**
- CLI commands for schema management
- Admin gateway UI for visual management
- Bulk schema import/export
- Schema comparison tools

⏳ **Security & Governance**
- RBAC for schema operations
- Audit logging for all changes
- Schema approval workflows
- Size limits and quotas

---

## Summary

The Schema Registry implementation represents a fundamental transformation of Danube's schema capabilities, moving from a basic attachment system to an enterprise-grade schema management platform.

**Key Achievements:**
- ✅ Production-ready for JSON Schema use cases
- ✅ Full schema versioning with compatibility checking
- ✅ Multi-layer validation (producer, broker, consumer)
- ✅ Type-safe client API with excellent developer experience
- ✅ Distributed caching following Danube architecture patterns
- ✅ Comprehensive examples and documentation

**Foundation Laid:**
- ✅ Framework for Avro and Protobuf validation
- ✅ Extensible architecture for future schema types
- ✅ Metrics and monitoring infrastructure
- ✅ Clean API design following industry standards

**Status:** Ready for production use with JSON schemas. Avro and Protobuf support can be added incrementally as needed.
