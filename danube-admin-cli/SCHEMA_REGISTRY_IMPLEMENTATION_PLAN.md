# Schema Registry Implementation Plan for Admin CLI

## Executive Summary

The Danube admin CLI currently uses an **inline schema model** where schemas are embedded in topic creation requests. The new **Schema Registry system** introduces a centralized schema management service where schemas are registered independently and topics reference them by subject name.

This document outlines the changes needed to make `danube-admin-cli` compatible with the new Schema Registry while maintaining backward compatibility.

---

## Current State Analysis

### ✅ What Works Now
- **Topic Creation**: Uses inline `schema_type` and `schema_data` in `NewTopicRequest`
- **Topic Description**: Returns schema via `DescribeTopicResponse` with `type_schema` and `schema_data`
- **Admin Services**: BrokerAdmin, NamespaceAdmin, TopicAdmin via `DanubeAdmin.proto`

### ❌ Issues with New Schema Registry
1. **No Schema Registry Client**: Admin CLI cannot interact with Schema Registry service
2. **Inline Schema Model**: Topics still use embedded schemas instead of schema subjects
3. **No Schema Management**: Cannot register, update, or query schemas independently
4. **No Compatibility Management**: Cannot set or check compatibility modes
5. **No Version Management**: Cannot view or manage schema versions

---

## Architecture Changes Required

### 1. Proto Changes (`DanubeAdmin.proto`)

**Add schema subject support to topic creation:**

```protobuf
message NewTopicRequest {
  string name = 1;
  
  // DEPRECATED: Use schema_subject instead
  string schema_type = 2 [deprecated = true];
  string schema_data = 3 [deprecated = true];
  
  // NEW: Reference to registered schema subject
  optional string schema_subject = 6;
  
  DispatchStrategy dispatch_strategy = 4;
}

message PartitionedTopicRequest {
  string base_name = 1;
  uint32 partitions = 2;
  
  // DEPRECATED: Use schema_subject instead
  string schema_type = 3 [deprecated = true];
  string schema_data = 4 [deprecated = true];
  
  // NEW: Reference to registered schema subject
  optional string schema_subject = 7;
  
  DispatchStrategy dispatch_strategy = 5;
}

message DescribeTopicResponse {
  string name = 1;
  
  // OLD fields (keep for backward compatibility)
  int32 type_schema = 2;
  bytes schema_data = 3;
  
  repeated string subscriptions = 4;
  string broker_id = 5;
  string delivery = 6;
  
  // NEW fields for schema registry
  optional string schema_subject = 7;      // Schema subject name
  optional uint64 schema_id = 8;           // Schema ID
  optional uint32 schema_version = 9;      // Schema version
  optional string schema_type = 10;        // "json_schema", "avro", etc.
  optional string compatibility_mode = 11; // "BACKWARD", "FORWARD", etc.
}
```

**Why keep deprecated fields:**
- Backward compatibility with existing topics
- Gradual migration path
- Support for topics created before schema registry

---

### 2. Client Changes (`src/client.rs`)

**Add Schema Registry client:**

```rust
use danube_core::schema_proto::schema_registry_client::SchemaRegistryClient;

pub async fn schema_registry_client() -> Result<SchemaRegistryClient<Channel>, Box<dyn std::error::Error>> {
    let ch = admin_channel().await?;
    Ok(SchemaRegistryClient::new(ch))
}
```

---

### 3. New Module (`src/schema_registry.rs`)

**Create comprehensive schema management commands:**

```rust
#[derive(Debug, Args)]
pub(crate) struct SchemaRegistry {
    #[command(subcommand)]
    command: SchemaCommands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum SchemaCommands {
    /// Register a new schema or get existing ID
    Register {
        #[arg(help = "Schema subject name")]
        subject: String,
        
        #[arg(long, value_parser = ["json_schema", "avro", "protobuf", "string", "bytes"], 
              help = "Schema type")]
        schema_type: String,
        
        #[arg(long, help = "Path to schema definition file")]
        file: Option<String>,
        
        #[arg(long, help = "Inline schema definition")]
        schema: Option<String>,
        
        #[arg(long, help = "Human-readable description")]
        description: Option<String>,
        
        #[arg(long, help = "Tags for categorization (repeatable)")]
        tags: Vec<String>,
    },
    
    /// Get a schema by ID or subject
    Get {
        #[arg(long, conflicts_with = "subject", help = "Schema ID")]
        id: Option<u64>,
        
        #[arg(long, conflicts_with = "id", help = "Schema subject")]
        subject: Option<String>,
        
        #[arg(long, help = "Specific version (default: latest)")]
        version: Option<u32>,
        
        #[arg(long, value_parser = ["json", "yaml"], help = "Output format")]
        output: Option<String>,
    },
    
    /// List all versions for a subject
    Versions {
        #[arg(help = "Schema subject name")]
        subject: String,
        
        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },
    
    /// Check if a schema is compatible
    Check {
        #[arg(help = "Schema subject name")]
        subject: String,
        
        #[arg(long, help = "Path to new schema file")]
        file: Option<String>,
        
        #[arg(long, help = "Inline schema definition")]
        schema: Option<String>,
        
        #[arg(long, value_parser = ["json_schema", "avro", "protobuf"], 
              help = "Schema type")]
        schema_type: String,
        
        #[arg(long, help = "Override compatibility mode")]
        mode: Option<String>,
    },
    
    /// Set compatibility mode for a subject
    SetCompatibility {
        #[arg(help = "Schema subject name")]
        subject: String,
        
        #[arg(long, value_parser = ["none", "backward", "forward", "full"], 
              help = "Compatibility mode")]
        mode: String,
    },
    
    /// Delete a specific schema version
    Delete {
        #[arg(help = "Schema subject name")]
        subject: String,
        
        #[arg(long, help = "Version to delete")]
        version: u32,
        
        #[arg(long, help = "Confirm deletion")]
        confirm: bool,
    },
    
    /// List all schema subjects
    List {
        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },
}
```

---

### 4. Topics Module Updates (`src/topics.rs`)

**Support both inline schemas and schema subjects:**

```rust
TopicsCommands::Create {
    topic,
    namespace,
    partitions,
    schema,
    schema_file,
    schema_data,
    schema_subject,  // NEW
    dispatch_strategy,
} => {
    let topic_path = normalize_topic(&topic, namespace.as_deref())?;

    // Determine schema mode
    let (use_inline, schema_type, schema_payload, subject) = if let Some(subj) = schema_subject {
        // NEW: Use schema subject reference
        (false, String::new(), String::new(), Some(subj))
    } else {
        // OLD: Use inline schema (backward compatible)
        let payload = if let Some(path) = schema_file {
            Some(String::from_utf8(fs::read(path)?)?)
        } else { schema_data };
        (true, schema, payload.unwrap_or_else(|| "{}".into()), None)
    };

    if let Some(parts) = partitions {
        let req = PartitionedTopicRequest {
            base_name: topic_path,
            partitions: parts as u32,
            schema_type: if use_inline { schema_type.clone() } else { String::new() },
            schema_data: if use_inline { schema_payload.clone() } else { String::new() },
            schema_subject: subject.clone(),
            dispatch_strategy: parse_dispatch_strategy(&dispatch_strategy) as i32,
        };
        // ...
    }
}
```

**Add schema subject option:**

```rust
Create {
    // ... existing fields ...
    
    #[arg(long, conflicts_with_all = ["schema", "schema_file", "schema_data"],
          help = "Schema subject name (from schema registry)")]
    schema_subject: Option<String>,
}
```

**Update describe command to show schema registry info:**

```rust
TopicsCommands::Describe { topic, namespace, output } => {
    let name = normalize_topic(&topic, namespace.as_deref())?;
    let desc_resp = client.describe_topic(DescribeTopicRequest { name: name.clone() }).await?;
    let topic_info = desc_resp.into_inner();

    if matches!(output.as_deref(), Some("json")) {
        let out = serde_json::json!({
            "topic": topic_info.name,
            "broker_id": topic_info.broker_id,
            "delivery": topic_info.delivery,
            "schema_subject": topic_info.schema_subject,
            "schema_id": topic_info.schema_id,
            "schema_version": topic_info.schema_version,
            "schema_type": topic_info.schema_type,
            "compatibility_mode": topic_info.compatibility_mode,
            "subscriptions": topic_info.subscriptions,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Topic: {}", topic_info.name);
        println!("Broker ID: {}", topic_info.broker_id);
        println!("Delivery: {}", topic_info.delivery);
        
        // NEW: Schema registry info
        if let Some(subject) = topic_info.schema_subject {
            println!("\n📋 Schema Registry:");
            println!("  Subject: {}", subject);
            if let Some(id) = topic_info.schema_id {
                println!("  Schema ID: {}", id);
            }
            if let Some(version) = topic_info.schema_version {
                println!("  Version: {}", version);
            }
            if let Some(schema_type) = topic_info.schema_type {
                println!("  Type: {}", schema_type);
            }
            if let Some(mode) = topic_info.compatibility_mode {
                println!("  Compatibility: {}", mode);
            }
        } else {
            // Fallback to inline schema display
            println!("\n📋 Inline Schema (Legacy):");
            println!("  Type: {}", topic_info.type_schema);
            // ... display schema_data ...
        }
        
        println!("\nSubscriptions: {:?}", topic_info.subscriptions);
    }
}
```

---

### 5. Main Module Updates (`src/main.rs`)

**Add schema command:**

```rust
mod schema_registry;
use schema_registry::SchemaRegistry;

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(about = "Manage and view brokers information from the Danube cluster")]
    Brokers(Brokers),
    
    #[command(about = "Manage the namespaces from the Danube cluster")]
    Namespaces(Namespaces),
    
    #[command(name = "topics", about = "Manage the topics from the Danube cluster")]
    Topics(Topics),
    
    #[command(name = "schemas", about = "Manage schemas in the Schema Registry")]
    Schemas(SchemaRegistry),
}

match cli.command {
    Commands::Brokers(brokers) => brokers::handle_command(brokers).await?,
    Commands::Namespaces(namespaces) => namespaces::handle_command(namespaces).await?,
    Commands::Topics(topics) => topics::handle_command(topics).await?,
    Commands::Schemas(schemas) => schema_registry::handle_command(schemas).await?,
}
```

---

## Command Examples

### Schema Management

```bash
# Register a JSON schema
danube-admin-cli schemas register user-events \
  --schema-type json_schema \
  --file user-schema.json \
  --description "User event schema" \
  --tags analytics users

# Get latest schema
danube-admin-cli schemas get --subject user-events

# Get specific version
danube-admin-cli schemas get --subject user-events --version 2

# List all versions
danube-admin-cli schemas versions user-events --output json

# Check compatibility before registering
danube-admin-cli schemas check user-events \
  --file user-schema-v2.json \
  --schema-type json_schema \
  --mode backward

# Set compatibility mode
danube-admin-cli schemas set-compatibility user-events --mode full

# Delete a version
danube-admin-cli schemas delete user-events --version 1 --confirm

# List all schemas
danube-admin-cli schemas list --output json
```

### Topic Creation with Schema Registry

```bash
# NEW: Create topic with schema subject
danube-admin-cli topics create /default/events \
  --schema-subject user-events \
  --dispatch-strategy reliable

# OLD: Still works (backward compatible)
danube-admin-cli topics create /default/legacy \
  --schema Json \
  --schema-file legacy.json

# Describe shows schema registry info
danube-admin-cli topics describe /default/events --output json
```

---

## Migration Strategy

### Phase 1: Add Schema Registry Support (Non-Breaking)
1. ✅ Update `DanubeAdmin.proto` with optional schema_subject fields
2. ✅ Add `SchemaRegistryClient` to `client.rs`
3. ✅ Create `schema_registry.rs` module
4. ✅ Add schema command to main.rs
5. ✅ Keep existing inline schema support

**Result:** Both old and new methods work

### Phase 2: Update Topic Commands
1. ✅ Add `--schema-subject` option to topic create
2. ✅ Update describe command to show schema registry info
3. ✅ Add migration helper command to convert inline → registry

**Result:** Users can choose either method

### Phase 3: Deprecation (Future)
1. ⚠️ Mark inline schema parameters as deprecated
2. ⚠️ Add warnings for inline schema usage
3. ⚠️ Document migration path

**Result:** Encourage schema registry adoption

---

## Testing Requirements

### Unit Tests
- [ ] Schema registration with all types (JSON, Avro, Protobuf, String, Bytes)
- [ ] Schema versioning (V1 → V2 → V3)
- [ ] Compatibility checking (backward, forward, full, none)
- [ ] Schema deletion
- [ ] Topic creation with schema subjects
- [ ] Topic creation with inline schemas (backward compat)

### Integration Tests
- [ ] End-to-end: Register schema → Create topic → Describe topic
- [ ] Schema evolution: V1 → V2 with compatibility checks
- [ ] Mixed mode: Topics with inline + registry schemas coexist
- [ ] Error handling: Invalid schemas, compatibility violations
- [ ] Migration: Convert inline schema topics to registry

### CLI Tests
- [ ] All schema commands with various outputs (json, yaml, plain)
- [ ] Topic commands with --schema-subject
- [ ] Backward compatibility with existing scripts

---

## Implementation Checklist

### Proto Changes
- [ ] Add `schema_subject` to `NewTopicRequest`
- [ ] Add `schema_subject` to `PartitionedTopicRequest`
- [ ] Extend `DescribeTopicResponse` with schema registry fields
- [ ] Mark old schema fields as deprecated
- [ ] Compile proto files

### Code Changes
- [ ] Add `schema_registry_client()` to `client.rs`
- [ ] Create `schema_registry.rs` module
- [ ] Implement all schema commands (register, get, versions, check, etc.)
- [ ] Add schema command to `main.rs`
- [ ] Update topic create command
- [ ] Update topic describe command
- [ ] Add `--schema-subject` CLI argument

### Documentation
- [ ] Update README with schema registry examples
- [ ] Add migration guide (inline → registry)
- [ ] Document all new CLI commands
- [ ] Add compatibility matrix
- [ ] Create troubleshooting guide

### Testing
- [ ] Write unit tests for schema_registry.rs
- [ ] Write integration tests
- [ ] Test backward compatibility
- [ ] Test error scenarios
- [ ] Manual CLI testing

---

## Backward Compatibility Matrix

| Scenario | Old CLI | New CLI | Supported |
|----------|---------|---------|-----------|
| Create topic with inline schema | ✅ Works | ✅ Works | ✅ Yes |
| Create topic with schema subject | ❌ N/A | ✅ Works | ✅ Yes |
| Describe topic (inline schema) | ✅ Shows schema | ✅ Shows schema | ✅ Yes |
| Describe topic (registry schema) | ❌ Limited info | ✅ Full info | ✅ Yes |
| Manage schemas | ❌ Not possible | ✅ Full support | ✅ Yes |

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking existing scripts | High | Keep inline schema support, mark as deprecated |
| Proto incompatibility | High | Use optional fields, maintain backward compat |
| Migration complexity | Medium | Provide migration tools and clear docs |
| Performance impact | Low | Schema registry caching handles this |
| Learning curve | Medium | Good docs and examples |

---

## Success Criteria

- ✅ All schema registry operations available via CLI
- ✅ Topics can use schema subjects or inline schemas
- ✅ Existing scripts continue to work
- ✅ Schema evolution workflow is smooth
- ✅ Error messages are clear and actionable
- ✅ Documentation is comprehensive
- ✅ All tests pass

---

## Timeline Estimate

- **Proto Changes**: 1 hour
- **Schema Registry Module**: 4 hours
- **Topic Command Updates**: 2 hours
- **Testing**: 3 hours
- **Documentation**: 2 hours

**Total: ~12 hours** (1.5 days)

---

## Next Steps

1. **Review this plan** with the team
2. **Update `DanubeAdmin.proto`** with schema_subject fields
3. **Compile proto** and verify no breaking changes
4. **Implement `schema_registry.rs`** module
5. **Update topics.rs** for schema subject support
6. **Write tests** and validate backward compatibility
7. **Update documentation** and create examples

---

## Appendix: Schema Registry Proto Reference

```protobuf
service SchemaRegistry {
    rpc RegisterSchema(RegisterSchemaRequest) returns (RegisterSchemaResponse);
    rpc GetSchema(GetSchemaRequest) returns (GetSchemaResponse);
    rpc GetLatestSchema(GetLatestSchemaRequest) returns (GetSchemaResponse);
    rpc ListVersions(ListVersionsRequest) returns (ListVersionsResponse);
    rpc CheckCompatibility(CheckCompatibilityRequest) returns (CheckCompatibilityResponse);
    rpc DeleteSchemaVersion(DeleteSchemaVersionRequest) returns (DeleteSchemaVersionResponse);
    rpc SetCompatibilityMode(SetCompatibilityModeRequest) returns (SetCompatibilityModeResponse);
}
```

All these RPCs need corresponding CLI commands in `schema_registry.rs`.
