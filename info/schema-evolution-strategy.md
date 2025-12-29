# Danube Schema Evolution Strategy

## Executive Summary

This document outlines the comprehensive strategy for evolving Danube's schema system from its current basic implementation to a production-grade schema management system. The current implementation has critical limitations that prevent schema evolution, lack validation, and provide only four primitive types. This proposal defines the path forward to build a robust schema system competitive with Apache Pulsar and Kafka.

## Must-Have Features (Non-Negotiable)

The following five features are **mandatory** for the new schema implementation:

### 1. Schema Registry ✓ MUST HAVE
A dedicated schema management system that:
- Stores schemas independently of topics
- Provides unique schema IDs for efficient message encoding
- Enables schema reuse across multiple topics
- Offers centralized governance and discovery
- Provides REST/gRPC API for schema operations

### 2. Schema Versioning ✓ MUST HAVE
Complete version control for schemas:
- Automatic version incrementing on schema changes
- Full version history retention
- Ability to query any historical schema version
- Version metadata (timestamp, author, description)
- Support for pinning topics to specific versions or using latest
- Compatibility checking between versions

### 3. Schema Validation ✓ MUST HAVE
Multi-layer validation system:
- **Producer-side validation:** Validate before serialization
- **Broker-side validation:** Optional enforcement at write time
- **Consumer-side validation:** Validate after deserialization
- Configurable per-topic validation policies
- Descriptive error messages on validation failures
- Performance-optimized validation paths

### 4. Extended Schema Type System ✓ MUST HAVE
Support for industry-standard schema formats:
- **Avro:** Full support with built-in evolution semantics
- **Protocol Buffers:** Binary efficiency with field numbering
- **JSON Schema:** Enhanced with proper parsing and validation
- **Custom Schemas:** Plugin system for user-defined validators
- Type safety across language boundaries
- Efficient binary serialization formats

### 5. Schema Metadata and Discovery ✓ MUST HAVE
Rich metadata management:
- Schema ownership and creator tracking
- Human-readable descriptions and documentation
- Tagging system for categorization
- Deprecation markers and migration guides
- Full-text search across schemas
- Schema dependency tracking
- Auto-generated documentation
- Sample data and usage examples

---

## Current State Analysis

### Existing Implementation

**Schema Definition:**
```rust
pub enum SchemaType {
    Bytes,
    String,
    Int64,
    Json(String), // JSON schema as raw string
}

pub struct Schema {
    pub name: String,
    pub schema_data: Option<Vec<u8>>,
    pub type_schema: SchemaType,
}
```

**Storage Location:**
- Metadata path: `/topics/{topic_name}/schema`
- Stored in ETCD alongside topic metadata
- One schema per topic, immutable after creation
- No versioning mechanism

**Protocol Definition:**
```protobuf
message Schema {
    enum TypeSchema {
        Bytes = 0;
        String = 1;
        Int64 = 2;
        JSON = 3;
    }
    string name = 1;
    bytes schema_data = 3;
    TypeSchema type_schema = 4;
}
```

**Current Workflow:**
1. Producer creates with schema during ProducerRequest
2. Schema stored in topic metadata if topic is created
3. Consumers can query schema via Discovery service GetSchema RPC
4. No validation at broker level
5. Validation entirely client responsibility

### Critical Limitations

**No Schema Evolution:**
- Impossible to change schema after topic creation
- Application changes require new topics or unmanaged compatibility
- No way to track schema changes over time
- Breaking changes go undetected

**No Compatibility Checking:**
- No enforcement of backward/forward compatibility
- Consumers can break when schema changes
- No safety net for schema modifications
- Manual compatibility management required

**No Schema Registry Pattern:**
- Schemas tightly coupled to topics
- Cannot reuse schemas across topics
- No central management point
- Difficult to discover and govern schemas

**Limited Type System:**
- Only four primitive types
- No Avro (industry standard for streaming)
- No Protobuf (efficient binary format)
- No custom validator support
- JSON schema is just an unvalidated string

**No Schema Metadata:**
- Missing creator/owner information
- No description or documentation
- No deprecation workflow
- No tagging or categorization
- Cannot track schema lineage

**No Validation at Write Time:**
- Broker accepts any payload regardless of schema
- Invalid data can poison topics
- Errors discovered late at consumption
- No early feedback to producers
- Debugging becomes difficult

**Inefficient Schema Transmission:**
- Full schema not included in messages
- No schema ID mechanism for efficiency
- Cannot negotiate schema versions
- Poor network efficiency for schema-heavy systems

**JSON Schema as Opaque String:**
- No parsing of JSON schema definitions
- Cannot validate schema is well-formed
- No extraction of schema properties
- Broker treats as raw data
- Cannot index or search schema content

---

## Industry Best Practices

### Apache Pulsar Approach

**Schema Registry:**
- Built-in schema registry in broker
- Automatic schema versioning
- Full version history
- Schema evolution tracking

**Schema Types:**
- Avro with full compatibility checking
- JSON with JSON Schema validation
- Protobuf support
- Auto-schema from reflection
- Key-Value schemas (separate schemas for keys/values)

**Compatibility Modes:**
- ALWAYS_COMPATIBLE: no checks
- BACKWARD: new schema reads old data
- FORWARD: old schema reads new data
- FULL: both directions
- BACKWARD_TRANSITIVE: backward across all versions
- FORWARD_TRANSITIVE: forward across all versions
- FULL_TRANSITIVE: full transitive compatibility

**Validation:**
- Producer validates before sending
- Optional broker validation (configurable)
- Consumer validates on receive
- Type safety across languages

**Schema Storage:**
- Separate storage in BookKeeper
- Versioned with monotonic IDs
- Schema negotiation at connection time
- Messages carry schema version in metadata

### Kafka + Confluent Schema Registry

**Separate Service Architecture:**
- Schema Registry as standalone REST service
- Not embedded in broker
- Central repository for all schemas
- Topics reference schemas by ID

**Schema ID in Messages:**
- Messages contain 4-5 byte schema ID
- Extremely efficient overhead
- Consumer looks up schema from registry
- Enables evolution without retransmission

**Subject Naming Strategies:**
- TopicNameStrategy: one schema per topic
- RecordNameStrategy: schema per record type
- TopicRecordNameStrategy: combination approach
- Supports multiple schemas per topic

**Compatibility Enforcement:**
- Strict checking before registration
- Validates against existing versions
- Rejects incompatible changes
- Configurable per subject
- Protects consumers automatically

**REST API:**
- Register new schemas
- Check compatibility
- List versions
- Get schema by ID/version
- Delete schemas

### Common Patterns

**Versioning is Fundamental:**
Both systems treat schema evolution as first-class concern with tracked versions.

**Multi-Layer Validation:**
Validation at producer, broker (optional), and consumer provides defense-in-depth.

**Schema as Metadata:**
Schemas have IDs, versions, timestamps, compatibility rules governing lifecycle.

**Efficiency Through Indirection:**
Schema IDs/versions reduce message size significantly versus full schema transmission.

**Polyglot Support:**
Rich formats like Avro/Protobuf enable cross-language type-safe serialization.

---

## Proposed Architecture

### Phase 1: Schema Registry Foundation (MUST HAVE - Priority 1)

**New Component: SchemaRegistry**

Location: `danube-broker/src/schema_registry.rs` (or separate `danube-schema-registry` crate)

**Core Structure:**
```rust
pub struct SchemaRegistry {
    // Storage backend (ETCD or dedicated store)
    storage: Arc<MetadataStorage>,
    // In-memory cache for fast lookups
    cache: Arc<RwLock<SchemaCache>>,
    // Schema ID generator
    id_generator: AtomicU64,
}

pub struct SchemaMetadata {
    pub id: u64,
    pub name: String,
    pub versions: Vec<SchemaVersion>,
    pub compatibility_mode: CompatibilityMode,
    pub created_at: u64,
    pub created_by: String,
}

pub struct SchemaVersion {
    pub version: u32,
    pub schema_def: SchemaDefinition,
    pub created_at: u64,
    pub created_by: String,
    pub description: String,
    pub fingerprint: String, // Hash of schema for deduplication
}

pub enum SchemaDefinition {
    Avro(AvroSchema),
    Protobuf(ProtobufSchema),
    JsonSchema(JsonSchemaValidator),
    Custom(CustomSchema),
}

pub enum CompatibilityMode {
    None,
    Backward,
    Forward,
    Full,
    BackwardTransitive,
    ForwardTransitive,
    FullTransitive,
}
```

**Storage Hierarchy:**
```
/schemas/{schema_name}/metadata
/schemas/{schema_name}/versions/{version_id}
/schemas/{schema_name}/compatibility_mode
/topics/{topic_name}/schema_ref -> {schema_name}:{version_id}
```

**API Operations:**
- `register_schema(name, definition, compatibility_mode) -> Result<(SchemaId, Version)>`
- `get_schema(schema_id, version) -> Result<SchemaDefinition>`
- `get_latest_schema(schema_id) -> Result<SchemaVersion>`
- `list_versions(schema_id) -> Result<Vec<SchemaVersion>>`
- `check_compatibility(schema_id, new_definition) -> Result<CompatibilityCheck>`
- `delete_schema_version(schema_id, version) -> Result<()>`

**gRPC Service Definition:**
```protobuf
service SchemaRegistryService {
    rpc RegisterSchema(RegisterSchemaRequest) returns (RegisterSchemaResponse);
    rpc GetSchema(GetSchemaRequest) returns (GetSchemaResponse);
    rpc GetLatestSchema(GetLatestSchemaRequest) returns (GetSchemaResponse);
    rpc ListVersions(ListVersionsRequest) returns (ListVersionsResponse);
    rpc CheckCompatibility(CheckCompatibilityRequest) returns (CheckCompatibilityResponse);
    rpc DeleteSchemaVersion(DeleteSchemaVersionRequest) returns (DeleteSchemaVersionResponse);
    rpc SetCompatibilityMode(SetCompatibilityModeRequest) returns (SetCompatibilityModeResponse);
}

message RegisterSchemaRequest {
    string schema_name = 1;
    string schema_type = 2; // "avro", "protobuf", "json", "custom"
    bytes schema_definition = 3;
    string description = 4;
    string created_by = 5;
}

message RegisterSchemaResponse {
    uint64 schema_id = 1;
    uint32 version = 2;
    bool is_new_schema = 3; // false if this exact schema exists
}

message GetSchemaRequest {
    uint64 schema_id = 1;
    optional uint32 version = 2; // if omitted, get latest
}

message GetSchemaResponse {
    uint64 schema_id = 1;
    uint32 version = 2;
    string schema_type = 3;
    bytes schema_definition = 4;
    string description = 5;
    uint64 created_at = 6;
}

message CheckCompatibilityRequest {
    uint64 schema_id = 1;
    bytes new_schema_definition = 2;
    string compatibility_mode = 3;
}

message CheckCompatibilityResponse {
    bool is_compatible = 1;
    repeated string incompatibility_reasons = 2;
}
```

### Phase 2: Schema Versioning (MUST HAVE - Priority 1)

**Version Management:**

Every schema change creates a new version:
- Version numbers start at 1, increment sequentially
- Versions are immutable once created
- All versions retained indefinitely (or with configurable TTL)
- Deduplication: registering identical schema returns existing version

**Version Metadata:**
```rust
pub struct SchemaVersion {
    pub version: u32,
    pub schema_def: SchemaDefinition,
    pub created_at: u64,
    pub created_by: String,
    pub description: String,
    pub fingerprint: String, // SHA256 of canonical schema representation
    pub tags: Vec<String>,
    pub is_deprecated: bool,
    pub deprecation_message: Option<String>,
}
```

**Topic Schema References:**

Topics reference schemas by name and version:
```rust
pub struct TopicSchemaReference {
    pub schema_name: String,
    pub version_mode: VersionMode,
}

pub enum VersionMode {
    Latest,                    // Always use latest version
    Pinned(u32),               // Use specific version
    MinVersion(u32),           // Use version >= specified
}
```

**Producer/Consumer Negotiation:**

When producer/consumer connects:
1. Client specifies supported schema versions
2. Broker validates against topic's schema reference
3. Connection established with agreed schema version
4. Messages include schema version in metadata

**Message Format Update:**
```protobuf
message StreamMessage {
    uint64 request_id = 1;
    MsgID msg_id = 2;
    bytes payload = 3;
    uint64 publish_time = 4;
    string producer_name = 5;
    string subscription_name = 6;
    map<string, string> attributes = 7;
    
    // NEW: Schema version information
    uint64 schema_id = 8;
    uint32 schema_version = 9;
}
```

### Phase 3: Schema Validation (MUST HAVE - Priority 2)

**Multi-Layer Validation:**

**1. Producer-Side Validation (Client SDK):**
```rust
impl TopicProducer {
    pub async fn send_validated<T: Serialize>(
        &mut self,
        data: &T,
        schema_validator: &SchemaValidator,
    ) -> Result<u64> {
        // Serialize to bytes
        let bytes = schema_validator.serialize(data)?;
        
        // Validate against schema
        schema_validator.validate(&bytes)?;
        
        // Send
        self.send(bytes, None).await
    }
}
```

**2. Broker-Side Validation (Optional):**
```rust
pub struct Topic {
    // ... existing fields
    pub validation_policy: ValidationPolicy,
    pub schema_validator: Option<Arc<SchemaValidator>>,
}

pub enum ValidationPolicy {
    None,                      // No broker validation
    Warn,                      // Validate and log warnings
    Enforce,                   // Reject invalid messages
}

impl Topic {
    pub async fn publish_message_async(&self, msg: StreamMessage) -> Result<()> {
        // Existing checks...
        
        // NEW: Optional schema validation
        if let ValidationPolicy::Enforce = self.validation_policy {
            if let Some(validator) = &self.schema_validator {
                validator.validate(&msg.payload)
                    .map_err(|e| anyhow!("Schema validation failed: {}", e))?;
            }
        }
        
        // Continue with storage...
    }
}
```

**3. Consumer-Side Validation (Client SDK):**
```rust
impl Consumer {
    pub async fn receive_validated<T: DeserializeOwned>(
        &mut self,
        schema_validator: &SchemaValidator,
    ) -> Result<T> {
        let message = self.receive_raw().await?;
        
        // Validate
        schema_validator.validate(&message.payload)?;
        
        // Deserialize
        let data: T = schema_validator.deserialize(&message.payload)?;
        
        Ok(data)
    }
}
```

**Schema Validator Trait:**
```rust
pub trait SchemaValidator: Send + Sync {
    fn validate(&self, data: &[u8]) -> Result<()>;
    fn serialize<T: Serialize>(&self, data: &T) -> Result<Vec<u8>>;
    fn deserialize<T: DeserializeOwned>(&self, data: &[u8]) -> Result<T>;
}

pub struct AvroValidator {
    schema: apache_avro::Schema,
}

pub struct JsonSchemaValidator {
    validator: jsonschema::Validator,
}

pub struct ProtobufValidator {
    descriptor: prost_reflect::DynamicMessage,
}
```

**Validation Policies:**

Per-topic configuration:
```rust
pub struct TopicPolicies {
    // ... existing fields
    pub schema_validation_policy: ValidationPolicy,
    pub reject_unversioned_messages: bool,
    pub allow_schema_auto_update: bool,
}
```

### Phase 4: Extended Schema Type System (MUST HAVE - Priority 2)

**Enhanced Schema Type Enum:**
```rust
pub enum SchemaType {
    // Existing
    Bytes,
    String,
    Int64,
    
    // NEW: Enhanced JSON with parsed schema
    JsonSchema(JsonSchemaDefinition),
    
    // NEW: Avro support (PRIORITY)
    Avro(AvroSchemaDefinition),
    
    // NEW: Protobuf support
    Protobuf(ProtobufSchemaDefinition),
    
    // NEW: Custom validators
    Custom(CustomSchemaDefinition),
}

pub struct JsonSchemaDefinition {
    pub raw_schema: String,
    pub parsed_schema: serde_json::Value,
    pub validator: jsonschema::Validator,
}

pub struct AvroSchemaDefinition {
    pub raw_schema: String,
    pub parsed_schema: apache_avro::Schema,
    pub fingerprint: String,
}

pub struct ProtobufSchemaDefinition {
    pub raw_proto: String,
    pub descriptor: prost_types::FileDescriptorSet,
    pub message_name: String,
}

pub struct CustomSchemaDefinition {
    pub schema_type_name: String,
    pub schema_definition: Vec<u8>,
    pub validator_wasm: Option<Vec<u8>>, // WASM-based custom validator
}
```

**Avro Integration (Priority 1):**

Add Cargo dependency: `apache-avro = "0.16"`

```rust
pub struct AvroSchemaManager {
    schemas: HashMap<u64, apache_avro::Schema>,
}

impl AvroSchemaManager {
    pub fn parse_schema(raw: &str) -> Result<apache_avro::Schema> {
        apache_avro::Schema::parse_str(raw)
            .map_err(|e| anyhow!("Invalid Avro schema: {}", e))
    }
    
    pub fn check_compatibility(
        old: &apache_avro::Schema,
        new: &apache_avro::Schema,
        mode: CompatibilityMode,
    ) -> Result<()> {
        match mode {
            CompatibilityMode::Backward => {
                // Check if new schema can read data written with old schema
                Self::is_backward_compatible(old, new)?;
            }
            CompatibilityMode::Forward => {
                // Check if old schema can read data written with new schema
                Self::is_forward_compatible(old, new)?;
            }
            CompatibilityMode::Full => {
                Self::is_backward_compatible(old, new)?;
                Self::is_forward_compatible(old, new)?;
            }
            CompatibilityMode::None => {}
            // Transitive modes check against all previous versions
            _ => unimplemented!("Transitive modes require full version history"),
        }
        Ok(())
    }
    
    fn is_backward_compatible(
        writer: &apache_avro::Schema,
        reader: &apache_avro::Schema,
    ) -> Result<()> {
        // Avro's resolution rules:
        // - Fields in writer but not in reader: ignored (OK)
        // - Fields in reader but not in writer: must have defaults (check)
        // - Field type changes: limited compatibility rules
        
        // Use Avro's built-in compatibility checking
        match apache_avro::schema::ResolvedSchema::try_from((reader, writer)) {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("Backward incompatible: {}", e)),
        }
    }
}
```

**Protobuf Integration:**

Add Cargo dependency: `prost = "0.12"`, `prost-reflect = "0.12"`

```rust
pub struct ProtobufSchemaManager {
    descriptors: HashMap<u64, prost_reflect::DescriptorPool>,
}

impl ProtobufSchemaManager {
    pub fn parse_proto(proto_source: &str) -> Result<ProtobufSchemaDefinition> {
        // Compile .proto to FileDescriptorSet
        let descriptor = Self::compile_proto(proto_source)?;
        
        Ok(ProtobufSchemaDefinition {
            raw_proto: proto_source.to_string(),
            descriptor,
            message_name: Self::extract_message_name(&descriptor)?,
        })
    }
    
    pub fn check_compatibility(
        old: &ProtobufSchemaDefinition,
        new: &ProtobufSchemaDefinition,
        mode: CompatibilityMode,
    ) -> Result<()> {
        // Protobuf compatibility rules:
        // - Field numbers must not change
        // - Field types can only change in compatible ways
        // - Adding optional fields is safe
        // - Removing required fields breaks backward compatibility
        
        match mode {
            CompatibilityMode::Backward => {
                Self::check_backward_compatible(&old.descriptor, &new.descriptor)?;
            }
            CompatibilityMode::Forward => {
                Self::check_forward_compatible(&old.descriptor, &new.descriptor)?;
            }
            _ => unimplemented!("Full compatibility checking for Protobuf"),
        }
        Ok(())
    }
}
```

**Enhanced JSON Schema Support:**

Upgrade JSON schema from opaque string to validated structure:

```rust
pub struct JsonSchemaManager {
    validators: HashMap<u64, jsonschema::Validator>,
}

impl JsonSchemaManager {
    pub fn parse_json_schema(raw: &str) -> Result<JsonSchemaDefinition> {
        // Parse the schema JSON
        let parsed: serde_json::Value = serde_json::from_str(raw)
            .map_err(|e| anyhow!("Invalid JSON schema: {}", e))?;
        
        // Compile validator
        let validator = jsonschema::validator_for(&parsed)
            .map_err(|e| anyhow!("Failed to compile JSON schema: {}", e))?;
        
        Ok(JsonSchemaDefinition {
            raw_schema: raw.to_string(),
            parsed_schema: parsed,
            validator,
        })
    }
    
    pub fn validate(&self, schema_id: u64, data: &[u8]) -> Result<()> {
        let validator = self.validators.get(&schema_id)
            .ok_or_else(|| anyhow!("Schema not found: {}", schema_id))?;
        
        // Parse data as JSON
        let json_data: serde_json::Value = serde_json::from_slice(data)?;
        
        // Validate
        if validator.is_valid(&json_data) {
            Ok(())
        } else {
            let errors: Vec<_> = validator.iter_errors(&json_data).collect();
            Err(anyhow!("JSON validation failed: {:?}", errors))
        }
    }
}
```

### Phase 5: Schema Metadata and Discovery (MUST HAVE - Priority 3)

**Rich Metadata Structure:**
```rust
pub struct SchemaMetadata {
    // Identity
    pub id: u64,
    pub name: String,
    
    // Versioning
    pub versions: Vec<SchemaVersion>,
    pub latest_version: u32,
    pub compatibility_mode: CompatibilityMode,
    
    // Ownership
    pub created_by: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub owner_team: Option<String>,
    
    // Documentation
    pub description: String,
    pub documentation_url: Option<String>,
    pub sample_data: Option<Vec<u8>>,
    pub migration_guide: Option<String>,
    
    // Categorization
    pub tags: Vec<String>,
    pub namespace: String,
    pub domain: Option<String>, // e.g., "customer", "order", "payment"
    
    // Lifecycle
    pub status: SchemaStatus,
    pub deprecation_info: Option<DeprecationInfo>,
    
    // Usage tracking
    pub topics_using: Vec<String>,
    pub references: Vec<SchemaReference>, // Schemas this depends on
    pub referenced_by: Vec<u64>,          // Schemas that depend on this
}

pub enum SchemaStatus {
    Active,
    Deprecated,
    Archived,
}

pub struct DeprecationInfo {
    pub deprecated_at: u64,
    pub deprecated_by: String,
    pub reason: String,
    pub replacement_schema_id: Option<u64>,
    pub migration_deadline: Option<u64>,
}

pub struct SchemaReference {
    pub schema_id: u64,
    pub version: u32,
    pub field_path: String, // Where in this schema the reference is used
}
```

**Schema Discovery Service:**
```rust
pub struct SchemaDiscoveryService {
    registry: Arc<SchemaRegistry>,
    search_index: Arc<RwLock<SchemaSearchIndex>>,
}

impl SchemaDiscoveryService {
    // Search schemas by various criteria
    pub async fn search(&self, query: SchemaQuery) -> Result<Vec<SchemaMetadata>> {
        let mut results = Vec::new();
        
        // Text search in name and description
        if let Some(text) = &query.text {
            results.extend(self.search_index.read().await.text_search(text)?);
        }
        
        // Filter by tags
        if !query.tags.is_empty() {
            results.retain(|s| s.tags.iter().any(|t| query.tags.contains(t)));
        }
        
        // Filter by domain
        if let Some(domain) = &query.domain {
            results.retain(|s| s.domain.as_ref() == Some(domain));
        }
        
        // Filter by status
        if let Some(status) = &query.status {
            results.retain(|s| &s.status == status);
        }
        
        Ok(results)
    }
    
    // List schemas by namespace
    pub async fn list_by_namespace(&self, namespace: &str) -> Result<Vec<SchemaMetadata>> {
        self.registry.list_schemas_in_namespace(namespace).await
    }
    
    // Find topics using a schema
    pub async fn find_topics_using_schema(&self, schema_id: u64) -> Result<Vec<String>> {
        let metadata = self.registry.get_schema_metadata(schema_id).await?;
        Ok(metadata.topics_using)
    }
    
    // Get schema dependency graph
    pub async fn get_dependency_graph(&self, schema_id: u64) -> Result<SchemaDependencyGraph> {
        let mut graph = SchemaDependencyGraph::new();
        self.build_dependency_graph(schema_id, &mut graph).await?;
        Ok(graph)
    }
}

pub struct SchemaQuery {
    pub text: Option<String>,
    pub tags: Vec<String>,
    pub domain: Option<String>,
    pub status: Option<SchemaStatus>,
    pub namespace: Option<String>,
}

pub struct SchemaDependencyGraph {
    pub nodes: HashMap<u64, SchemaNode>,
    pub edges: Vec<DependencyEdge>,
}

pub struct SchemaNode {
    pub schema_id: u64,
    pub name: String,
    pub version: u32,
}

pub struct DependencyEdge {
    pub from_schema: u64,
    pub to_schema: u64,
    pub dependency_type: DependencyType,
}

pub enum DependencyType {
    Reference,    // Schema A references schema B
    Evolution,    // Schema A evolved from schema B
}
```

**Search Index:**
```rust
pub struct SchemaSearchIndex {
    // Inverted index for text search
    text_index: HashMap<String, HashSet<u64>>,
    
    // Tag index
    tag_index: HashMap<String, HashSet<u64>>,
    
    // Domain index
    domain_index: HashMap<String, HashSet<u64>>,
    
    // Namespace index
    namespace_index: HashMap<String, HashSet<u64>>,
}

impl SchemaSearchIndex {
    pub fn index_schema(&mut self, metadata: &SchemaMetadata) {
        let id = metadata.id;
        
        // Index name and description
        for word in Self::tokenize(&metadata.name) {
            self.text_index.entry(word).or_default().insert(id);
        }
        for word in Self::tokenize(&metadata.description) {
            self.text_index.entry(word).or_default().insert(id);
        }
        
        // Index tags
        for tag in &metadata.tags {
            self.tag_index.entry(tag.clone()).or_default().insert(id);
        }
        
        // Index domain
        if let Some(domain) = &metadata.domain {
            self.domain_index.entry(domain.clone()).or_default().insert(id);
        }
        
        // Index namespace
        self.namespace_index.entry(metadata.namespace.clone()).or_default().insert(id);
    }
    
    fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    }
}
```

**Schema Documentation Generator:**
```rust
pub struct SchemaDocGenerator;

impl SchemaDocGenerator {
    pub async fn generate_html_doc(metadata: &SchemaMetadata) -> Result<String> {
        let mut html = String::new();
        
        html.push_str(&format!("<h1>Schema: {}</h1>", metadata.name));
        html.push_str(&format!("<p><strong>ID:</strong> {}</p>", metadata.id));
        html.push_str(&format!("<p><strong>Version:</strong> {}</p>", metadata.latest_version));
        html.push_str(&format!("<p>{}</p>", metadata.description));
        
        // Tags
        if !metadata.tags.is_empty() {
            html.push_str("<p><strong>Tags:</strong> ");
            html.push_str(&metadata.tags.join(", "));
            html.push_str("</p>");
        }
        
        // Topics using this schema
        if !metadata.topics_using.is_empty() {
            html.push_str("<h2>Topics Using This Schema</h2><ul>");
            for topic in &metadata.topics_using {
                html.push_str(&format!("<li>{}</li>", topic));
            }
            html.push_str("</ul>");
        }
        
        // Version history
        html.push_str("<h2>Version History</h2>");
        html.push_str("<table><tr><th>Version</th><th>Created</th><th>By</th><th>Description</th></tr>");
        for version in &metadata.versions {
            html.push_str(&format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                version.version,
                version.created_at,
                version.created_by,
                version.description
            ));
        }
        html.push_str("</table>");
        
        // Schema definition
        html.push_str("<h2>Schema Definition</h2>");
        match &metadata.versions.last().unwrap().schema_def {
            SchemaDefinition::Avro(avro) => {
                html.push_str("<pre><code class=\"language-json\">");
                html.push_str(&avro.raw_schema);
                html.push_str("</code></pre>");
            }
            SchemaDefinition::JsonSchema(json) => {
                html.push_str("<pre><code class=\"language-json\">");
                html.push_str(&json.raw_schema);
                html.push_str("</code></pre>");
            }
            _ => html.push_str("<p>Schema definition not available</p>"),
        }
        
        Ok(html)
    }
    
    pub async fn generate_markdown_doc(metadata: &SchemaMetadata) -> Result<String> {
        // Similar to HTML but in Markdown format
        // Useful for embedding in documentation systems like GitBook, Docusaurus
        todo!()
    }
}
```

**Admin CLI Extensions:**
```bash
# Search for schemas
danube-admin-cli schemas search --text "customer" --tag "pii"

# List all schemas in namespace
danube-admin-cli schemas list --namespace default

# Describe schema with full details
danube-admin-cli schemas describe customer-profile --output json

# Find topics using a schema
danube-admin-cli schemas topics customer-profile

# Get dependency graph
danube-admin-cli schemas dependencies customer-profile

# Tag management
danube-admin-cli schemas tag customer-profile --add "gdpr" --add "pii"

# Deprecate a schema
danube-admin-cli schemas deprecate customer-profile-v1 \
  --reason "Use customer-profile-v2 instead" \
  --replacement customer-profile-v2 \
  --deadline "2025-12-31"
```

---

## Implementation Roadmap

### Sprint 1-2: Schema Registry Foundation (4 weeks)

**Goals:**
- Design and implement core SchemaRegistry component
- Define storage schema in ETCD
- Implement basic CRUD operations for schemas
- Add schema ID generation and caching

**Deliverables:**
- `SchemaRegistry` struct with storage backend
- In-memory cache for fast schema lookups
- gRPC service definition and implementation
- Unit tests for all operations
- Integration tests with ETCD

**Tasks:**
1. Define data structures (SchemaMetadata, SchemaVersion, etc.)
2. Implement storage layer
3. Build in-memory cache with LRU eviction
4. Create gRPC service
5. Add admin CLI commands for schema management
6. Write comprehensive tests

### Sprint 3-4: Schema Versioning (3 weeks)

**Goals:**
- Implement automatic version incrementing
- Add version history tracking
- Update protobuf messages to include schema version
- Implement version pinning for topics

**Deliverables:**
- Full version tracking in metadata store
- Updated StreamMessage with schema_id and schema_version fields
- Topic schema reference configuration
- Version deduplication (same schema = same version)
- Updated client SDKs with version support

**Tasks:**
1. Extend SchemaVersion with full metadata
2. Implement fingerprinting for deduplication
3. Update protobuf definitions
4. Modify producer/consumer to handle versions
5. Add version negotiation at connection time
6. Update client SDK APIs for schema versioning

### Sprint 5-7: Avro Integration (4 weeks)

**Goals:**
- Add full Avro schema support
- Implement Avro serialization/deserialization
- Build Avro compatibility checker
- Add Avro validation in producer/broker/consumer

**Deliverables:**
- Avro schema parsing and validation
- AvroSchemaDefinition with apache-avro integration
- Backward/forward compatibility checking
- AvroValidator implementation
- Client SDK support for Avro
- Examples and documentation

**Tasks:**
1. Add apache-avro crate dependency
2. Implement AvroSchemaManager
3. Build compatibility checking logic
4. Create AvroValidator
5. Update client SDKs (Rust, Go)
6. Write Avro producer/consumer examples
7. Add integration tests
8. Write migration guide from JSON to Avro

### Sprint 8-9: Schema Validation (3 weeks)

**Goals:**
- Implement multi-layer validation
- Add configurable validation policies
- Build efficient validation paths
- Add descriptive error messages

**Deliverables:**
- Producer-side validation in client SDK
- Optional broker-side validation
- Consumer-side validation
- ValidationPolicy configuration
- Performance benchmarks
- Error message improvements

**Tasks:**
1. Implement SchemaValidator trait
2. Add validation to TopicProducer
3. Add optional validation to Topic
4. Implement validation policies in TopicPolicies
5. Add validation metrics
6. Optimize validation for hot path
7. Write validation error messages
8. Create validation examples

### Sprint 10-11: Enhanced JSON Schema & Protobuf (4 weeks)

**Goals:**
- Upgrade JSON schema from string to validated structure
- Add Protocol Buffers support
- Implement compatibility checking for both
- Add validation support

**Deliverables:**
- JsonSchemaDefinition with parsed schema and validator
- ProtobufSchemaDefinition with descriptor support
- Compatibility checkers for both formats
- Integration tests
- Documentation and examples

**Tasks:**
1. Refactor JSON schema handling
2. Add jsonschema crate for proper validation
3. Integrate prost and prost-reflect for Protobuf
4. Implement ProtobufSchemaManager
5. Build compatibility checkers
6. Update admin CLI for both formats
7. Add examples for each format
8. Performance benchmarks

### Sprint 12-14: Schema Metadata & Discovery (5 weeks)

**Goals:**
- Add rich metadata to schemas
- Build schema discovery service
- Implement search functionality
- Create documentation generator

**Deliverables:**
- Extended SchemaMetadata with all fields
- SchemaDiscoveryService with search
- Schema search index
- Dependency graph builder
- HTML/Markdown documentation generator
- Admin CLI search commands

**Tasks:**
1. Extend SchemaMetadata structure
2. Add metadata storage and indexing
3. Implement SchemaSearchIndex
4. Build SchemaDiscoveryService
5. Create dependency graph algorithms
6. Implement doc generators
7. Add search to admin CLI
8. Build web UI for schema browsing (optional)
9. Write comprehensive documentation

### Sprint 15: Integration & Polish (2 weeks)

**Goals:**
- End-to-end testing
- Performance optimization
- Documentation completion
- Release preparation

**Deliverables:**
- Full integration test suite
- Performance benchmarks and optimizations
- Complete user documentation
- Release notes documenting breaking changes
- Blog post announcing new features

**Tasks:**
1. End-to-end testing scenarios
2. Performance profiling and optimization
3. Documentation review and completion
4. Write comprehensive release notes
5. Write blog post and announcement
6. Prepare demo for users
7. Plan community feedback session

---

## Breaking Changes & Clean Implementation

### No Backward Compatibility Required

Since Danube is not yet a production system with live users, this schema evolution will be implemented as a **clean break** from the existing schema system. There is no requirement to maintain backward compatibility with the current limited implementation.

**Rationale:**
- **Pre-production status:** Danube is still in development without production deployments requiring stability guarantees
- **Fundamental redesign:** The new schema system is architecturally different and maintaining compatibility would compromise the design
- **Technical debt avoidance:** Building compatibility layers would add complexity and maintenance burden without value
- **Clean foundation:** Starting fresh enables optimal implementation without legacy constraints

### Implementation Approach

**Complete Replacement:**
- The existing schema system will be completely replaced, not extended
- Old protobuf definitions for `Schema` will be redesigned from scratch
- No migration tools or compatibility layers needed
- Clean codebase without deprecated code paths

**Breaking Changes:**
1. **Protobuf Schema Message:** Complete redesign to include versioning, IDs, and metadata
2. **Topic Schema Storage:** New metadata structure in ETCD (old paths will be unused)
3. **Client APIs:** Producer/Consumer APIs will change to support schema registry
4. **Message Format:** StreamMessage will include schema_id and schema_version fields
5. **Admin CLI:** Schema-related commands will be completely new

**Development Workflow:**
1. Implement new schema system in parallel branches
2. Update all components (broker, client SDKs, admin tools) together
3. Test end-to-end with new implementation
4. Deploy as breaking change with clear release notes
5. Update all examples and documentation to reflect new APIs

**User Communication:**
- Clear release notes highlighting breaking changes
- Updated documentation and examples
- Comprehensive tutorials for the new schema system
- Announcement that old schema APIs are completely replaced

---

## Performance Considerations

### Schema Cache

**In-Memory LRU Cache:**
- Cache frequently accessed schemas
- Configurable cache size (default 1000 schemas)
- Async cache updates
- Cache invalidation on schema updates

**Benchmark Targets:**
- Schema lookup: < 1μs (cache hit), < 100μs (cache miss)
- Schema registration: < 10ms (including ETCD write)
- Compatibility check: < 5ms for typical schemas

### Validation Performance

**Fast Path Optimizations:**
- Pre-compile validators at schema registration
- Reuse validators across messages
- Parallel validation for batch operations
- Skip validation when policy is None

**Benchmark Targets:**
- Avro validation: < 10μs per message (< 1KB)
- JSON Schema validation: < 50μs per message (< 1KB)
- Protobuf validation: < 5μs per message (< 1KB)

### Schema Version in Messages

**Minimal Overhead:**
- Schema ID: 8 bytes (u64)
- Schema version: 4 bytes (u32)
- Total: 12 bytes per message
- Negligible compared to payload size

**Optimization:**
- Consider delta encoding for schema versions in reliable topics
- Batch schema lookups for consumer sessions

---

## Testing Strategy

### Unit Tests

**Schema Registry:**
- CRUD operations
- Version management
- Compatibility checking
- Cache behavior
- Concurrent access

**Validation:**
- Each validator type (Avro, JSON, Protobuf)
- Error messages
- Performance characteristics

**Discovery:**
- Search functionality
- Index updates
- Dependency graphs

### Integration Tests

**End-to-End Workflows:**
- Register schema → Create topic → Produce → Consume
- Schema evolution → Compatibility check → Update topic
- Multiple versions → Consumer compatibility
- Schema validation → Reject invalid messages

**Multi-Broker:**
- Schema registry sync across brokers
- Cache consistency
- Failover scenarios

### Performance Tests

**Throughput:**
- Message throughput with validation enabled vs disabled
- Schema lookup performance under load
- Registry operations under concurrent access

**Latency:**
- P50, P99, P999 latencies for all operations
- Impact of validation on producer latency

### Schema Compatibility Tests

**Schema Evolution Testing:**
- Schema version N can read messages written with version N-1 (backward compatibility)
- Schema version N-1 can read messages written with version N (forward compatibility)
- Compatibility mode enforcement (reject incompatible schema registrations)
- Transitive compatibility across multiple versions

---

## Security Considerations

### Access Control

**Schema Operations:**
- Who can register new schemas?
- Who can update schemas?
- Who can deprecate schemas?
- Role-based access control (RBAC) for schema registry

**Topic Schema Enforcement:**
- Prevent unauthorized schema changes
- Audit log for schema modifications
- Schema ownership and permissions

### Schema Content Security

**Validation:**
- Prevent malicious schema definitions
- Size limits on schema definitions
- Reject schemas with known vulnerabilities

**Data Privacy:**
- Mark schemas containing PII
- Implement field-level encryption hints
- GDPR compliance metadata

---

## Monitoring & Observability

### Metrics

**Schema Registry:**
- `schema_registry_total_schemas`: Total number of registered schemas
- `schema_registry_total_versions`: Total schema versions across all schemas
- `schema_registry_cache_hit_rate`: Cache hit rate for schema lookups
- `schema_registry_operation_duration_ms`: Duration of registry operations

**Validation:**
- `schema_validation_total{result="success|failure"}`: Total validations
- `schema_validation_duration_ms`: Validation latency
- `schema_validation_errors_total{type="..."}`: Validation errors by type

**Compatibility:**
- `schema_compatibility_checks_total{result="compatible|incompatible"}`: Compatibility checks
- `schema_compatibility_check_duration_ms`: Compatibility check latency

### Logging

**Structured Logging:**
- Schema registration events
- Compatibility check results
- Validation failures
- Schema deprecation events

**Audit Trail:**
- Who registered/updated schemas
- When schema changes occurred
- What changed between versions

---

## Documentation Requirements

### User Documentation

**Getting Started:**
- Schema concepts and terminology
- How to register your first schema
- Producer/consumer examples with schemas
- Quick start tutorials

**Schema Types:**
- Avro guide with examples
- JSON Schema guide with examples
- Protobuf guide with examples
- Custom schema guide

**Schema Evolution:**
- Compatibility modes explained
- How to evolve schemas safely
- Common pitfalls and solutions
- Best practices

**Admin Guide:**
- Schema registry configuration
- Monitoring and troubleshooting
- Backup and recovery
- Performance tuning

### Developer Documentation

**Architecture:**
- System design overview
- Component interactions
- Storage schema
- API reference

**Contributing:**
- How to add new schema types
- How to add new validators
- Testing guidelines
- Code style guide

---

## Success Metrics

### Adoption Metrics
- Number of schemas registered
- Number of topics using schema registry
- Percentage of messages validated
- Schema version diversity (how many versions in use)

### Quality Metrics
- Schema validation error rate
- Compatibility check success rate
- Schema-related bug reports
- User satisfaction scores

### Performance Metrics
- Schema lookup latency (P99 < 100μs)
- Validation overhead (< 5% throughput impact)
- Registry operation latency (P99 < 20ms)
- Cache hit rate (> 95%)

---

## Conclusion

This comprehensive schema evolution strategy transforms Danube from a basic messaging system into a production-grade streaming platform with enterprise-ready schema management. The five must-have features—Schema Registry, Schema Versioning, Schema Validation, Extended Type System, and Metadata/Discovery—are non-negotiable foundations that will enable safe schema evolution, prevent data quality issues, and provide users with the tools they need to build reliable, scalable streaming applications.

The phased implementation approach delivers value incrementally over approximately 15 sprints (30 weeks) as a clean break from the existing limited schema system. This roadmap balances ambition with pragmatism, learning from industry leaders like Apache Pulsar and Kafka while adapting their best practices to Danube's unique architecture and goals. Since Danube is pre-production, we avoid the complexity of backward compatibility and can build the optimal schema system from the ground up.

By completing this evolution, Danube will offer:
- Safe schema evolution with automatic compatibility checking
- Rich type system with Avro, Protobuf, and enhanced JSON support
- Multi-layer validation preventing bad data from entering topics
- Comprehensive metadata enabling discovery, documentation, and governance
- Performance characteristics suitable for production workloads

This positions Danube as a compelling alternative to established streaming platforms, with modern schema management capabilities that developers expect and enterprises require.
