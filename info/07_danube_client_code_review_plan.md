# Danube Client Code Review & Improvement Plan

**Document Version:** 1.0  
**Created:** February 2026  
**Status:** Proposed  
**Related:** 06_danube_client_review_plan_merged.md (superseded)

## Executive Summary

This document outlines improvements to `danube-client` based on a comprehensive code review. The client serves as a reference implementation for other language bindings, requiring excellent structure, documentation, idiomatic Rust, and performance.

**Key Findings:**
- 5 high-priority correctness/safety issues (TLS panic, auth deadlock, stop signals, error handling)
- 5 medium-priority API clarity & ergonomics improvements
- 2 low-priority idiomatic refinements

**Timeline:** 2-3 weeks across 4 phases  
**Impact:** Enhanced safety, better API ergonomics, clearer documentation for language port authors

---

## Issues by Priority

### High Priority (Correctness & Safety)

#### Issue 1: TLS Panic with API Key Authentication
**Severity:** Critical  
**Files:** `client.rs:147-151`, `connection_manager.rs:90-109`

**Problem:**  
`DanubeClientBuilder::with_api_key()` sets `use_tls = true` but doesn't configure `tls_config`. When `new_rpc_connection()` executes, it calls `.expect("TLS config must be present")` and panics at runtime.

```rust
// Current problematic code in client.rs
pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
    self.api_key = Some(api_key.into());
    self.connection_options.use_tls = true; // ⚠️ No tls_config set!
    self
}
```

**Solution:**  
Set a default TLS config when API key is provided:

```rust
pub fn with_api_key(mut self, api_key: impl Into<String>) -> Result<Self> {
    self.api_key = Some(api_key.into());
    
    // Set default TLS config for API key auth (uses system certs)
    if self.connection_options.tls_config.is_none() {
        self.connection_options.tls_config = Some(ClientTlsConfig::new());
    }
    self.connection_options.use_tls = true;
    Ok(self)
}
```

**Migration:** Change return type to `Result<Self>` (breaking change, but necessary for safety).

---

#### Issue 2: Auth Token Refresh Holds Locks Across `.await`
**Severity:** High  
**Files:** `auth_service.rs:59-76`

**Problem:**  
`get_valid_token()` holds both `token` and `token_expiry` mutex guards while calling `authenticate_client().await`, which can cause contention and potential deadlocks in high-concurrency scenarios.

```rust
// Current problematic pattern
pub async fn get_valid_token(&self, addr: &Uri, api_key: &str) -> Result<String> {
    let now = Instant::now();
    let mut token_guard = self.token.lock().await;  // 🔒 Lock 1
    let mut expiry_guard = self.token_expiry.lock().await;  // 🔒 Lock 2
    
    // ... check expiry ...
    
    // ⚠️ Holding locks across async call
    let new_token = self.authenticate_client(addr, api_key).await?;
    // ...
}
```

**Solution:**  
Drop locks before calling `authenticate_client`:

```rust
pub async fn get_valid_token(&self, addr: &Uri, api_key: &str) -> Result<String> {
    let now = Instant::now();
    
    // Check expiry and return cached token (fast path)
    {
        let token_guard = self.token.lock().await;
        let expiry_guard = self.token_expiry.lock().await;
        
        if let Some(expiry) = *expiry_guard {
            if now < expiry {
                if let Some(token) = &*token_guard {
                    return Ok(token.clone());
                }
            }
        }
    } // 🔓 Locks dropped here
    
    // Slow path: refresh token (locks not held)
    let new_token = self.authenticate_client(addr, api_key).await?;
    
    // Update cache atomically
    {
        let mut token_guard = self.token.lock().await;
        let mut expiry_guard = self.token_expiry.lock().await;
        *token_guard = Some(new_token.clone());
        *expiry_guard = Some(now + Duration::from_secs(TOKEN_EXPIRY_SECS));
    }
    
    Ok(new_token)
}
```

**Testing:** Add concurrent token request test to verify no deadlocks.

---

#### Issue 3: Health Check Doesn't Respect Stop Signal
**Severity:** High  
**Files:** `health_check.rs:40-75`, `topic_consumer.rs:94-96`, `consumer.rs:203-217`

**Problem:**  
1. Health check loop never checks `stop_signal` before sleeping/continuing
2. `TopicConsumer::stop()` sets stop signal but health check ignores it
3. Consumer receive loop doesn't propagate broker CLOSE status

```rust
// Current health_check loop - never checks stop_signal!
tokio::spawn(async move {
    loop {
        // ... health check ...
        sleep(Duration::from_secs(HEALTH_CHECK_INTERVAL_SECS)).await;
        // ⚠️ No stop_signal check!
    }
});
```

**Solution:**

```rust
// In health_check.rs - check stop signal before sleeping
pub(crate) async fn start_health_check(
    &self,
    addr: &Uri,
    client_type: ClientType,
    client_id: u64,
    stop_signal: Arc<AtomicBool>,
) -> Result<()> {
    let grpc_cnx = self.cnx_manager.get_connection(addr, addr).await?;
    let stop_signal_clone = Arc::clone(&stop_signal);
    let request_id = Arc::clone(&self.request_id);
    let api_key = self.cnx_manager.connection_options.api_key.clone();
    let addr = addr.clone();
    let auth_service = self.auth_service.clone();
    
    tokio::spawn(async move {
        loop {
            // ✅ Check stop signal before health check
            if stop_signal_clone.load(Ordering::SeqCst) {
                break;
            }
            
            if let Err(e) = HealthCheckService::health_check(
                Arc::clone(&request_id),
                Arc::clone(&grpc_cnx),
                client_type,
                client_id,
                Arc::clone(&stop_signal_clone),
                api_key.as_deref(),
                &addr,
                auth_service.clone(),
            ).await {
                warn!("Health check error: {:?}", e);
                break;
            }
            
            // ✅ Interruptible sleep
            for _ in 0..HEALTH_CHECK_INTERVAL_SECS {
                if stop_signal_clone.load(Ordering::SeqCst) {
                    return;
                }
                sleep(Duration::from_secs(1)).await;
            }
        }
    });
    Ok(())
}
```

**Testing:** Test consumer close triggers health check stop within 1 second.

---

#### Issue 4: `LookupType::Failed` Panics with `todo!()`
**Severity:** High  
**Files:** `lookup_service.rs:125-133`

**Problem:**  
When broker returns `LookupType::Failed`, the client panics instead of returning an error.

```rust
// Current code
Some(LookupType::Failed) => {
    todo!()  // ⚠️ Production panic!
}
```

**Solution:**

```rust
Some(LookupType::Failed) => {
    Err(DanubeError::Unrecoverable(
        "Topic lookup failed: topic may not exist or cluster is unavailable".to_string()
    ))
}
```

---

#### Issue 5: `LookupResult` Fields Are Inaccessible
**Severity:** Medium-High  
**Files:** `lookup_service.rs:17-21`, `client.rs:72-86`

**Problem:**  
`DanubeClient::lookup_topic` returns `LookupResult`, but all fields are private. Users can't read the result.

**Solution:**

```rust
#[derive(Debug, Default)]
pub struct LookupResult {
    pub response_type: i32,
    pub addr: Uri,
}

impl LookupResult {
    /// Returns true if the broker redirected to another broker
    pub fn is_redirect(&self) -> bool {
        self.response_type == 0 // LookupType::Redirect
    }
    
    /// Returns the broker address to connect to
    pub fn broker_addr(&self) -> &Uri {
        &self.addr
    }
}
```

---

### Medium Priority (API Clarity & Ergonomics)

#### Issue 6: Outdated Documentation Examples
**Severity:** Medium  
**Files:** `producer.rs:275-289`, `producer.rs:416-423`

**Problem:**  
Examples reference non-existent APIs (`client.producer()`, old schema methods).

**Solution:**  
Update all doc examples:

```rust
/// # Example
/// ```no_run
/// use danube_client::DanubeClient;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = DanubeClient::builder()
///     .service_url("http://localhost:6650")
///     .build()
///     .await?;
///
/// let mut producer = client.new_producer()
///     .with_topic("user-events")
///     .with_name("my-producer")
///     .with_schema_subject("user-events-value")
///     .build()?;
///
/// producer.create().await?;
/// producer.send(b"hello".to_vec(), None).await?;
/// # Ok(())
/// # }
/// ```
```

---

#### Issue 7: `with_partitions(0)` Causes Divide-by-Zero
**Severity:** Medium  
**Files:** `producer.rs:78-80`, `message_router.rs:16-17`

**Problem:**  
If user sets `with_partitions(0)`, `MessageRouter::round_robin` divides by zero.

**Solution:**

```rust
// In ProducerBuilder::build()
pub fn build(self) -> Result<Producer> {
    let topic_name = self.topic.ok_or_else(|| {
        DanubeError::Unrecoverable("topic is required to build a Producer".into())
    })?;
    let producer_name = self.producer_name.ok_or_else(|| {
        DanubeError::Unrecoverable("producer name is required to build a Producer".into())
    })?;
    
    // ✅ Validate partitions
    if let Some(partitions) = self.num_partitions {
        if partitions == 0 {
            return Err(DanubeError::Unrecoverable(
                "partitions must be > 0 or omitted for non-partitioned topic".into()
            ));
        }
    }

    Ok(Producer::new(/* ... */))
}
```

---

#### Issue 8: Consumer Subscribe Uses Unnecessary Tasks
**Severity:** Low-Medium  
**Files:** `consumer.rs:100-128`

**Problem:**  
`subscribe()` spawns tasks for parallel construction, but `TopicConsumer::new()` + `subscribe()` is synchronous construction + async RPC. Can use `try_join_all`.

**Solution:**

```rust
pub async fn subscribe(&mut self) -> Result<()> {
    let partitions = self.client.lookup_service
        .topic_partitions(&self.client.uri, &self.topic_name)
        .await?;

    // Build futures without spawning tasks
    let futures: Vec<_> = partitions.into_iter().map(|topic_partition| {
        let mut topic_consumer = TopicConsumer::new(
            self.client.clone(),
            topic_partition.clone(),
            self.consumer_name.clone(),
            self.subscription.clone(),
            Some(self.subscription_type.clone()),
            self.consumer_options.clone(),
        );
        
        async move {
            topic_consumer.subscribe().await?;
            Ok::<_, DanubeError>((topic_partition, topic_consumer))
        }
    }).collect();

    // Execute in parallel with clearer error propagation
    let results = futures::future::try_join_all(futures).await?;
    
    for (partition_name, consumer) in results {
        self.consumers.insert(partition_name, Arc::new(Mutex::new(consumer)));
    }

    if self.consumers.is_empty() {
        return Err(DanubeError::Unrecoverable("No partitions found".to_string()));
    }

    Ok(())
}
```

---

#### Issue 9: Schema Registration Metadata Is Hardcoded
**Severity:** Low-Medium  
**Files:** `schema_registry_client.rs:199-221`, `schema_registry_client.rs:253-289`

**Problem:**  
`register_schema_internal` always uses empty `description`, `"danube-client"` for `created_by`, and empty `tags`. As a reference implementation, it should demonstrate these fields.

**Solution:**

```rust
pub struct SchemaRegistrationBuilder<'a> {
    client: &'a SchemaRegistryClient,
    subject: String,
    schema_type: Option<SchemaType>,
    schema_data: Option<Vec<u8>>,
    description: Option<String>,
    created_by: Option<String>,
    tags: Vec<String>,
}

impl<'a> SchemaRegistrationBuilder<'a> {
    pub fn with_type(mut self, schema_type: SchemaType) -> Self {
        self.schema_type = Some(schema_type);
        self
    }

    pub fn with_schema_data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.schema_data = Some(data.into());
        self
    }
    
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
    
    pub fn with_created_by(mut self, created_by: impl Into<String>) -> Self {
        self.created_by = Some(created_by.into());
        self
    }
    
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    pub async fn execute(self) -> Result<u64> {
        let schema_type = self.schema_type
            .ok_or_else(|| DanubeError::SchemaError("Schema type is required".into()))?;
        let schema_data = self.schema_data
            .ok_or_else(|| DanubeError::SchemaError("Schema data is required".into()))?;

        let response = self.client.register_schema_internal(
            self.subject,
            schema_type.as_str().to_string(),
            schema_data,
            self.description.unwrap_or_default(),
            self.created_by.unwrap_or_else(|| "danube-client".to_string()),
            self.tags,
        ).await?;

        Ok(response.schema_id)
    }
}
```

---

#### Issue 10: Inefficient `schema_definition_as_string`
**Severity:** Low  
**Files:** `schema_types.rs:148-153`

**Problem:**  
Clones bytes before UTF-8 conversion.

**Solution:**

```rust
pub fn schema_definition_as_string(&self) -> Option<String> {
    std::str::from_utf8(&self.schema_definition)
        .ok()
        .map(|s| s.to_string())
}
```

---

### Low Priority (Idiomatic Rust)

#### Issue 11: Retry Options Use Sentinel `0` for Defaults
**Severity:** Low  
**Files:** `consumer.rs:425-434`, `producer.rs:446-455`, `retry_manager.rs:26-43`

**Problem:**  
Using `0` to mean "use default" is non-idiomatic. Better to use `Option` or implement `Default` trait.

**Solution:**

```rust
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConsumerOptions {
    pub max_retries: Option<usize>,
    pub base_backoff_ms: Option<u64>,
    pub max_backoff_ms: Option<u64>,
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            max_retries: Some(5),
            base_backoff_ms: Some(200),
            max_backoff_ms: Some(5_000),
        }
    }
}

// In RetryManager::new
impl RetryManager {
    pub fn new(max_retries: Option<usize>, base_backoff_ms: Option<u64>, max_backoff_ms: Option<u64>) -> Self {
        Self {
            max_retries: max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            base_backoff_ms: base_backoff_ms.unwrap_or(DEFAULT_BASE_BACKOFF_MS),
            max_backoff_ms: max_backoff_ms.unwrap_or(DEFAULT_MAX_BACKOFF_MS),
        }
    }
}
```

---

#### Issue 12: Token Expiry Is Hardcoded
**Severity:** Low  
**Files:** `auth_service.rs:12-13`, `auth_service.rs:47-76`

**Problem:**  
JWT token expiry is assumed to be 3600 seconds. Should read from JWT `exp` claim or use server-provided TTL.

**Solution:**

```rust
use jsonwebtoken::{decode, DecodingKey, Validation, Algorithm};

#[derive(Debug, serde::Deserialize)]
struct Claims {
    exp: usize,
}

fn extract_token_expiry(token: &str) -> Option<Instant> {
    // Decode without validation (we just need exp claim)
    let validation = Validation::new(Algorithm::HS256);
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(&[]), // Dummy key, we don't verify signature
        &validation
    ).ok()?;
    
    let exp_duration = Duration::from_secs(token_data.claims.exp as u64);
    Some(Instant::now() + exp_duration)
}

pub async fn authenticate_client(&self, addr: &Uri, api_key: &str) -> Result<String> {
    // ... existing code ...
    let token = response.into_inner().token;

    // Extract expiry from JWT, fallback to default
    let expiry = extract_token_expiry(&token)
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(TOKEN_EXPIRY_SECS));

    // Store token and expiry
    {
        let mut token_guard = self.token.lock().await;
        let mut expiry_guard = self.token_expiry.lock().await;
        *token_guard = Some(token.clone());
        *expiry_guard = Some(expiry);
    }

    Ok(token)
}
```

---

## Implementation Phases

### Phase 1: Critical Safety Fixes (Week 1)
**Duration:** 3-4 days  
**Focus:** Issues 1-5 (high priority)

**Tasks:**
1. Fix TLS panic in `with_api_key` (Issue 1)
2. Fix auth token lock contention (Issue 2)
3. Implement health check stop signal (Issue 3)
4. Fix lookup failure panic (Issue 4)
5. Make LookupResult fields accessible (Issue 5)

**Testing:**
- Add unit test for API key without TLS config
- Add concurrent token refresh test
- Add consumer close + health check stop test
- Add lookup failure test

**Success Criteria:**
- No panics in common error paths
- No deadlocks under concurrent load
- Clean shutdown propagates to all background tasks

---

### Phase 2: Documentation & Examples (Week 1-2)
**Duration:** 2-3 days  
**Focus:** Issue 6, plus general doc improvements

**Tasks:**
1. Update all doc examples to use current APIs
2. Add crate-level quickstart example
3. Add "porting guide" doc for other language implementers
4. Add architecture diagram showing client components

**Example Crate-Level Docs:**

```rust
//! # Danube Client
//!
//! Official Rust client for the Danube messaging system. This crate serves as
//! the reference implementation for client libraries in other languages.
//!
//! ## Quick Start
//!
//! ```no_run
//! use danube_client::DanubeClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to Danube broker
//!     let client = DanubeClient::builder()
//!         .service_url("http://localhost:6650")
//!         .build()
//!         .await?;
//!
//!     // Create a producer
//!     let mut producer = client.new_producer()
//!         .with_topic("my-topic")
//!         .with_name("my-producer")
//!         .build()?;
//!     producer.create().await?;
//!
//!     // Send a message
//!     let seq_id = producer.send(b"Hello, Danube!".to_vec(), None).await?;
//!     println!("Message sent with sequence ID: {}", seq_id);
//!
//!     // Create a consumer
//!     let mut consumer = client.new_consumer()
//!         .with_topic("my-topic")
//!         .with_consumer_name("my-consumer")
//!         .with_subscription("my-subscription")
//!         .build()?;
//!     consumer.subscribe().await?;
//!
//!     // Receive messages
//!     let mut rx = consumer.receive().await?;
//!     while let Some(message) = rx.recv().await {
//!         println!("Received: {:?}", String::from_utf8_lossy(&message.payload));
//!         consumer.ack(&message).await?;
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Features
//!
//! - **Producers**: Send messages to topics with automatic partitioning
//! - **Consumers**: Subscribe to topics with exclusive/shared/failover modes
//! - **Schema Registry**: Centralized schema management with versioning
//! - **Retry Logic**: Automatic reconnection and exponential backoff
//! - **TLS/mTLS**: Secure connections with certificate-based authentication
//! - **JWT Auth**: API key authentication with token caching
//!
//! ## Architecture
//!
//! The client is organized into:
//! - `DanubeClient`: Main entry point and connection manager
//! - `Producer`/`Consumer`: High-level messaging APIs
//! - `SchemaRegistryClient`: Schema management
//! - Internal services: auth, health check, lookup, retry logic
//!
//! ## For Language Port Authors
//!
//! When porting this client to another language:
//! 1. Match the builder patterns for ergonomic API
//! 2. Implement connection pooling per broker address
//! 3. Use state machines for producer/consumer lifecycle (Disconnected → Ready)
//! 4. Centralize retry logic with exponential backoff + jitter
//! 5. Handle stop signals for graceful shutdown
//! 6. Cache JWT tokens with expiry tracking
//!
//! See proto files in `danube-core/proto/` for gRPC service definitions.
```

---

### Phase 3: API Ergonomics (Week 2)
**Duration:** 3-4 days  
**Focus:** Issues 7-10

**Tasks:**
1. Add partition validation (Issue 7)
2. Optimize consumer subscribe (Issue 8)
3. Add schema registration metadata fields (Issue 9)
4. Optimize `schema_definition_as_string` (Issue 10)

**Testing:**
- Test `with_partitions(0)` returns error
- Benchmark parallel subscribe performance
- Test schema registration with full metadata

---

### Phase 4: Idiomatic Refinements (Week 3)
**Duration:** 2-3 days  
**Focus:** Issues 11-12, general polish

**Tasks:**
1. Use `Option` for retry config (Issue 11)
2. Extract JWT expiry (Issue 12)
3. Add `#[must_use]` to builder methods
4. Run `cargo clippy -- -W clippy::pedantic`
5. Add examples/ directory with runnable samples

**Deliverables:**
- `examples/producer.rs`
- `examples/consumer.rs`
- `examples/schema_registry.rs`
- `examples/tls_auth.rs`

---

## Testing Strategy

### Unit Tests (New)
- `test_api_key_without_tls` → should error or set default TLS
- `test_concurrent_token_refresh` → no deadlocks
- `test_health_check_stop` → stops within 1 second
- `test_lookup_failed` → returns error, not panic
- `test_partitions_zero` → returns validation error

### Integration Tests (Enhance)
- Multi-broker cluster with redirects
- Consumer close → health check stops
- Token expiry → auto-refresh
- Partitioned topic producer + consumer

### Performance Tests (Optional)
- Throughput: 100k msgs/sec per producer
- Latency: p99 < 10ms under load
- Connection pooling: reuse across multiple producers

---

## Migration Notes

### Breaking Changes
1. `DanubeClientBuilder::with_api_key()` now returns `Result<Self>`
2. `ConsumerOptions`/`ProducerOptions` fields change from `usize/u64` to `Option<usize>/Option<u64>`

### Deprecations
None (all changes are fixes or additions)

### Recommended Upgrade Path
1. Update to new version
2. Handle `with_api_key()` result: `.with_api_key(key)?`
3. Update retry options to use `Some(value)` or rely on defaults
4. Test shutdown behavior (health checks now stop properly)

---

## Success Criteria

### Correctness
- ✅ No panics in error paths
- ✅ No deadlocks under concurrent load
- ✅ Clean shutdown propagates to all tasks

### Documentation
- ✅ All examples use current APIs
- ✅ Crate-level quickstart compiles and runs
- ✅ Porting guide helps other language authors

### Ergonomics
- ✅ Builder errors caught at compile-time or build()
- ✅ Schema registration exposes all metadata fields
- ✅ Partition validation prevents runtime errors

### Performance
- ✅ No unnecessary allocations (e.g., schema_definition_as_string)
- ✅ Auth token refresh doesn't block other operations
- ✅ Parallel subscribe uses try_join_all (cleaner, similar perf)

---

## Future Considerations (Out of Scope)

1. **Async traits for Producer/Consumer** → Requires async_trait or Rust 1.75+
2. **Message batching** → Reduce per-message overhead
3. **Compression** → Payload compression for large messages
4. **Metrics/Observability** → Export Prometheus metrics
5. **Schema validation** → Client-side Avro/Protobuf validation

---

## References

- Proto files: `danube-core/proto/DanubeApi.proto`, `SchemaRegistry.proto`
- Server implementation: `danube-broker/src/broker_server/`
- Previous plan: `info/06_danube_client_review_plan_merged.md` (superseded)

---

**Next Steps:**
1. Review this plan with team
2. Create GitHub issues for each phase
3. Begin Phase 1 (critical fixes) immediately
4. Schedule Phase 2-4 based on release timeline
