# MinIO Upload Issue Analysis

## Problem Statement

Data is not being written to MinIO despite proper configuration in the Docker Compose setup with danube-persistent-storage. Messages are written to local WAL but never uploaded to the S3-compatible MinIO storage.

## Root Cause Analysis

### 1. **Missing Uploader Service**

**Critical Issue**: The `Uploader` background service is not being started during broker initialization.

**Evidence**:
- `danube-persistent-storage/src/uploader.rs` exists and implements background upload functionality
- `danube_broker.yml` contains uploader configuration:
  ```yaml
  uploader:
    interval_seconds: 5
    max_batch_bytes: 16777216
  ```
- `danube-broker/src/main.rs` creates `CloudStore` and `EtcdMetadata` but never starts an `Uploader` instance

### 2. **WalStorage Architecture Gap**

**Issue**: `WalStorage` handles reads with cloud fallback but doesn't trigger uploads.

**Code Analysis**:
```rust
// In WalStorage::new() and WalStorage::from_wal()
pub struct WalStorage {
    wal: Wal,
    cloud: Option<CloudStore>,     // Used for reads only
    etcd: Option<EtcdMetadata>,    // Used for metadata queries only
    topic_path: Option<String>,
}
```

**Per-topic WalStorage creation** (in `broker_service.rs`):
```rust
let wal_storage = WalStorage::from_wal(self.base_wal.clone()).with_cloud(
    self.cloud_store.clone(),
    self.etcd_meta.clone(),
    topic_name.to_string(),
)
```

This only configures cloud reads, not uploads.

### 3. **Configuration Flow Analysis**

**Working Components**:
- ✅ WAL configuration and initialization
- ✅ CloudStore creation with S3/MinIO backend
- ✅ EtcdMetadata wrapper creation
- ✅ Environment variable mapping (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

**Missing Components**:
- ❌ Uploader service initialization
- ❌ Background upload task spawning
- ❌ UploaderConfig parsing from `wal_cloud.uploader` section

### 4. **Expected Data Flow vs Actual**

**Expected Flow**:
1. Messages written to local WAL ✅
2. Uploader reads WAL periodically ❌
3. Uploader uploads batches to MinIO ❌
4. EtcdMetadata tracks uploaded objects ❌
5. WalStorage combines cloud + WAL for reads ✅ (configured but unused)

**Actual Flow**:
1. Messages written to local WAL ✅
2. No background uploads occur ❌
3. MinIO remains empty ❌

## Technical Details

### Configuration Mapping
The broker correctly maps S3 configuration from YAML to `BackendConfig::Cloud`:

```rust
// In main.rs lines 136-176
CloudConfig::S3 { ... } => {
    let mut options: HashMap<String, String> = HashMap::new();
    if let Some(v) = endpoint {
        options.insert("endpoint".into(), v.clone());  // MinIO endpoint
    }
    if let Some(v) = access_key {
        options.insert("access_key".into(), v.clone());  // From env vars
    }
    // ... other S3 options
}
```

### Missing Initialization
The broker should initialize and start an Uploader:

```rust
// Missing code that should exist in main.rs
let uploader_config = UploaderConfig {
    interval_seconds: wal_cfg.uploader.interval_seconds,
    max_batch_bytes: wal_cfg.uploader.max_batch_bytes,
};

let uploader = Uploader::new(
    wal.clone(),
    cloud_store.clone(),
    etcd_meta.clone(),
    uploader_config,
);

// Start background upload task
tokio::spawn(async move {
    uploader.run().await;
});
```

## Solution Requirements

1. **Parse uploader configuration** from `wal_cloud.uploader` section in YAML
2. **Create Uploader instance** with WAL, CloudStore, and EtcdMetadata
3. **Spawn background task** to run uploader loop
4. **Ensure proper error handling** and logging for upload failures
5. **Add graceful shutdown** for uploader service

## Files Requiring Changes

- `danube-broker/src/main.rs` - Add uploader initialization
- `danube-broker/src/service_configuration.rs` - Ensure UploaderNode is properly mapped
- Potentially `danube-broker/src/danube_service.rs` - Add uploader to service lifecycle

## Impact

Without this fix, the cloud-ready architecture is incomplete:
- Messages remain only in local WAL
- No persistence across broker restarts
- No cloud analytics capabilities
- MinIO/S3 integration is non-functional

## Priority

**High** - This breaks the core value proposition of cloud-native storage and the Docker Compose demo.
