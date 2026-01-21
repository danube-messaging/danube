# LoadManager Phase 1: Real Resource Monitoring

**Priority:** CRITICAL  
**Timeline:** 2-3 weeks  
**Status:** Not Started

---

## 🎯 Goals

Replace mock resource data with real system metrics that work across:
- Native platforms (Linux, Windows, macOS)
- Containerized environments (Docker)
- Kubernetes clusters (cgroup-aware)

---

## 📋 Implementation Steps

### Step 1: Add Dependencies
**Status:** ☐ Not Started

**Task:** Update `danube-broker/Cargo.toml` with required crates

```toml
[dependencies]
# Existing dependencies...

# Cross-platform system information
sysinfo = "0.30"

# Linux-specific cgroups (optional, for better container support)
procfs = { version = "0.16", optional = true }

# Kubernetes API (optional, for pod-level metrics)
k8s-openapi = { version = "0.20", optional = true }
```

**Verification:**
- [ ] Run `cargo check -p danube-broker`
- [ ] Verify no dependency conflicts

---

### Step 2: Create Resource Monitor Abstraction
**Status:** ☐ Not Started

**Task:** Create `danube-broker/src/danube_service/load_manager/resource_monitor.rs`

```rust
use anyhow::Result;
use async_trait::async_trait;

/// Cross-platform system resource monitoring
#[async_trait]
pub trait ResourceMonitor: Send + Sync {
    /// Get CPU usage as percentage (0.0-100.0)
    async fn get_cpu_usage(&self) -> Result<f64>;
    
    /// Get memory usage as percentage (0.0-100.0)
    async fn get_memory_usage(&self) -> Result<f64>;
    
    /// Get disk I/O statistics
    async fn get_disk_io(&self) -> Result<DiskIOStats>;
    
    /// Get network I/O statistics
    async fn get_network_io(&self) -> Result<NetworkIOStats>;
}

#[derive(Debug, Clone)]
pub struct DiskIOStats {
    pub read_bytes_per_sec: u64,
    pub write_bytes_per_sec: u64,
}

impl DiskIOStats {
    pub fn total_bytes_per_sec(&self) -> u64 {
        self.read_bytes_per_sec + self.write_bytes_per_sec
    }
}

#[derive(Debug, Clone)]
pub struct NetworkIOStats {
    pub rx_bytes_per_sec: u64,
    pub tx_bytes_per_sec: u64,
}

impl NetworkIOStats {
    pub fn total_bytes_per_sec(&self) -> u64 {
        self.rx_bytes_per_sec + self.tx_bytes_per_sec
    }
}

/// Factory function to create appropriate monitor for the environment
pub fn create_resource_monitor() -> Box<dyn ResourceMonitor> {
    if is_running_in_k8s() {
        Box::new(KubernetesResourceMonitor::new())
    } else {
        Box::new(NativeResourceMonitor::new())
    }
}

fn is_running_in_k8s() -> bool {
    std::env::var("KUBERNETES_SERVICE_HOST").is_ok() ||
    std::path::Path::new("/var/run/secrets/kubernetes.io").exists()
}
```

**Verification:**
- [ ] File compiles without errors
- [ ] Trait is properly marked with `async_trait`
- [ ] Factory function can be called

---

### Step 3: Implement Native Resource Monitor
**Status:** ☐ Not Started

**Task:** Implement `NativeResourceMonitor` using `sysinfo` crate

**Add to `resource_monitor.rs`:**

```rust
use sysinfo::{System, SystemExt, CpuExt, NetworkExt, DiskExt};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::{Duration, Instant};

pub struct NativeResourceMonitor {
    system: Arc<Mutex<System>>,
    prev_snapshot: Arc<Mutex<ResourceSnapshot>>,
}

#[derive(Debug, Clone)]
struct ResourceSnapshot {
    timestamp: Instant,
    network_rx_bytes: u64,
    network_tx_bytes: u64,
    disk_read_bytes: u64,
    disk_write_bytes: u64,
}

impl NativeResourceMonitor {
    pub fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();
        
        Self {
            system: Arc::new(Mutex::new(system)),
            prev_snapshot: Arc::new(Mutex::new(ResourceSnapshot {
                timestamp: Instant::now(),
                network_rx_bytes: 0,
                network_tx_bytes: 0,
                disk_read_bytes: 0,
                disk_write_bytes: 0,
            })),
        }
    }
}

#[async_trait]
impl ResourceMonitor for NativeResourceMonitor {
    async fn get_cpu_usage(&self) -> Result<f64> {
        let mut system = self.system.lock().await;
        system.refresh_cpu();
        
        // Wait a bit for accurate CPU measurement
        tokio::time::sleep(Duration::from_millis(200)).await;
        system.refresh_cpu();
        
        // Average across all CPUs
        let total_usage: f32 = system.cpus().iter().map(|cpu| cpu.cpu_usage()).sum();
        let cpu_count = system.cpus().len() as f32;
        
        Ok((total_usage / cpu_count) as f64)
    }
    
    async fn get_memory_usage(&self) -> Result<f64> {
        let mut system = self.system.lock().await;
        system.refresh_memory();
        
        let used = system.used_memory();
        let total = system.total_memory();
        
        if total == 0 {
            return Ok(0.0);
        }
        
        Ok((used as f64 / total as f64) * 100.0)
    }
    
    async fn get_disk_io(&self) -> Result<DiskIOStats> {
        let mut system = self.system.lock().await;
        system.refresh_disks_list();
        system.refresh_disks();
        
        let mut total_read = 0u64;
        let mut total_write = 0u64;
        
        // Aggregate across all disks
        for disk in system.disks() {
            // Note: sysinfo doesn't provide per-second rates directly
            // We'll need to calculate deltas
            total_read += disk.total_space() - disk.available_space();
        }
        
        let mut prev = self.prev_snapshot.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(prev.timestamp).as_secs_f64();
        
        let read_rate = if elapsed > 0.0 {
            ((total_read - prev.disk_read_bytes) as f64 / elapsed) as u64
        } else {
            0
        };
        
        prev.disk_read_bytes = total_read;
        prev.timestamp = now;
        
        Ok(DiskIOStats {
            read_bytes_per_sec: read_rate,
            write_bytes_per_sec: 0, // TODO: Need process-level tracking
        })
    }
    
    async fn get_network_io(&self) -> Result<NetworkIOStats> {
        let mut system = self.system.lock().await;
        system.refresh_networks_list();
        system.refresh_networks();
        
        let mut total_rx = 0u64;
        let mut total_tx = 0u64;
        
        for (_, network) in system.networks() {
            total_rx += network.received();
            total_tx += network.transmitted();
        }
        
        let mut prev = self.prev_snapshot.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(prev.timestamp).as_secs_f64();
        
        let rx_rate = if elapsed > 0.0 {
            ((total_rx - prev.network_rx_bytes) as f64 / elapsed) as u64
        } else {
            0
        };
        
        let tx_rate = if elapsed > 0.0 {
            ((total_tx - prev.network_tx_bytes) as f64 / elapsed) as u64
        } else {
            0
        };
        
        prev.network_rx_bytes = total_rx;
        prev.network_tx_bytes = total_tx;
        prev.timestamp = now;
        
        Ok(NetworkIOStats {
            rx_bytes_per_sec: rx_rate,
            tx_bytes_per_sec: tx_rate,
        })
    }
}
```

**Verification:**
- [ ] Compiles without errors
- [ ] Test on Linux: `cargo test -p danube-broker test_native_monitor`
- [ ] Test on macOS (if available)
- [ ] Test on Windows (if available)

---

### Step 4: Implement Kubernetes Resource Monitor
**Status:** ☐ Not Started

**Task:** Implement cgroup-aware monitoring for Kubernetes

**Add to `resource_monitor.rs`:**

```rust
#[cfg(target_os = "linux")]
pub struct KubernetesResourceMonitor {
    cgroup_path: String,
    prev_snapshot: Arc<Mutex<CgroupSnapshot>>,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone)]
struct CgroupSnapshot {
    timestamp: Instant,
    cpu_usage_ns: u64,
    network_rx_bytes: u64,
    network_tx_bytes: u64,
}

#[cfg(target_os = "linux")]
impl KubernetesResourceMonitor {
    pub fn new() -> Self {
        Self {
            cgroup_path: detect_cgroup_path(),
            prev_snapshot: Arc::new(Mutex::new(CgroupSnapshot {
                timestamp: Instant::now(),
                cpu_usage_ns: 0,
                network_rx_bytes: 0,
                network_tx_bytes: 0,
            })),
        }
    }
    
    fn read_cgroup_file(&self, filename: &str) -> Result<String> {
        let path = format!("{}/{}", self.cgroup_path, filename);
        std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("Failed to read {}: {}", path, e))
    }
}

#[cfg(target_os = "linux")]
#[async_trait]
impl ResourceMonitor for KubernetesResourceMonitor {
    async fn get_cpu_usage(&self) -> Result<f64> {
        // Read cgroup CPU statistics
        let cpu_stat = self.read_cgroup_file("cpu.stat")?;
        let usage_usec = parse_cpu_stat(&cpu_stat)?;
        
        // Read CPU quota and period
        let quota = self.read_cgroup_file("cpu.max")?;
        let (quota_us, period_us) = parse_cpu_quota(&quota)?;
        
        let mut prev = self.prev_snapshot.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(prev.timestamp).as_secs_f64();
        
        if elapsed == 0.0 {
            return Ok(0.0);
        }
        
        let usage_delta = usage_usec - prev.cpu_usage_ns;
        let usage_rate = (usage_delta as f64 / elapsed) / 1_000_000.0; // Convert to seconds
        
        prev.cpu_usage_ns = usage_usec;
        prev.timestamp = now;
        
        // Calculate percentage based on quota
        let cpu_limit = quota_us as f64 / period_us as f64;
        Ok((usage_rate / cpu_limit) * 100.0)
    }
    
    async fn get_memory_usage(&self) -> Result<f64> {
        let current = self.read_cgroup_file("memory.current")?
            .trim()
            .parse::<u64>()?;
        
        let limit = self.read_cgroup_file("memory.max")?
            .trim()
            .parse::<u64>()
            .unwrap_or(u64::MAX);
        
        if limit == u64::MAX {
            // No limit set, use node memory
            let mut sys = System::new();
            sys.refresh_memory();
            let total = sys.total_memory();
            return Ok((current as f64 / total as f64) * 100.0);
        }
        
        Ok((current as f64 / limit as f64) * 100.0)
    }
    
    async fn get_disk_io(&self) -> Result<DiskIOStats> {
        // Read from /proc/self/io
        let io_stat = std::fs::read_to_string("/proc/self/io")?;
        let (read_bytes, write_bytes) = parse_proc_io(&io_stat)?;
        
        Ok(DiskIOStats {
            read_bytes_per_sec: read_bytes,
            write_bytes_per_sec: write_bytes,
        })
    }
    
    async fn get_network_io(&self) -> Result<NetworkIOStats> {
        // Read from /proc/net/dev
        let net_dev = std::fs::read_to_string("/proc/net/dev")?;
        let (rx_bytes, tx_bytes) = parse_net_dev(&net_dev)?;
        
        let mut prev = self.prev_snapshot.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(prev.timestamp).as_secs_f64();
        
        if elapsed == 0.0 {
            return Ok(NetworkIOStats {
                rx_bytes_per_sec: 0,
                tx_bytes_per_sec: 0,
            });
        }
        
        let rx_rate = ((rx_bytes - prev.network_rx_bytes) as f64 / elapsed) as u64;
        let tx_rate = ((tx_bytes - prev.network_tx_bytes) as f64 / elapsed) as u64;
        
        prev.network_rx_bytes = rx_bytes;
        prev.network_tx_bytes = tx_bytes;
        prev.timestamp = now;
        
        Ok(NetworkIOStats {
            rx_bytes_per_sec: rx_rate,
            tx_bytes_per_sec: tx_rate,
        })
    }
}

#[cfg(target_os = "linux")]
fn detect_cgroup_path() -> String {
    // Try cgroup v2 first
    if std::path::Path::new("/sys/fs/cgroup/cgroup.controllers").exists() {
        return "/sys/fs/cgroup".to_string();
    }
    // Fallback to v1
    "/sys/fs/cgroup/cpu".to_string()
}

#[cfg(target_os = "linux")]
fn parse_cpu_stat(content: &str) -> Result<u64> {
    for line in content.lines() {
        if line.starts_with("usage_usec") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                return parts[1].parse().map_err(|e| anyhow::anyhow!("Parse error: {}", e));
            }
        }
    }
    Err(anyhow::anyhow!("usage_usec not found in cpu.stat"))
}

#[cfg(target_os = "linux")]
fn parse_cpu_quota(content: &str) -> Result<(u64, u64)> {
    let parts: Vec<&str> = content.trim().split_whitespace().collect();
    if parts.len() >= 2 {
        let quota = parts[0].parse().unwrap_or(u64::MAX);
        let period = parts[1].parse().unwrap_or(100_000);
        Ok((quota, period))
    } else {
        Ok((u64::MAX, 100_000))
    }
}

#[cfg(target_os = "linux")]
fn parse_proc_io(content: &str) -> Result<(u64, u64)> {
    let mut read_bytes = 0u64;
    let mut write_bytes = 0u64;
    
    for line in content.lines() {
        if line.starts_with("read_bytes:") {
            read_bytes = line.split(':').nth(1)
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0);
        } else if line.starts_with("write_bytes:") {
            write_bytes = line.split(':').nth(1)
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0);
        }
    }
    
    Ok((read_bytes, write_bytes))
}

#[cfg(target_os = "linux")]
fn parse_net_dev(content: &str) -> Result<(u64, u64)> {
    let mut total_rx = 0u64;
    let mut total_tx = 0u64;
    
    for line in content.lines().skip(2) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 10 {
            total_rx += parts[1].parse::<u64>().unwrap_or(0);
            total_tx += parts[9].parse::<u64>().unwrap_or(0);
        }
    }
    
    Ok((total_rx, total_tx))
}

// Non-Linux platforms: use native monitor
#[cfg(not(target_os = "linux"))]
pub struct KubernetesResourceMonitor;

#[cfg(not(target_os = "linux"))]
impl KubernetesResourceMonitor {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(not(target_os = "linux"))]
#[async_trait]
impl ResourceMonitor for KubernetesResourceMonitor {
    async fn get_cpu_usage(&self) -> Result<f64> {
        Err(anyhow::anyhow!("Kubernetes monitoring only supported on Linux"))
    }
    
    async fn get_memory_usage(&self) -> Result<f64> {
        Err(anyhow::anyhow!("Kubernetes monitoring only supported on Linux"))
    }
    
    async fn get_disk_io(&self) -> Result<DiskIOStats> {
        Err(anyhow::anyhow!("Kubernetes monitoring only supported on Linux"))
    }
    
    async fn get_network_io(&self) -> Result<NetworkIOStats> {
        Err(anyhow::anyhow!("Kubernetes monitoring only supported on Linux"))
    }
}
```

**Verification:**
- [ ] Compiles on Linux
- [ ] Test in Docker container: `docker run --rm -it danube-broker`
- [ ] Test in Kubernetes pod
- [ ] Verify cgroup v2 detection works

---

### Step 5: Update LoadReport Structure
**Status:** ☐ Not Started

**Task:** Enhance `load_report.rs` with new resource types and timestamp

```rust
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LoadReport {
    pub(crate) resources_usage: Vec<SystemLoad>,
    pub(crate) topics_len: usize,
    pub(crate) topic_list: Vec<String>,
    pub(crate) timestamp: u64, // Unix timestamp
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SystemLoad {
    pub(crate) resource: ResourceType,
    pub(crate) usage: f64, // Changed from usize to f64 for better precision
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ResourceType {
    CPU,
    Memory,
    DiskIO,     // NEW
    NetworkIO,  // NEW
}
```

**Verification:**
- [ ] Existing code compiles
- [ ] Serialization/deserialization tests pass

---

### Step 6: Replace Mock with Real Resource Monitoring
**Status:** ☐ Not Started

**Task:** Update `generate_load_report()` to use ResourceMonitor

```rust
use super::resource_monitor::{ResourceMonitor, create_resource_monitor};
use std::sync::Arc;
use tokio::sync::OnceCell;

// Global resource monitor instance
static RESOURCE_MONITOR: OnceCell<Arc<Box<dyn ResourceMonitor>>> = OnceCell::const_new();

async fn get_resource_monitor() -> Arc<Box<dyn ResourceMonitor>> {
    RESOURCE_MONITOR
        .get_or_init(|| async {
            Arc::new(create_resource_monitor())
        })
        .await
        .clone()
}

pub(crate) async fn generate_load_report(
    topics_len: usize,
    topic_list: Vec<String>,
) -> LoadReport {
    let monitor = get_resource_monitor().await;
    
    // Collect real system metrics
    let cpu_usage = monitor.get_cpu_usage().await.unwrap_or(0.0);
    let mem_usage = monitor.get_memory_usage().await.unwrap_or(0.0);
    let disk_io = monitor.get_disk_io().await.ok();
    let net_io = monitor.get_network_io().await.ok();
    
    let mut resources = vec![
        SystemLoad {
            resource: ResourceType::CPU,
            usage: cpu_usage,
        },
        SystemLoad {
            resource: ResourceType::Memory,
            usage: mem_usage,
        },
    ];
    
    if let Some(disk) = disk_io {
        resources.push(SystemLoad {
            resource: ResourceType::DiskIO,
            usage: disk.total_bytes_per_sec() as f64,
        });
    }
    
    if let Some(net) = net_io {
        resources.push(SystemLoad {
            resource: ResourceType::NetworkIO,
            usage: net.total_bytes_per_sec() as f64,
        });
    }
    
    LoadReport {
        resources_usage: resources,
        topics_len,
        topic_list,
        timestamp: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    }
}

// Remove the old mock function
// #[deprecated(note = "Use generate_load_report with ResourceMonitor")]
// pub(crate) fn get_system_resource_usage() -> Vec<SystemLoad> { ... }
```

**Verification:**
- [ ] Remove `get_system_resource_usage()` function
- [ ] Update all call sites
- [ ] Run integration tests
- [ ] Verify real metrics are being collected

---

### Step 7: Update Module Exports
**Status:** ☐ Not Started

**Task:** Export new module in `load_manager.rs`

```rust
pub(crate) mod load_report;
pub(crate) mod resource_monitor; // NEW
mod rankings;
```

**Verification:**
- [ ] Module is accessible
- [ ] No circular dependencies

---

### Step 8: Add Unit Tests
**Status:** ☐ Not Started

**Task:** Create `danube-broker/src/danube_service/load_manager/resource_monitor_test.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_native_monitor_creation() {
        let monitor = NativeResourceMonitor::new();
        assert!(monitor.system.lock().await.cpus().len() > 0);
    }
    
    #[tokio::test]
    async fn test_cpu_usage_range() {
        let monitor = NativeResourceMonitor::new();
        let cpu = monitor.get_cpu_usage().await.unwrap();
        assert!(cpu >= 0.0 && cpu <= 100.0, "CPU usage out of range: {}", cpu);
    }
    
    #[tokio::test]
    async fn test_memory_usage_range() {
        let monitor = NativeResourceMonitor::new();
        let mem = monitor.get_memory_usage().await.unwrap();
        assert!(mem >= 0.0 && mem <= 100.0, "Memory usage out of range: {}", mem);
    }
    
    #[tokio::test]
    async fn test_disk_io_stats() {
        let monitor = NativeResourceMonitor::new();
        let disk = monitor.get_disk_io().await.unwrap();
        assert!(disk.read_bytes_per_sec >= 0);
        assert!(disk.write_bytes_per_sec >= 0);
    }
    
    #[tokio::test]
    async fn test_network_io_stats() {
        let monitor = NativeResourceMonitor::new();
        let net = monitor.get_network_io().await.unwrap();
        assert!(net.rx_bytes_per_sec >= 0);
        assert!(net.tx_bytes_per_sec >= 0);
    }
    
    #[tokio::test]
    async fn test_factory_creates_monitor() {
        let monitor = create_resource_monitor();
        // Should not panic
        let _ = monitor.get_cpu_usage().await;
    }
    
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_k8s_detection() {
        // Test environment variable detection
        std::env::set_var("KUBERNETES_SERVICE_HOST", "10.0.0.1");
        assert!(is_running_in_k8s());
        std::env::remove_var("KUBERNETES_SERVICE_HOST");
    }
}
```

**Verification:**
- [ ] All tests pass: `cargo test -p danube-broker resource_monitor`
- [ ] Tests run on CI/CD pipeline

---

### Step 9: Integration Testing
**Status:** ☐ Not Started

**Task:** Test end-to-end LoadReport generation with real metrics

```rust
#[tokio::test]
async fn test_load_report_with_real_metrics() {
    let report = generate_load_report(5, vec![
        "/default/topic1".to_string(),
        "/default/topic2".to_string(),
    ]).await;
    
    // Verify structure
    assert_eq!(report.topics_len, 5);
    assert_eq!(report.topic_list.len(), 2);
    assert!(report.timestamp > 0);
    
    // Verify at least CPU and Memory are present
    assert!(report.resources_usage.len() >= 2);
    
    // Verify CPU metric
    let cpu = report.resources_usage.iter()
        .find(|r| matches!(r.resource, ResourceType::CPU))
        .expect("CPU metric missing");
    assert!(cpu.usage >= 0.0 && cpu.usage <= 100.0);
    
    // Verify Memory metric
    let mem = report.resources_usage.iter()
        .find(|r| matches!(r.resource, ResourceType::Memory))
        .expect("Memory metric missing");
    assert!(mem.usage >= 0.0 && mem.usage <= 100.0);
}
```

**Verification:**
- [ ] Test passes on native platform
- [ ] Test passes in Docker
- [ ] Test passes in Kubernetes (if available)

---

### Step 10: Documentation
**Status:** ☐ Not Started

**Task:** Update documentation

**Files to update:**
- [ ] `danube-broker/README.md` - Document resource monitoring
- [ ] `danube-broker/src/danube_service/load_manager/README.md` - Add architecture notes
- [ ] Add inline documentation to all public functions

**Example:**
```markdown
## Resource Monitoring

The LoadManager now uses real system metrics instead of mock data:

- **CPU Usage**: Percentage of CPU utilization (0-100%)
- **Memory Usage**: Percentage of memory utilization (0-100%)
- **Disk I/O**: Read/write bytes per second
- **Network I/O**: RX/TX bytes per second

### Platform Support

- **Linux**: Native + cgroup-aware (Docker/Kubernetes)
- **macOS**: Native using sysinfo
- **Windows**: Native using sysinfo

### Container Environments

Automatically detects container environments and uses cgroup limits:
- Kubernetes pods: Reads from cgroup v2 (/sys/fs/cgroup)
- Docker containers: Reads from cgroup v1 or v2
```

**Verification:**
- [ ] Documentation is clear and accurate
- [ ] Examples work as documented

---

## ✅ Success Criteria

Phase 1 is complete when:

- [ ] All 10 implementation steps are marked complete
- [ ] Real metrics replace mock data in LoadReport
- [ ] Tests pass on Linux, macOS, and Windows
- [ ] Container detection works in Docker and Kubernetes
- [ ] LoadManager uses accurate resource data for rankings
- [ ] No performance degradation (metric collection <1% CPU overhead)
- [ ] Documentation is updated

---

## 🧪 Testing Checklist

- [ ] Unit tests for ResourceMonitor implementations
- [ ] Integration tests for LoadReport generation
- [ ] Platform-specific tests (Linux, macOS, Windows)
- [ ] Container tests (Docker)
- [ ] Kubernetes tests (minikube or real cluster)
- [ ] Performance tests (overhead < 1% CPU)
- [ ] Stress tests (1000+ topics)

---

## 📊 Metrics to Track

After completion, monitor:
- Load report accuracy (compare with system tools)
- Metric collection overhead (CPU/memory)
- Latency of metric collection (should be <100ms)
- Error rates in container environments

---

## 🔗 Dependencies

- `sysinfo = "0.30"` - Cross-platform system info
- `procfs = "0.16"` (Linux) - Cgroup parsing
- `async-trait = "0.1"` - Async trait support
- `anyhow = "1.0"` - Error handling
- `tokio = { version = "1.0", features = ["sync", "time"] }`

---

## 📝 Notes

- Start with native monitoring and verify it works before adding K8s support
- Test incrementally on each platform
- Keep metric collection lightweight (avoid blocking operations)
- Consider caching metrics for short periods (1-5 seconds) to reduce overhead
