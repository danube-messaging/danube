//! System Resource Monitoring
//!
//! Cross-platform resource monitoring for CPU, memory, disk I/O, and network I/O.
//! Provides async interfaces for collecting real-time system metrics.
//!
//! Automatically detects containerized environments (Docker/K8s) and uses
//! cgroup-aware metrics when available.

mod container;
mod native;

pub(crate) use container::ContainerResourceMonitor;
pub(crate) use native::NativeResourceMonitor;

use anyhow::Result;
use async_trait::async_trait;

/// Cross-platform system resource monitoring trait
#[async_trait]
pub(crate) trait ResourceMonitor: Send + Sync {
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
pub(crate) struct DiskIOStats {
    pub(crate) read_bytes_per_sec: u64,
    pub(crate) write_bytes_per_sec: u64,
}

impl DiskIOStats {
    pub(crate) fn total_bytes_per_sec(&self) -> u64 {
        self.read_bytes_per_sec + self.write_bytes_per_sec
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NetworkIOStats {
    pub(crate) rx_bytes_per_sec: u64,
    pub(crate) tx_bytes_per_sec: u64,
}

impl NetworkIOStats {
    pub(crate) fn total_bytes_per_sec(&self) -> u64 {
        self.rx_bytes_per_sec + self.tx_bytes_per_sec
    }
}

/// Factory function to create the appropriate resource monitor
/// 
/// Automatically detects if running in a container and uses cgroup-aware
/// monitoring when available. Falls back to native system monitoring.
pub(crate) fn create_resource_monitor() -> Box<dyn ResourceMonitor> {
    // Try container-aware monitoring first
    if container::is_containerized() {
        if let Ok(monitor) = ContainerResourceMonitor::new() {
            tracing::info!("Using container-aware resource monitoring (cgroups)");
            return Box::new(monitor);
        } else {
            tracing::warn!("Container detected but cgroup monitoring failed, falling back to native");
        }
    }
    
    tracing::info!("Using native resource monitoring (sysinfo)");
    Box::new(NativeResourceMonitor::new())
}
