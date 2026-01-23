//! Native resource monitoring implementation using sysinfo
//!
//! Works on Linux, macOS, and Windows platforms.

use super::{DiskIOStats, NetworkIOStats, ResourceMonitor};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, Networks, RefreshKind, System};
use tokio::sync::Mutex;

/// Native resource monitor using sysinfo crate
pub(crate) struct NativeResourceMonitor {
    system: Arc<Mutex<System>>,
    prev_snapshot: Arc<Mutex<ResourceSnapshot>>,
}

#[derive(Debug, Clone)]
struct ResourceSnapshot {
    timestamp: Instant,
    network_rx_bytes: u64,
    network_tx_bytes: u64,
}

impl NativeResourceMonitor {
    pub(crate) fn new() -> Self {
        let system = System::new_with_specifics(
            RefreshKind::nothing()
                .with_cpu(CpuRefreshKind::everything())
                .with_memory(MemoryRefreshKind::everything()),
        );

        Self {
            system: Arc::new(Mutex::new(system)),
            prev_snapshot: Arc::new(Mutex::new(ResourceSnapshot {
                timestamp: Instant::now(),
                network_rx_bytes: 0,
                network_tx_bytes: 0,
            })),
        }
    }
}

#[async_trait]
impl ResourceMonitor for NativeResourceMonitor {
    async fn get_cpu_usage(&self) -> Result<f64> {
        let mut system = self.system.lock().await;
        
        // First refresh to get initial values
        system.refresh_cpu_all();
        
        // Wait for accurate measurement (sysinfo recommendation)
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // Second refresh to calculate actual usage
        system.refresh_cpu_all();
        
        // Calculate average CPU usage across all cores
        let cpus = system.cpus();
        if cpus.is_empty() {
            return Ok(0.0);
        }
        
        let total_usage: f32 = cpus.iter().map(|cpu| cpu.cpu_usage()).sum();
        let avg_usage = total_usage / cpus.len() as f32;
        
        Ok(avg_usage as f64)
    }
    
    async fn get_memory_usage(&self) -> Result<f64> {
        let mut system = self.system.lock().await;
        system.refresh_memory();
        
        let used = system.used_memory();
        let total = system.total_memory();
        
        if total == 0 {
            return Ok(0.0);
        }
        
        let percentage = (used as f64 / total as f64) * 100.0;
        Ok(percentage)
    }
    
    async fn get_disk_io(&self) -> Result<DiskIOStats> {
        // Note: sysinfo doesn't provide real-time disk I/O rates directly
        // For now, return 0 - will be improved in container/K8s phase
        Ok(DiskIOStats {
            read_bytes_per_sec: 0,
            write_bytes_per_sec: 0,
        })
    }
    
    async fn get_network_io(&self) -> Result<NetworkIOStats> {
        // Create temporary Networks instance for this measurement
        let networks = Networks::new_with_refreshed_list();
        
        let mut total_rx = 0u64;
        let mut total_tx = 0u64;
        
        for (_, network) in &networks {
            total_rx += network.total_received();
            total_tx += network.total_transmitted();
        }
        
        let mut prev = self.prev_snapshot.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(prev.timestamp).as_secs_f64();
        
        if elapsed == 0.0 {
            return Ok(NetworkIOStats {
                rx_bytes_per_sec: 0,
                tx_bytes_per_sec: 0,
            });
        }
        
        let rx_rate = if prev.network_rx_bytes > 0 {
            ((total_rx.saturating_sub(prev.network_rx_bytes)) as f64 / elapsed) as u64
        } else {
            0
        };
        
        let tx_rate = if prev.network_tx_bytes > 0 {
            ((total_tx.saturating_sub(prev.network_tx_bytes)) as f64 / elapsed) as u64
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_all_monitoring_functions() {
        let monitor = NativeResourceMonitor::new();
        
        // Test CPU monitoring
        let cpu_result = monitor.get_cpu_usage().await;
        assert!(cpu_result.is_ok(), "CPU monitoring should work");
        let cpu = cpu_result.unwrap();
        assert!(cpu >= 0.0 && cpu <= 100.0, "CPU should be valid percentage");
        
        // Test memory monitoring
        let mem_result = monitor.get_memory_usage().await;
        assert!(mem_result.is_ok(), "Memory monitoring should work");
        let mem = mem_result.unwrap();
        assert!(mem > 0.0 && mem <= 100.0, "Memory should be >0 on running system");
        
        // Test disk I/O (currently returns 0)
        let disk_result = monitor.get_disk_io().await;
        assert!(disk_result.is_ok(), "Disk I/O should not panic");
        
        // Test network I/O
        let net_result = monitor.get_network_io().await;
        assert!(net_result.is_ok(), "Network I/O should work");
        
        // Test multiple measurements work
        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
        let cpu2 = monitor.get_cpu_usage().await.unwrap();
        assert!(cpu2 >= 0.0 && cpu2 <= 100.0, "Second measurement should also work");
    }
}
