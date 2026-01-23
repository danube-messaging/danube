//! Container-aware resource monitoring for Docker and Kubernetes
//!
//! Reads cgroup v1 and v2 metrics to accurately report resource usage
//! relative to container limits rather than host machine totals.

use super::{DiskIOStats, NetworkIOStats, ResourceMonitor};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::fs;
use std::path::Path;

/// Container resource monitor using cgroup metrics
pub(crate) struct ContainerResourceMonitor {
    cgroup_version: CgroupVersion,
}

#[derive(Debug, Clone)]
enum CgroupVersion {
    V1,
    V2,
}

impl ContainerResourceMonitor {
    pub(crate) fn new() -> Result<Self> {
        let version = detect_cgroup_version()?;
        Ok(Self {
            cgroup_version: version,
        })
    }
}

#[async_trait]
impl ResourceMonitor for ContainerResourceMonitor {
    async fn get_cpu_usage(&self) -> Result<f64> {
        match self.cgroup_version {
            CgroupVersion::V1 => get_cpu_usage_v1().await,
            CgroupVersion::V2 => get_cpu_usage_v2().await,
        }
    }

    async fn get_memory_usage(&self) -> Result<f64> {
        match self.cgroup_version {
            CgroupVersion::V1 => get_memory_usage_v1().await,
            CgroupVersion::V2 => get_memory_usage_v2().await,
        }
    }

    async fn get_disk_io(&self) -> Result<DiskIOStats> {
        match self.cgroup_version {
            CgroupVersion::V1 => get_disk_io_v1().await,
            CgroupVersion::V2 => get_disk_io_v2().await,
        }
    }

    async fn get_network_io(&self) -> Result<NetworkIOStats> {
        // Network I/O is not in cgroups, use procfs
        get_network_io_procfs().await
    }
}

// ============================================================================
// Detection
// ============================================================================

fn detect_cgroup_version() -> Result<CgroupVersion> {
    // Check for cgroup v2 (unified hierarchy)
    if Path::new("/sys/fs/cgroup/cgroup.controllers").exists() {
        return Ok(CgroupVersion::V2);
    }

    // Check for cgroup v1
    if Path::new("/sys/fs/cgroup/memory").exists() || Path::new("/sys/fs/cgroup/cpu").exists() {
        return Ok(CgroupVersion::V1);
    }

    anyhow::bail!("No cgroup filesystem detected")
}

/// Detect if running inside a container
pub(crate) fn is_containerized() -> bool {
    // Check for Docker
    if Path::new("/.dockerenv").exists() {
        return true;
    }

    // Check for Kubernetes
    if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
        return true;
    }

    // Check cgroup for container indicators
    if let Ok(content) = fs::read_to_string("/proc/1/cgroup") {
        if content.contains("docker") || content.contains("kubepods") {
            return true;
        }
    }

    false
}

// ============================================================================
// cgroup v1 Implementation
// ============================================================================

async fn get_cpu_usage_v1() -> Result<f64> {
    // Read CPU quota/period to determine limits
    let quota = read_file_i64("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .or_else(|_| read_file_i64("/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us"))
        .unwrap_or(-1);

    let period = read_file_u64("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        .or_else(|_| read_file_u64("/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us"))
        .unwrap_or(100_000);

    // Calculate CPU percentage
    if quota > 0 && period > 0 {
        let cpu_limit = quota as f64 / period as f64 * 100.0;

        // Get current usage via procfs
        if let Ok(stat) = procfs::process::Process::myself().and_then(|p| p.stat()) {
            let total_time = stat.utime + stat.stime;

            // Read uptime from /proc/uptime
            if let Ok(uptime_data) = fs::read_to_string("/proc/uptime") {
                if let Some(uptime_str) = uptime_data.split_whitespace().next() {
                    if let Ok(uptime) = uptime_str.parse::<f64>() {
                        let hz = procfs::ticks_per_second();

                        let cpu_usage = (total_time as f64 / hz as f64 / uptime) * 100.0;
                        let percentage = (cpu_usage / cpu_limit) * 100.0;

                        return Ok(percentage.min(100.0));
                    }
                }
            }
        }
    }

    Ok(0.0)
}

async fn get_memory_usage_v1() -> Result<f64> {
    let usage = read_file_u64("/sys/fs/cgroup/memory/memory.usage_in_bytes")?;
    let limit = read_file_u64("/sys/fs/cgroup/memory/memory.limit_in_bytes")?;

    if limit == 0 {
        return Ok(0.0);
    }

    let percentage = (usage as f64 / limit as f64) * 100.0;
    Ok(percentage.min(100.0))
}

async fn get_disk_io_v1() -> Result<DiskIOStats> {
    let content = fs::read_to_string("/sys/fs/cgroup/blkio/blkio.throttle.io_service_bytes")
        .context("Failed to read blkio stats")?;

    let mut read_bytes = 0u64;
    let mut write_bytes = 0u64;

    for line in content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 3 {
            let value = parts[2].parse::<u64>().unwrap_or(0);
            if parts[1] == "Read" {
                read_bytes += value;
            } else if parts[1] == "Write" {
                write_bytes += value;
            }
        }
    }

    // These are totals, not rates - caller should calculate rate
    Ok(DiskIOStats {
        read_bytes_per_sec: read_bytes,
        write_bytes_per_sec: write_bytes,
    })
}

// ============================================================================
// cgroup v2 Implementation
// ============================================================================

async fn get_cpu_usage_v2() -> Result<f64> {
    // Read CPU limit
    let max_content =
        fs::read_to_string("/sys/fs/cgroup/cpu.max").unwrap_or_else(|_| "max 100000".to_string());

    let parts: Vec<&str> = max_content.trim().split_whitespace().collect();
    if parts.len() >= 2 {
        if parts[0] != "max" {
            if let (Ok(quota), Ok(period)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
                let cpu_limit = quota as f64 / period as f64 * 100.0;

                // Calculate current usage
                if let Ok(stat) = procfs::process::Process::myself().and_then(|p| p.stat()) {
                    let total_time = stat.utime + stat.stime;

                    // Read uptime from /proc/uptime
                    if let Ok(uptime_data) = fs::read_to_string("/proc/uptime") {
                        if let Some(uptime_str) = uptime_data.split_whitespace().next() {
                            if let Ok(uptime) = uptime_str.parse::<f64>() {
                                let hz = procfs::ticks_per_second();

                                let cpu_usage = (total_time as f64 / hz as f64 / uptime) * 100.0;
                                let percentage = (cpu_usage / cpu_limit) * 100.0;

                                return Ok(percentage.min(100.0));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(0.0)
}

async fn get_memory_usage_v2() -> Result<f64> {
    let usage = read_file_u64("/sys/fs/cgroup/memory.current")?;
    let limit = read_file_u64("/sys/fs/cgroup/memory.max").unwrap_or(u64::MAX);

    if limit == 0 || limit == u64::MAX {
        return Ok(0.0);
    }

    let percentage = (usage as f64 / limit as f64) * 100.0;
    Ok(percentage.min(100.0))
}

async fn get_disk_io_v2() -> Result<DiskIOStats> {
    let content = fs::read_to_string("/sys/fs/cgroup/io.stat").context("Failed to read io.stat")?;

    let mut read_bytes = 0u64;
    let mut write_bytes = 0u64;

    for line in content.lines() {
        for part in line.split_whitespace().skip(1) {
            if let Some((key, value)) = part.split_once('=') {
                let val = value.parse::<u64>().unwrap_or(0);
                match key {
                    "rbytes" => read_bytes += val,
                    "wbytes" => write_bytes += val,
                    _ => {}
                }
            }
        }
    }

    Ok(DiskIOStats {
        read_bytes_per_sec: read_bytes,
        write_bytes_per_sec: write_bytes,
    })
}

// ============================================================================
// Network I/O via procfs
// ============================================================================

async fn get_network_io_procfs() -> Result<NetworkIOStats> {
    let net = procfs::net::dev_status().context("Failed to read network stats")?;

    let mut total_rx = 0u64;
    let mut total_tx = 0u64;

    for (_, stats) in net {
        total_rx += stats.recv_bytes;
        total_tx += stats.sent_bytes;
    }

    // These are totals, not rates - caller should calculate rate
    Ok(NetworkIOStats {
        rx_bytes_per_sec: total_rx,
        tx_bytes_per_sec: total_tx,
    })
}

// ============================================================================
// Helpers
// ============================================================================

fn read_file_u64(path: &str) -> Result<u64> {
    let content = fs::read_to_string(path).with_context(|| format!("Failed to read {}", path))?;
    content
        .trim()
        .parse()
        .with_context(|| format!("Failed to parse {} as u64", path))
}

fn read_file_i64(path: &str) -> Result<i64> {
    let content = fs::read_to_string(path).with_context(|| format!("Failed to read {}", path))?;
    content
        .trim()
        .parse()
        .with_context(|| format!("Failed to parse {} as i64", path))
}
