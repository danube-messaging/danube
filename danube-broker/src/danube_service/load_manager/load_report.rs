use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::OnceCell;

use crate::danube_service::resource_monitor::{create_resource_monitor, ResourceMonitor};

// LoadReport holds information that are required by Load Manager
// to take the topics allocation decision to brokers
//
// The broker periodically reports its load metrics to Metadata Store.
// In this struct can be added any information that can serve to Load Manager
// to take a better decision in the allocation of topics/partitions to brokers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LoadReport {
    // System resource usage metrics
    pub(crate) resources_usage: Vec<SystemLoad>,
    // Number of topics served by the broker
    pub(crate) topics_len: usize,
    // The list of topic_name (/{namespace}/{topic}) served by the broker
    pub(crate) topic_list: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SystemLoad {
    pub(crate) resource: ResourceType,
    pub(crate) usage: f64,  // Changed from usize to f64 for better precision
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ResourceType {
    CPU,
    Memory,
    DiskIO,
    NetworkIO,
}

// Global resource monitor instance (singleton pattern)
static RESOURCE_MONITOR: OnceCell<Arc<Box<dyn ResourceMonitor>>> = OnceCell::const_new();

async fn get_resource_monitor() -> Arc<Box<dyn ResourceMonitor>> {
    RESOURCE_MONITOR
        .get_or_init(|| async { Arc::new(create_resource_monitor()) })
        .await
        .clone()
}

/// Generates a LoadReport with real system metrics
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
    
    // Add disk I/O if available
    if let Some(disk) = disk_io {
        resources.push(SystemLoad {
            resource: ResourceType::DiskIO,
            usage: disk.total_bytes_per_sec() as f64,
        });
    }
    
    // Add network I/O if available
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
    }
}
