use serde::{Deserialize, Serialize};

/// LoadReport holds information that are required by Load Manager
/// to take the topics allocation decision to brokers
///
/// The broker periodically reports its load metrics to Metadata Store.
/// In this struct can be added any information that can serve to Load Manager
/// to take a better decision in the allocation of topics/partitions to brokers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadReport {
    /// System resource usage metrics (CPU, memory, etc.)
    pub resources_usage: Vec<SystemLoad>,
    /// Number of topics served by the broker
    pub topics_len: usize,
    /// The list of topic_name (/{namespace}/{topic}) served by the broker
    pub topic_list: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemLoad {
    pub resource: ResourceType,
    pub usage: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceType {
    CPU,
    Memory,
}

/// Generates a load report for the current broker
///
/// ## Parameters
/// - `topics_len`: Number of topics managed by this broker
/// - `topic_list`: List of topic paths (/{namespace}/{topic})
///
/// ## Returns
/// A LoadReport containing system resource usage and topic information
pub fn generate_load_report(topics_len: usize, topic_list: Vec<String>) -> LoadReport {
    // Mock system resource usage for now (will be replaced in Phase 1)
    let system_load: Vec<SystemLoad> = get_system_resource_usage();

    LoadReport {
        resources_usage: system_load,
        topics_len,
        topic_list,
    }
}

/// Gets current system resource usage
///
/// **Note**: This is currently a mock implementation.
/// Phase 1 will replace this with real system metrics.
pub fn get_system_resource_usage() -> Vec<SystemLoad> {
    let mut system_load = Vec::new();
    let cpu_usage = SystemLoad {
        resource: ResourceType::CPU,
        usage: 30,
    };
    let mem_usage = SystemLoad {
        resource: ResourceType::Memory,
        usage: 30,
    };
    system_load.push(cpu_usage);
    system_load.push(mem_usage);

    system_load
}
