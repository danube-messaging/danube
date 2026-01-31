//! MCP Resources module
//!
//! Re-exports resource definitions from submodules and provides
//! the main routing functions for list_resources and read_resource.

pub mod metrics;

use rmcp::model::{ListResourcesResult, ReadResourceRequestParams, ReadResourceResult};

/// Get the list of all available resources
pub fn list_resources() -> ListResourcesResult {
    let mut resources = Vec::new();

    // Add metrics resource
    resources.push(metrics::resource());

    // Future: Add more resource categories here
    // resources.push(config::resource());
    // resources.push(schemas::resource());

    ListResourcesResult {
        resources,
        next_cursor: None,
        meta: None,
    }
}

/// Read a specific resource by URI, routing to the appropriate submodule
pub fn read_resource(params: &ReadResourceRequestParams) -> Option<ReadResourceResult> {
    // Try metrics resource first
    if let Some(result) = metrics::read(&params.uri) {
        return Some(result);
    }

    // Future: Try other resource types
    // if let Some(result) = config::read(&params.uri) {
    //     return Some(result);
    // }

    None
}
