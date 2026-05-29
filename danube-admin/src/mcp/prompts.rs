//! MCP Prompts module
//!
//! Re-exports prompt definitions from submodules and provides
//! the main routing functions for list_prompts and get_prompt.

pub mod cluster_operations;
pub mod schema_operations;
pub mod security_operations;
pub mod troubleshooting;

use rmcp::model::{GetPromptRequestParams, GetPromptResult, ListPromptsResult};

/// Get the list of all available prompts
pub fn list_prompts() -> ListPromptsResult {
    let mut prompts = Vec::new();

    // Troubleshooting (cluster health check)
    prompts.extend(troubleshooting::prompts());

    // Operational workflows
    prompts.extend(cluster_operations::prompts());
    prompts.extend(schema_operations::prompts());
    prompts.extend(security_operations::prompts());

    ListPromptsResult {
        prompts,
        next_cursor: None,
        meta: None,
    }
}

/// Get a specific prompt by name, routing to the appropriate submodule
pub fn get_prompt(params: &GetPromptRequestParams) -> Option<GetPromptResult> {
    if let Some(result) = troubleshooting::get_prompt(params) {
        return Some(result);
    }

    if let Some(result) = cluster_operations::get_prompt(params) {
        return Some(result);
    }

    if let Some(result) = schema_operations::get_prompt(params) {
        return Some(result);
    }

    if let Some(result) = security_operations::get_prompt(params) {
        return Some(result);
    }

    None
}
