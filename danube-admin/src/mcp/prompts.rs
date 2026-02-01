//! MCP Prompts module
//!
//! Re-exports prompt definitions from submodules and provides
//! the main routing functions for list_prompts and get_prompt.

pub mod cluster_operations;
pub mod schema_operations;
pub mod topic_operations;
pub mod troubleshooting;

use rmcp::model::{GetPromptRequestParams, GetPromptResult, ListPromptsResult};

/// Get the list of all available prompts
pub fn list_prompts() -> ListPromptsResult {
    let mut prompts = Vec::new();

    // Add troubleshooting prompts
    prompts.extend(troubleshooting::prompts());

    // Add operational workflow prompts
    prompts.extend(topic_operations::prompts());
    prompts.extend(schema_operations::prompts());
    prompts.extend(cluster_operations::prompts());

    // Future: Add more prompt categories here
    // prompts.extend(onboarding::prompts());

    ListPromptsResult {
        prompts,
        next_cursor: None,
        meta: None,
    }
}

/// Get a specific prompt by name, routing to the appropriate submodule
pub fn get_prompt(params: &GetPromptRequestParams) -> Option<GetPromptResult> {
    // Try troubleshooting prompts first
    if let Some(result) = troubleshooting::get_prompt(params) {
        return Some(result);
    }

    // Try operational prompts (by domain)
    if let Some(result) = topic_operations::get_prompt(params) {
        return Some(result);
    }

    if let Some(result) = schema_operations::get_prompt(params) {
        return Some(result);
    }

    if let Some(result) = cluster_operations::get_prompt(params) {
        return Some(result);
    }

    // Future: Try other prompt categories
    // if let Some(result) = onboarding::get_prompt(params) {
    //     return Some(result);
    // }

    None
}
