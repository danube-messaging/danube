//! MCP Tool implementations
//!
//! Each module contains tool functions that interact with the Danube cluster
//! and return human-readable output for AI assistants.

pub mod cluster;
pub mod diagnostics;
pub mod logs;
pub mod metrics;
pub mod schemas;
pub mod security;
pub mod topics;

/// Enforce explicit confirmation before executing destructive MCP operations.
pub(crate) fn require_destructive_confirmation(
    confirm_destructive: bool,
    operation: &str,
) -> Option<String> {
    if confirm_destructive {
        None
    } else {
        Some(format!(
            "Refused: destructive operation '{}' requires confirm_destructive=true",
            operation
        ))
    }
}
