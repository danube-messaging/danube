//! Danube admin library: client-side commands (`topic`, `cluster`, `produce`, ...),
//! the web console backend and the MCP server used by the unified `danube` binary.
//!
//! The command-line tree itself lives in the `danube` binary (`danube-broker/src/cli`);
//! this crate exposes the clap `Args` structs and their handlers.

pub mod cli;
pub mod core;
pub mod mcp;
pub mod metrics;
pub mod server;

pub use server::{ConsoleArgs, McpArgs};

/// Run `danube console`.
pub async fn run_console(args: ConsoleArgs) -> anyhow::Result<()> {
    server::run(args.into_server_args()?).await
}

/// Run `danube mcp`.
pub async fn run_mcp(args: McpArgs) -> anyhow::Result<()> {
    server::run(args.into_server_args()?).await
}
