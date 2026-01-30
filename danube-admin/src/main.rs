mod cli;
mod core;
mod mcp;
mod server;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "danube-admin")]
#[command(about = "Danube Admin - Unified CLI and server for managing Danube clusters", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// [Server] Start the HTTP admin server
    #[command(alias = "server")]
    #[command(display_order = 1)]
    Serve(server::ServerArgs),

    /// [CLI] Manage brokers in the cluster
    #[command(display_order = 10)]
    Brokers(cli::brokers::Brokers),

    /// [CLI] Manage namespaces
    #[command(display_order = 11)]
    Namespaces(cli::namespaces::Namespaces),

    /// [CLI] Manage topics
    #[command(display_order = 12)]
    Topics(cli::topics::Topics),

    /// [CLI] Manage schemas in the schema registry
    #[command(display_order = 13)]
    Schemas(cli::schemas::Schemas),
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing/logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve(args) => {
            tracing::info!("Starting danube-admin server");
            server::run(args).await
        }
        Commands::Brokers(cmd) => {
            cli::brokers::handle(cmd).await
        }
        Commands::Namespaces(cmd) => {
            cli::namespaces::handle(cmd).await
        }
        Commands::Topics(cmd) => {
            cli::topics::handle(cmd).await
        }
        Commands::Schemas(cmd) => {
            cli::schemas::handle(cmd).await
        }
    }
}
