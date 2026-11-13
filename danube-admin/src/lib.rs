pub mod cli;
pub mod core;
pub mod mcp;
pub mod metrics;
pub mod server;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::ffi::OsString;

#[derive(Debug, Parser)]
#[command(name = "danube-admin", version, about = "Danube cluster admin CLI & MCP gateway")]
pub struct Cli {
    /// Broker gRPC endpoint (supports --endpoint or DANUBE_ADMIN_ENDPOINT)
    #[arg(
        long,
        global = true,
        env = "DANUBE_ADMIN_ENDPOINT",
        default_value = "http://127.0.0.1:6650"
    )]
    pub endpoint: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// [Server] Start the HTTP admin UI / Streamable HTTP MCP server
    #[command(alias = "server")]
    #[command(display_order = 1)]
    Serve(server::ServerArgs),

    /// [Data] Produce messages to a topic
    #[command(display_order = 5)]
    Produce(cli::produce::Produce),

    /// [Data] Consume messages from a topic
    #[command(display_order = 6)]
    Consume(cli::consume::Consume),

    /// [CLI] Manage Raft cluster membership
    #[command(display_order = 9)]
    Cluster(cli::cluster::Cluster),

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

    /// [CLI] Manage authorization roles and bindings
    #[command(display_order = 14)]
    Security(cli::security::Security),

    /// [CLI] Plan or apply a declarative desired-state manifest
    #[command(display_order = 15)]
    Apply(cli::apply::Apply),
}

/// Runs the admin CLI parser and handler using the provided argument iterator
/// (including argv[0]).
pub async fn run_cli_from_args<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::parse_from(args);
    dispatch_cli(cli).await
}

/// Starts the MCP server entrypoint (`danube mcp`) defaulting `--mode mcp`
/// unless `--http` or `--mode` is explicitly supplied.
pub async fn run_mcp_from_args<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let raw: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let has_mode = raw
        .iter()
        .any(|a| a.to_string_lossy() == "--mode" || a.to_string_lossy().starts_with("--mode="));
    let mut forwarded = Vec::with_capacity(raw.len() + 4);
    forwarded.push(OsString::from("danube mcp"));
    forwarded.push(OsString::from("serve"));
    if !has_mode {
        forwarded.push(OsString::from("--mode"));
        forwarded.push(OsString::from("mcp"));
    }
    forwarded.extend(raw.into_iter().skip(1));

    run_cli_from_args(forwarded).await
}

async fn dispatch_cli(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Serve(args) => {
            tracing::info!("Starting danube admin/MCP server");
            server::run(args).await
        }
        Commands::Produce(cmd) => cli::produce::handle(cmd).await,
        Commands::Consume(cmd) => cli::consume::handle(cmd).await,
        Commands::Cluster(cmd) => cli::cluster::handle(cmd, &cli.endpoint).await,
        Commands::Brokers(cmd) => cli::brokers::handle(cmd, &cli.endpoint).await,
        Commands::Namespaces(cmd) => cli::namespaces::handle(cmd, &cli.endpoint).await,
        Commands::Topics(cmd) => cli::topics::handle(cmd, &cli.endpoint).await,
        Commands::Schemas(cmd) => cli::schemas::handle(cmd, &cli.endpoint).await,
        Commands::Security(cmd) => cli::security::handle(cmd, &cli.endpoint).await,
        Commands::Apply(cmd) => cli::apply::handle(cmd, &cli.endpoint).await,
    }
}

