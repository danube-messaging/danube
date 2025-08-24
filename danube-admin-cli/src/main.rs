mod brokers;
mod namespaces;
mod shared;
mod topics;
mod client;

use brokers::Brokers;
use namespaces::Namespaces;
use topics::Topics;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "danube-admin-cli")]
#[command(about = "CLI for managing the Danube pub/sub platform", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(about = "Manage and view brokers information from the Danube cluster")]
    Brokers(Brokers),
    #[command(about = "Manage the namespaces from the Danube cluster")]
    Namespaces(Namespaces),
    #[command(name = "topics", about = "Manage the topics from the Danube cluster")]
    Topics(Topics),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Brokers(brokers) => brokers::handle_command(brokers).await?,
        Commands::Namespaces(namespaces) => namespaces::handle_command(namespaces).await?,
        Commands::Topics(topics) => topics::handle_command(topics).await?,
    }

    Ok(())
}
