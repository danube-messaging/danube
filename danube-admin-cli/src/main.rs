mod brokers;
mod namespaces;
mod schema_registry;
mod shared;
mod topics;
mod client;

use brokers::Brokers;
use namespaces::Namespaces;
use schema_registry::SchemaRegistry;
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
    #[command(name = "schemas", about = "Manage schemas in the Schema Registry")]
    Schemas(SchemaRegistry),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Brokers(brokers) => brokers::handle_command(brokers).await?,
        Commands::Namespaces(namespaces) => namespaces::handle_command(namespaces).await?,
        Commands::Topics(topics) => topics::handle_command(topics).await?,
        Commands::Schemas(schemas) => schema_registry::handle_command(schemas).await?,
    }

    Ok(())
}
