mod consume;
mod produce;
mod schema;
mod utils;

use anyhow::Result;
use clap::{Parser, Subcommand};
use consume::Consume;
use produce::Produce;
use schema::Schema;

#[derive(Debug, Parser)]
#[command(name = "danube-cli")]
#[command(about = "A command-line tool to interact with Danube service")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(about = "Produce messages to a topic")]
    Produce(Produce),
    
    #[command(about = "Consume messages from a topic")]
    Consume(Consume),
    
    #[command(about = "Manage schemas in the schema registry")]
    Schema(Schema),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Produce(produce) => produce::handle_produce(produce).await?,
        Commands::Consume(consume) => consume::handle_consume(consume).await?,
        Commands::Schema(schema) => schema::handle_schema(schema).await?,
    }

    Ok(())
}
