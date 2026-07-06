//! danube-iceberg: Standalone Iceberg lakehouse converter for Danube messaging.
//!
//! Continuously exports sealed Danube segments (.dnb1) to Apache Parquet format,
//! making streaming data queryable by DuckDB, Snowflake, Athena, Trino, and
//! any other engine that supports the Parquet / Apache Iceberg table format.
//!
//! Architecture: standalone binary that discovers new segments via the Danube
//! gRPC API, reads .dnb1 files directly from object storage, and writes
//! Parquet files back to object storage.

mod config;

use clap::Parser;
use tracing::info;

/// Danube Iceberg Lakehouse Converter
#[derive(Parser, Debug)]
#[command(name = "danube-iceberg", version, about)]
struct Cli {
    /// Path to the configuration file
    #[arg(short, long, default_value = "danube-iceberg-config.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    info!(config = %cli.config, "starting danube-iceberg");

    let config = config::Config::load(&cli.config)?;
    info!(
        topics = config.topics.len(),
        poll_interval = config.polling.interval_seconds,
        "loaded configuration"
    );

    // TODO: Phase 2b — spawn TopicWorkers per topic
    info!("danube-iceberg initialized, no workers spawned yet (Phase 2b)");

    Ok(())
}
