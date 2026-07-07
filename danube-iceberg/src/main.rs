//! danube-iceberg: Standalone Iceberg lakehouse converter for Danube messaging.
//!
//! Continuously exports sealed Danube segments (.dnb1) to Apache Parquet format,
//! making streaming data queryable by DuckDB, Snowflake, Athena, Trino, and
//! any other engine that supports the Parquet / Apache Iceberg table format.
//!
//! Architecture: standalone binary that discovers new segments via the Danube
//! gRPC API, reads .dnb1 files directly from object storage, and writes
//! Parquet files back to object storage.

mod catalog;
mod checkpoint;
mod config;
mod iceberg_schema;
mod schema;
mod schema_resolver;
mod segment_reader;
mod storage;
mod table_manager;
mod worker;
mod writer;

use clap::Parser;
use std::sync::Arc;
use tokio::sync::watch;
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

    // Build object storage handle
    let storage = Arc::new(storage::build_storage(&config.storage)?);
    info!(
        backend = %config.storage.backend,
        root = %config.storage.root,
        "connected to object storage"
    );

    // Build Iceberg catalog (if configured)
    let iceberg_catalog: Option<Arc<dyn iceberg::Catalog>> = if config.catalog.is_disabled() {
        info!("catalog type is 'none' — running in Parquet-only mode (no Iceberg catalog)");
        None
    } else {
        let cat = catalog::build_catalog(&config.catalog).await?;
        Some(cat)
    };

    // Shutdown signal
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Build Danube client for Schema Registry access
    let danube_client = danube_client::DanubeClient::builder()
        .service_url(&format!("http://{}", config.broker.address))
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("failed to build Danube client: {}", e))?;
    info!("connected to Danube Schema Registry");

    // Spawn one worker per configured topic
    let mut handles = Vec::new();

    for topic_cfg in config.topics {
        let compaction = topic_cfg.effective_compaction(&config.compaction);
        let fq_topic = topic_cfg.fully_qualified_topic();

        info!(
            topic = %fq_topic,
            table_name = %topic_cfg.table_name,
            target_size_mb = compaction.target_parquet_size_mb,
            flush_interval_s = compaction.max_flush_interval_seconds,
            "spawning topic worker"
        );

        // Each worker gets its own SchemaResolver (with its own cache)
        let resolver = schema_resolver::SchemaResolver::new(danube_client.clone());

        let worker = worker::TopicWorker::new(
            topic_cfg,
            compaction,
            storage.clone(),
            config.storage.output_prefix.clone(),
            config.broker.address.clone(),
            config.polling.interval_seconds,
            iceberg_catalog.clone(),
            resolver,
        );

        let rx = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            worker.run(rx).await;
        }));
    }

    info!(
        workers = handles.len(),
        "all topic workers started, waiting for shutdown signal"
    );

    // Wait for SIGTERM/SIGINT
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received, stopping workers...");

    // Signal all workers to stop
    let _ = shutdown_tx.send(true);

    // Wait for all workers to finish (they flush remaining buffers)
    for handle in handles {
        let _ = handle.await;
    }

    info!("danube-iceberg stopped gracefully");
    Ok(())
}
