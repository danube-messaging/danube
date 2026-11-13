//! Feature-gated lakehouse sink library (`danube-iceberg`) for embedding
//! Iceberg/Parquet segment conversion inside `danube` without starving the
//! broker's hot-path async runtime.

pub mod catalog;
pub mod checkpoint;
pub mod config;
pub mod iceberg_schema;
pub mod schema;
pub mod schema_resolver;
pub mod segment_reader;
pub mod storage;
pub mod table_manager;
pub mod worker;
pub mod writer;

use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tracing::info;

use crate::config::Config;

/// Default bounded worker thread pool size for Arrow/Parquet conversion.
pub const DEFAULT_LAKEHOUSE_WORKER_THREADS: usize = 2;

/// Default memory budget cap for in-flight conversion buffers (128 MiB).
pub const DEFAULT_LAKEHOUSE_MEMORY_CAP_BYTES: usize = 128 * 1024 * 1024;

/// Isolated Tokio runtime driving lakehouse export workers.
pub struct LakehouseRuntime {
    runtime: Runtime,
    shutdown_tx: watch::Sender<bool>,
    memory_cap_bytes: usize,
    worker_threads: usize,
}

impl LakehouseRuntime {
    /// Builds an isolated `danube-lakehouse` runtime and spawns workers for `config`.
    pub fn start(config: Config) -> anyhow::Result<Self> {
        Self::start_with_limits(
            config,
            DEFAULT_LAKEHOUSE_WORKER_THREADS,
            DEFAULT_LAKEHOUSE_MEMORY_CAP_BYTES,
        )
    }

    /// Builds an isolated `danube-lakehouse` runtime with explicit worker thread
    /// and buffer cap limits, spawning topic workers for `config`.
    pub fn start_with_limits(
        config: Config,
        worker_threads: usize,
        memory_cap_bytes: usize,
    ) -> anyhow::Result<Self> {
        let threads = worker_threads.max(1);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("danube-lakehouse")
            .enable_all()
            .build()?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        runtime.block_on(async move {
            let storage = Arc::new(storage::build_storage(&config.storage)?);
            let iceberg_catalog: Option<Arc<dyn iceberg::Catalog>> = if config.catalog.is_disabled() {
                None
            } else {
                Some(catalog::build_catalog(&config.catalog).await?)
            };

            let danube_client = danube_client::DanubeClient::builder()
                .service_url(&format!("http://{}", config.broker.address))
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("failed to build Danube client: {}", e))?;

            for topic_cfg in config.topics {
                let compaction = topic_cfg.effective_compaction(&config.compaction);
                let worker = worker::TopicWorker::new(
                    topic_cfg,
                    compaction,
                    storage.clone(),
                    config.storage.output_prefix.clone(),
                    danube_client.clone(),
                    config.polling.interval_seconds,
                    iceberg_catalog.clone(),
                );
                let rx = shutdown_rx.clone();
                tokio::spawn(async move {
                    worker.run(rx).await;
                });
            }
            Ok::<_, anyhow::Error>(())
        })?;

        info!(
            worker_threads = threads,
            memory_cap_bytes,
            "danube-lakehouse runtime started"
        );

        Ok(Self {
            runtime,
            shutdown_tx,
            memory_cap_bytes,
            worker_threads: threads,
        })
    }

    /// Creates an empty isolated `danube-lakehouse` runtime (useful for tests or delayed attach).
    pub fn new_idle(worker_threads: usize, memory_cap_bytes: usize) -> anyhow::Result<Self> {
        let threads = worker_threads.max(1);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("danube-lakehouse")
            .enable_all()
            .build()?;
        let (shutdown_tx, _) = watch::channel(false);
        Ok(Self {
            runtime,
            shutdown_tx,
            memory_cap_bytes,
            worker_threads: threads,
        })
    }

    pub fn memory_cap_bytes(&self) -> usize {
        self.memory_cap_bytes
    }

    pub fn worker_threads(&self) -> usize {
        self.worker_threads
    }

    /// Signals workers to flush & exit and tears down the isolated runtime.
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        self.runtime.shutdown_timeout(std::time::Duration::from_secs(10));
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;

    #[test]
    fn builds_and_shuts_down_isolated_lakehouse_runtime() {
        let rt = LakehouseRuntime::new_idle(
            DEFAULT_LAKEHOUSE_WORKER_THREADS,
            DEFAULT_LAKEHOUSE_MEMORY_CAP_BYTES,
        )
        .unwrap();
        assert_eq!(rt.worker_threads(), 2);
        assert_eq!(rt.memory_cap_bytes(), 128 * 1024 * 1024);
        rt.shutdown();
    }
}
