//! Edge services for Danube messaging.
//!
//! This crate provides the complete **edge-side** runtime:
//!
//! - **`config`** — Unified configuration (edge identity, replicator, MQTT)
//! - **`edge_service`** — Orchestrator that owns replicator + MQTT gateway
//! - **`readiness`** — Per-topic readiness tracking (local, cluster, schema)
//! - **`replicator`** — Background WAL tailing, batching, and cloud replication
//! - **`mqtt`** — MQTT device ingestion (TCP server, session, topic mapping, batched WAL writes)

pub mod config;
pub mod edge_service;
pub mod mqtt;
pub mod readiness;
pub mod replicator;
