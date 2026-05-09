//! Edge services for Danube messaging.
//!
//! This crate provides the complete **edge-side** runtime:
//!
//! - **`config`** — Unified configuration (edge identity, replicator, MQTT)
//! - **`edge_service`** — Orchestrator that owns replicator + MQTT gateway
//! - **`replicator`** — Background WAL tailing, batching, and cloud replication
//! - **`mqtt`** — MQTT device ingestion (TCP server, session, topic mapping, batched WAL writes)

pub mod config;
pub mod edge_service;
pub mod mqtt;
pub mod replicator;
