//! Edge replication for Danube messaging.
//!
//! This crate provides the **edge-side** replication pipeline:
//! - Background replicator that tails local WAL, batches messages,
//!   and streams them to the cloud cluster via gRPC.
//! - Cluster client for connecting to the cloud broker's `EdgeReplicatorService`.
//! - Per-topic WAL tailer and checkpointing.
//!
//! The cluster-side gRPC service (which receives edge data) lives in
//! `danube-broker/src/edge_service/`.

pub mod edge;
pub mod mqtt;
