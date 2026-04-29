//! Edge ↔ Cluster replication for Danube messaging.
//!
//! This crate provides both sides of the edge replication pipeline:
//!
//! - **`cluster`** — gRPC service hosted on cloud brokers that receives batches
//!   from edge brokers and writes directly to WAL via `append_batch()`.
//! - **`edge`** — background replicator that tails local WAL, batches messages,
//!   and streams them to the cloud cluster.

pub mod cluster;
pub mod edge;

/// Generated protobuf types and gRPC client/server for `EdgeReplicatorService`.
pub mod proto {
    include!("proto/danube.edge.rs");
}
