//! Cluster-side edge replication service.
//!
//! Consolidates all edge ingestion behavior on the broker side:
//! - gRPC `EdgeReplicatorService` implementation (register, heartbeat, replicate)
//! - WAL batch ingestion and metadata management
//! - Per-edge state management in Raft
//!
//! Authentication is handled by the broker's standard gRPC interceptor.

pub(crate) mod edge_registry;
mod service;
mod storage;

pub(crate) use service::EdgeReplicatorServiceImpl;
pub(crate) use storage::EdgeReplicationStorage;
