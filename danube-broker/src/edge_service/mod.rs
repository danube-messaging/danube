//! Cluster-side edge replication service.
//!
//! Consolidates all edge ingestion behavior on the broker side:
//! - gRPC `EdgeReplicatorService` implementation
//! - WAL batch ingestion and metadata management
//!
//! Authentication is handled by the broker's standard gRPC interceptor.

mod service;
mod storage;

pub(crate) use service::EdgeReplicatorServiceImpl;
pub(crate) use storage::EdgeReplicationStorage;
