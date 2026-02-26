//! `danube-raft` — Embedded Raft consensus layer for Danube.
//!
//! Replaces ETCD as the metadata store for the Danube Cloud cluster.
//! Uses `openraft` for Raft consensus and `redb` for persistent log/state storage.

pub mod commands;
pub mod leadership;
pub mod log_store;
pub mod network;
pub mod node;
pub mod raft_store;
pub mod server;
pub mod state_machine;
pub mod ttl_worker;
pub mod typ;

/// Re-export openraft types needed by downstream crates (e.g. danube-broker).
pub use openraft::{BasicNode, Raft};
