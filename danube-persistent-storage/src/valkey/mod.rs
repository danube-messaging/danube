/// Valkey/Redis integration for the Danube write buffer.
///
/// Submodules:
/// - [`client`]             — Async Valkey/Redis client (standalone + cluster)
/// - [`config`]             — `WriteBufferConfig` and related types
/// - [`recovery`]           — Crash recovery: download active_segment for WAL replay
/// - [`segment_lifecycle`]  — Post-export cleanup, segment promotion, eviction

mod client;
pub mod config;
pub mod recovery;
pub mod segment_lifecycle;

pub use client::{ValkeyClient, WaitResult};
