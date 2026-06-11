/// Valkey/Redis integration for the Danube write buffer.
///
/// Submodules:
/// - [`client`]             — Async Valkey/Redis client (standalone + cluster)
/// - [`config`]             — `WriteBufferConfig` and related types
/// - [`recovery`]           — Crash recovery: download active_segment for WAL replay
/// - [`rotation_listener`]  — Background task: clean up Valkey on WAL rotation
/// - [`segment_lifecycle`]  — Offset cleanup and (future) segment promotion/eviction

mod client;
pub mod config;
pub mod recovery;
pub mod rotation_listener;

pub use client::{ValkeyClient, WaitResult};
