/// Valkey/Redis integration for the Danube write buffer.
///
/// Submodules:
/// - [`client`]             — Async Valkey/Redis client (standalone + cluster)
/// - [`config`]             — `WriteBufferConfig` and related types
/// - [`recovery`]           — Crash recovery: download active_segment for WAL replay
/// - [`rotation_listener`]  — Background task: clean up Valkey on WAL rotation
/// - [`stream_reader`]      — Warm-tier read path via XRANGE

mod client;
pub mod config;
pub mod recovery;
pub mod rotation_listener;
pub mod stream_reader;

pub use client::{ValkeyClient, WaitResult};
