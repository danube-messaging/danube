//! Key-Shared dispatcher — supports KeyShared subscription type.
//!
//! Routes messages based on routing key hash to ensure per-key ordering
//! while allowing parallel dispatch of different keys.
//!
//! Architecture:
//! - `consumer_state`: hash-ring-based consumer selection with glob key filtering
//! - `in_flight_window`: multi-message ack tracking with contiguous cursor advancement
//! - `reliable`: dispatch loop (mirrors shared/reliable.rs structure)

pub(super) mod consumer_state;
pub(super) mod in_flight_window;
pub(super) mod reliable;
