//! MQTT gateway for Danube Edge.
//!
//! Provides a high-performance MQTT ingestion pipeline that accepts IoT
//! device connections, maps MQTT topics to Danube topics, and batches
//! messages for efficient WAL ingestion.
//!
//! # Architecture
//!
//! ```text
//! IoT Device ──MQTT──→ server.rs (TCP :1883)
//!                          ↓
//!                     session.rs (per-connection state machine)
//!                          ↓ MqttPacket::Publish
//!                     bridge.rs (topic mapping + StreamMessage)
//!                          ↓ StreamMessage
//!                     ingester.rs (per-topic batching)
//!                          ↓ Vec<StreamMessage>
//!                     WAL append_batch() → EdgeReplicator → Cloud
//! ```

pub mod bridge;
pub mod config;
pub mod ingester;
pub mod server;
pub mod session;
