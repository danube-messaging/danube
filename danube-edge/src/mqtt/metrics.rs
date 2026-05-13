//! MQTT gateway Prometheus metrics.
//!
//! All MQTT-related counters, gauges, and histograms are defined here.
//! Instrumented by `session.rs` (per-connection) and `ingester.rs` (batching).

use metrics::{counter, gauge, histogram};

// ---------------------------------------------------------------------------
// Connection metrics
// ---------------------------------------------------------------------------

/// Increment when a new MQTT session is established (after CONNACK).
pub fn connection_opened() {
    counter!("mqtt_connections_total").increment(1);
    gauge!("mqtt_connections_active").increment(1.0);
}

/// Decrement when an MQTT session ends (disconnect, timeout, error).
pub fn connection_closed() {
    gauge!("mqtt_connections_active").decrement(1.0);
}

/// Increment when a connection is rejected (max connections reached).
pub fn connection_rejected() {
    counter!("mqtt_connections_rejected_total").increment(1);
}

// ---------------------------------------------------------------------------
// Message metrics
// ---------------------------------------------------------------------------

/// Increment on each PUBLISH received from a device.
pub fn message_received(topic: &str) {
    counter!("mqtt_messages_received_total", "topic" => topic.to_string()).increment(1);
}

/// Increment when a message is dropped (validation failure, no mapping, etc.).
pub fn message_dropped(topic: &str, reason: &str) {
    counter!("mqtt_messages_dropped_total",
             "topic" => topic.to_string(),
             "reason" => reason.to_string())
    .increment(1);
}

// ---------------------------------------------------------------------------
// Ingester metrics
// ---------------------------------------------------------------------------

/// Record a batch flush to WAL.
pub fn batch_flushed(topic: &str, size: usize) {
    counter!("mqtt_ingester_batch_flushes_total", "topic" => topic.to_string()).increment(1);
    histogram!("mqtt_ingester_batch_size", "topic" => topic.to_string()).record(size as f64);
}

/// Record flush latency in milliseconds.
pub fn flush_latency_ms(topic: &str, duration_ms: f64) {
    histogram!("mqtt_ingester_flush_latency_ms", "topic" => topic.to_string()).record(duration_ms);
}
