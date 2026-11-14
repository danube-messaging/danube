pub mod config;
pub mod grpc_client;

pub use config::GrpcClientConfig;
pub use grpc_client::AdminGrpcClient;

/// Formats a client-side summary of the implicit settings applied by a `StreamIntent`
/// name or alias for display immediately after topic creation.
pub fn intent_summary_for_topic(raw_intent: &str, topic_name: &str) -> Option<String> {
    match raw_intent.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "realtime" | "telemetry_firehose" | "firehose" | "telemetry" | "ephemeral" => {
            Some("realtime (delivery=non_reliable, in_memory)".to_string())
        }
        "event_stream" | "agent_event_mesh" | "event_mesh" | "events" | "lakehouse_feed"
        | "lakehouse" | "custom" => Some(format!(
            "event_stream (delivery=reliable, wal_sync=os_cache_flush, max_retries=3, backoff_ms=500, poison_policy=dead_letter, dlq={}-dlq)",
            topic_name
        )),
        "task_queue" | "agent_task_queue" | "tasks" | "work_queue" => Some(format!(
            "task_queue (delivery=reliable, wal_sync=fsync_batch, max_retries=5, backoff_ms=1000, poison_policy=dead_letter, dlq={}-dlq)",
            topic_name
        )),
        "audit_log" | "tool_invocation_log" | "tool_log" | "audit" | "state_log" => Some(
            "audit_log (delivery=reliable, wal_sync=fsync_batch, max_retries=10, backoff_ms=500, poison_policy=block, dlq=none)".to_string(),
        ),
        _ => None,
    }
}
