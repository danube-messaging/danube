use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// High-level workload intent profiles that expand into explicit Raft topic
/// and default subscription settings so behavior never drifts across broker upgrades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StreamIntent {
    /// High-speed in-memory streaming (zero disk I/O): heartbeats, live token streaming, RPC.
    #[serde(
        alias = "telemetry_firehose",
        alias = "firehose",
        alias = "telemetry",
        alias = "ephemeral"
    )]
    Realtime,
    /// General reliable pub/sub events, microservice & agent event buses, lakehouse feeds.
    #[serde(
        alias = "agent_event_mesh",
        alias = "event_mesh",
        alias = "events",
        alias = "lakehouse_feed",
        alias = "lakehouse",
        alias = "custom"
    )]
    EventStream,
    /// Durable background job queues and long-running agent task queues with automatic DLQ.
    #[serde(alias = "agent_task_queue", alias = "tasks", alias = "work_queue")]
    TaskQueue,
    /// Immutable tool-call logs, compliance trails, and ordered state logs (blocks on poison rather than skipping to DLQ).
    #[serde(
        alias = "tool_invocation_log",
        alias = "tool_log",
        alias = "audit",
        alias = "state_log"
    )]
    AuditLog,
}

impl StreamIntent {
    /// Canonical snake_case identifier for this intent profile.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Realtime => "realtime",
            Self::EventStream => "event_stream",
            Self::TaskQueue => "task_queue",
            Self::AuditLog => "audit_log",
        }
    }
}

impl FromStr for StreamIntent {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "realtime" | "telemetry_firehose" | "firehose" | "telemetry" | "ephemeral" => {
                Ok(Self::Realtime)
            }
            "event_stream" | "agent_event_mesh" | "event_mesh" | "events" | "lakehouse_feed"
            | "lakehouse" | "custom" => Ok(Self::EventStream),
            "task_queue" | "agent_task_queue" | "tasks" | "work_queue" => Ok(Self::TaskQueue),
            "audit_log" | "tool_invocation_log" | "tool_log" | "audit" | "state_log" => {
                Ok(Self::AuditLog)
            }
            other => Err(format!(
                "unknown stream intent '{}' (expected: realtime, event_stream, task_queue, audit_log)",
                other
            )),
        }
    }
}

impl fmt::Display for StreamIntent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Concrete settings expanded from a `StreamIntent` and stored explicitly in Raft.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ExpandedStreamSettings {
    pub(crate) intent: StreamIntent,
    pub(crate) dispatch_strategy: ConfigDispatchStrategy,
    pub(crate) wal_sync_mode: String,
    pub(crate) max_retries: u32,
    pub(crate) backoff_ms: u64,
    pub(crate) dlq_suffix: Option<String>,
}

impl StreamIntent {
    /// Expands this profile into concrete, self-contained topic settings.
    pub(crate) fn expand(&self) -> ExpandedStreamSettings {
        match self {
            Self::Realtime => ExpandedStreamSettings {
                intent: *self,
                dispatch_strategy: ConfigDispatchStrategy::NonReliable,
                wal_sync_mode: "async".to_string(),
                max_retries: 0,
                backoff_ms: 0,
                dlq_suffix: None,
            },
            Self::EventStream => ExpandedStreamSettings {
                intent: *self,
                dispatch_strategy: ConfigDispatchStrategy::Reliable,
                wal_sync_mode: "os_cache_flush".to_string(),
                max_retries: 3,
                backoff_ms: 500,
                dlq_suffix: Some("-dlq".to_string()),
            },
            Self::TaskQueue => ExpandedStreamSettings {
                intent: *self,
                dispatch_strategy: ConfigDispatchStrategy::Reliable,
                wal_sync_mode: "fsync_batch".to_string(),
                max_retries: 5,
                backoff_ms: 1_000,
                dlq_suffix: Some("-dlq".to_string()),
            },
            Self::AuditLog => ExpandedStreamSettings {
                intent: *self,
                dispatch_strategy: ConfigDispatchStrategy::Reliable,
                wal_sync_mode: "fsync_batch".to_string(),
                max_retries: 10,
                backoff_ms: 500,
                dlq_suffix: None,
            },
        }
    }
}

impl ExpandedStreamSettings {
    /// Formats a human-readable summary of the implicit settings applied to `topic_name`.
    pub(crate) fn summary_for_topic(&self, topic_name: &str) -> String {
        match self.dispatch_strategy {
            ConfigDispatchStrategy::NonReliable => {
                format!("{} (delivery=non_reliable, in_memory)", self.intent.as_str())
            }
            ConfigDispatchStrategy::Reliable => match &self.dlq_suffix {
                Some(suffix) => format!(
                    "{} (delivery=reliable, wal_sync={}, max_retries={}, backoff_ms={}, poison_policy=dead_letter, dlq={}{})",
                    self.intent.as_str(),
                    self.wal_sync_mode,
                    self.max_retries,
                    self.backoff_ms,
                    topic_name,
                    suffix
                ),
                None => format!(
                    "{} (delivery=reliable, wal_sync={}, max_retries={}, backoff_ms={}, poison_policy=block, dlq=none)",
                    self.intent.as_str(),
                    self.wal_sync_mode,
                    self.max_retries,
                    self.backoff_ms
                ),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_and_legacy_stream_intents() {
        let cases = [
            ("realtime", StreamIntent::Realtime),
            ("telemetry_firehose", StreamIntent::Realtime),
            ("firehose", StreamIntent::Realtime),
            ("telemetry", StreamIntent::Realtime),
            ("ephemeral", StreamIntent::Realtime),
            ("event_stream", StreamIntent::EventStream),
            ("agent_event_mesh", StreamIntent::EventStream),
            ("event_mesh", StreamIntent::EventStream),
            ("events", StreamIntent::EventStream),
            ("lakehouse_feed", StreamIntent::EventStream),
            ("lakehouse", StreamIntent::EventStream),
            ("custom", StreamIntent::EventStream),
            ("task_queue", StreamIntent::TaskQueue),
            ("agent-task-queue", StreamIntent::TaskQueue),
            ("tasks", StreamIntent::TaskQueue),
            ("work_queue", StreamIntent::TaskQueue),
            ("audit_log", StreamIntent::AuditLog),
            ("tool_invocation_log", StreamIntent::AuditLog),
            ("tool_log", StreamIntent::AuditLog),
            ("audit", StreamIntent::AuditLog),
            ("state_log", StreamIntent::AuditLog),
        ];

        for (raw, expected) in cases {
            let parsed: StreamIntent = raw.parse().expect("valid intent");
            assert_eq!(parsed, expected, "failed parsing '{}'", raw);
            let expanded = parsed.expand();
            assert_eq!(expanded.intent, expected);
            if expected == StreamIntent::Realtime {
                assert_eq!(expanded.dispatch_strategy, ConfigDispatchStrategy::NonReliable);
            } else {
                assert_eq!(expanded.dispatch_strategy, ConfigDispatchStrategy::Reliable);
            }
        }
    }

    #[test]
    fn expands_four_distinct_profiles_and_formats_summary() {
        let rt = StreamIntent::Realtime.expand();
        assert_eq!(
            rt.summary_for_topic("/default/metrics"),
            "realtime (delivery=non_reliable, in_memory)"
        );

        let es = StreamIntent::EventStream.expand();
        assert_eq!(es.wal_sync_mode, "os_cache_flush");
        assert_eq!(es.max_retries, 3);
        assert_eq!(es.backoff_ms, 500);
        assert_eq!(es.dlq_suffix.as_deref(), Some("-dlq"));
        assert_eq!(
            es.summary_for_topic("/default/orders"),
            "event_stream (delivery=reliable, wal_sync=os_cache_flush, max_retries=3, backoff_ms=500, poison_policy=dead_letter, dlq=/default/orders-dlq)"
        );

        let tq = StreamIntent::TaskQueue.expand();
        assert_eq!(tq.wal_sync_mode, "fsync_batch");
        assert_eq!(tq.max_retries, 5);
        assert_eq!(tq.backoff_ms, 1_000);
        assert_eq!(tq.dlq_suffix.as_deref(), Some("-dlq"));
        assert_eq!(
            tq.summary_for_topic("/default/jobs"),
            "task_queue (delivery=reliable, wal_sync=fsync_batch, max_retries=5, backoff_ms=1000, poison_policy=dead_letter, dlq=/default/jobs-dlq)"
        );

        let al = StreamIntent::AuditLog.expand();
        assert_eq!(al.wal_sync_mode, "fsync_batch");
        assert_eq!(al.max_retries, 10);
        assert_eq!(al.backoff_ms, 500);
        assert_eq!(al.dlq_suffix, None);
        assert_eq!(
            al.summary_for_topic("/default/audit"),
            "audit_log (delivery=reliable, wal_sync=fsync_batch, max_retries=10, backoff_ms=500, poison_policy=block, dlq=none)"
        );
    }

    #[test]
    fn deserializes_legacy_raft_intent_json_and_roundtrips() {
        let legacy_json = serde_json::json!({
            "intent": "agent_task_queue",
            "dispatch_strategy": "Reliable",
            "default_partitions": 4,
            "retention_seconds": 604800,
            "retention_size_mb": 20480,
            "wal_sync_mode": "fsync_batch",
            "storage_tier": "cloud_tiered",
            "max_retries": 5,
            "backoff_ms": 1000,
            "dlq_suffix": "-dlq"
        });
        let deserialized: ExpandedStreamSettings =
            serde_json::from_value(legacy_json).expect("deserializes legacy Raft JSON");
        assert_eq!(deserialized.intent, StreamIntent::TaskQueue);
        assert_eq!(deserialized.wal_sync_mode, "fsync_batch");

        let canonical_json = serde_json::to_value(&deserialized).expect("serializes");
        let roundtrip: ExpandedStreamSettings =
            serde_json::from_value(canonical_json).expect("roundtrips");
        assert_eq!(roundtrip, deserialized);
    }
}
