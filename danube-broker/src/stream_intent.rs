use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// High-level workload intent profiles that expand into explicit Raft topic
/// and storage configuration so behavior never drifts across broker upgrades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StreamIntent {
    AgentEventMesh,
    AgentTaskQueue,
    ToolInvocationLog,
    TelemetryFirehose,
    LakehouseFeed,
    Custom,
}

impl FromStr for StreamIntent {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "agent_event_mesh" | "event_mesh" | "events" => Ok(Self::AgentEventMesh),
            "agent_task_queue" | "task_queue" | "tasks" => Ok(Self::AgentTaskQueue),
            "tool_invocation_log" | "tool_log" | "audit" => Ok(Self::ToolInvocationLog),
            "telemetry_firehose" | "firehose" | "telemetry" => Ok(Self::TelemetryFirehose),
            "lakehouse_feed" | "lakehouse" => Ok(Self::LakehouseFeed),
            "custom" => Ok(Self::Custom),
            other => Err(format!(
                "unknown stream intent '{}'; valid intents: agent_event_mesh, agent_task_queue, tool_invocation_log, telemetry_firehose, lakehouse_feed, custom",
                other
            )),
        }
    }
}

impl fmt::Display for StreamIntent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AgentEventMesh => write!(f, "agent_event_mesh"),
            Self::AgentTaskQueue => write!(f, "agent_task_queue"),
            Self::ToolInvocationLog => write!(f, "tool_invocation_log"),
            Self::TelemetryFirehose => write!(f, "telemetry_firehose"),
            Self::LakehouseFeed => write!(f, "lakehouse_feed"),
            Self::Custom => write!(f, "custom"),
        }
    }
}

/// Concrete settings expanded from a `StreamIntent` and stored explicitly in Raft.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ExpandedStreamSettings {
    pub(crate) intent: StreamIntent,
    pub(crate) dispatch_strategy: ConfigDispatchStrategy,
    pub(crate) default_partitions: u32,
    pub(crate) retention_seconds: u64,
    pub(crate) retention_size_mb: u64,
    pub(crate) wal_sync_mode: String,
    pub(crate) storage_tier: String,
    pub(crate) max_retries: u32,
    pub(crate) backoff_ms: u64,
    pub(crate) dlq_suffix: Option<String>,
}

impl StreamIntent {
    /// Expands this profile into concrete, self-contained topic settings.
    pub(crate) fn expand(&self) -> ExpandedStreamSettings {
        match self {
            Self::AgentEventMesh => ExpandedStreamSettings {
                intent: *self,
                dispatch_strategy: ConfigDispatchStrategy::Reliable,
                default_partitions: 1,
                retention_seconds: 86_400, // 24h
                retention_size_mb: 10_240,
                wal_sync_mode: "os_cache_flush".to_string(),
                storage_tier: "cloud_tiered".to_string(),
                max_retries: 3,
                backoff_ms: 500,
                dlq_suffix: Some("-dlq".to_string()),
            },
            Self::AgentTaskQueue => ExpandedStreamSettings {
                intent: *self,
                dispatch_strategy: ConfigDispatchStrategy::Reliable,
                default_partitions: 4,
                retention_seconds: 604_800, // 7d
                retention_size_mb: 20_480,
                wal_sync_mode: "fsync_batch".to_string(),
                storage_tier: "cloud_tiered".to_string(),
                max_retries: 5,
                backoff_ms: 1_000,
                dlq_suffix: Some("-dlq".to_string()),
            },
            Self::ToolInvocationLog => ExpandedStreamSettings {
                intent: *self,
                dispatch_strategy: ConfigDispatchStrategy::Reliable,
                default_partitions: 2,
                retention_seconds: 2_592_000, // 30d
                retention_size_mb: 51_200,
                wal_sync_mode: "fsync_batch".to_string(),
                storage_tier: "cloud_tiered".to_string(),
                max_retries: 10,
                backoff_ms: 500,
                dlq_suffix: None,
            },
            Self::TelemetryFirehose => ExpandedStreamSettings {
                intent: *self,
                dispatch_strategy: ConfigDispatchStrategy::NonReliable,
                default_partitions: 4,
                retention_seconds: 3_600, // 1h
                retention_size_mb: 4_096,
                wal_sync_mode: "async".to_string(),
                storage_tier: "local".to_string(),
                max_retries: 0,
                backoff_ms: 0,
                dlq_suffix: None,
            },
            Self::LakehouseFeed => ExpandedStreamSettings {
                intent: *self,
                dispatch_strategy: ConfigDispatchStrategy::Reliable,
                default_partitions: 2,
                retention_seconds: 604_800, // 7d
                retention_size_mb: 51_200,
                wal_sync_mode: "os_cache_flush".to_string(),
                storage_tier: "cloud_tiered".to_string(),
                max_retries: 5,
                backoff_ms: 1_000,
                dlq_suffix: Some("-dlq".to_string()),
            },
            Self::Custom => ExpandedStreamSettings {
                intent: *self,
                dispatch_strategy: ConfigDispatchStrategy::Reliable,
                default_partitions: 1,
                retention_seconds: 86_400,
                retention_size_mb: 10_240,
                wal_sync_mode: "os_cache_flush".to_string(),
                storage_tier: "local".to_string(),
                max_retries: 3,
                backoff_ms: 500,
                dlq_suffix: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_expands_all_stream_intents() {
        for (raw, expected) in [
            ("agent_event_mesh", StreamIntent::AgentEventMesh),
            ("agent-task-queue", StreamIntent::AgentTaskQueue),
            ("tool_invocation_log", StreamIntent::ToolInvocationLog),
            ("telemetry_firehose", StreamIntent::TelemetryFirehose),
            ("lakehouse_feed", StreamIntent::LakehouseFeed),
            ("custom", StreamIntent::Custom),
        ] {
            let parsed: StreamIntent = raw.parse().expect("valid intent");
            assert_eq!(parsed, expected);
            let expanded = parsed.expand();
            assert_eq!(expanded.intent, expected);
            if expected == StreamIntent::TelemetryFirehose {
                assert_eq!(expanded.dispatch_strategy, ConfigDispatchStrategy::NonReliable);
            } else {
                assert_eq!(expanded.dispatch_strategy, ConfigDispatchStrategy::Reliable);
            }
        }
    }
}
