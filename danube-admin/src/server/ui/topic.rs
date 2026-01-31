use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use base64::{engine::general_purpose::STANDARD, Engine};

use crate::metrics::queries::{
    fetch_latency_percentiles, fetch_message_size, fetch_producer_quality, fetch_reliable_metrics,
    fetch_topic_active, fetch_topic_core, fetch_topic_rates,
};
use crate::server::app::{AppState, CacheEntry};
use serde::Serialize;
use serde_json;

#[derive(Clone, Serialize)]
pub struct TopicPage {
    pub timestamp: String,
    pub topic: Topic,
    pub metrics: TopicMetrics,
    pub errors: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct Topic {
    pub name: String,

    // TODO: Remove once UI is updated to use schema registry fields
    // Old UI expects type_schema (0=none, 1=schema) and encoded schema_data
    pub type_schema: i32,    // DEPRECATED: 0 = no schema, 1 = has schema
    pub schema_data: String, // DEPRECATED: base64-encoded JSON with schema info

    // TODO: Add once UI is updated:
    // pub schema_subject: Option<String>,
    // pub schema_id: Option<i32>,
    // pub schema_version: Option<i32>,
    // pub schema_type: Option<String>,
    // pub compatibility_mode: Option<String>,
    pub subscriptions: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct TopicMetrics {
    pub core: CoreCounters,
    pub active: ActiveEntities,
    pub rates: Rates,
    pub quality: ProducerQuality,
    pub latency_size: LatencyAndSize,
    pub reliable: Option<ReliableTopicMetrics>,
}

#[derive(Clone, Serialize)]
pub struct CoreCounters {
    pub msg_in_total: u64,
    pub msg_out_total: u64,
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
}

#[derive(Clone, Serialize)]
pub struct ActiveEntities {
    pub producers: u64,
    pub consumers: u64,
    pub subscriptions: u64,
}

#[derive(Clone, Serialize)]
pub struct Rates {
    pub publish_rate_1m: f64,
    pub dispatch_rate_1m: f64,
}

#[derive(Clone, Serialize)]
pub struct ProducerQuality {
    pub send_ok_total: u64,
    pub send_error_total: u64,
    pub send_error_by_code: Vec<(String, u64)>,
}

#[derive(Clone, Serialize)]
pub struct LatencyAndSize {
    pub send_latency_ms_p50: f64,
    pub send_latency_ms_p95: f64,
    pub send_latency_ms_p99: f64,
    pub msg_size_bytes_avg: f64,
}

#[derive(Clone, Serialize)]
pub struct ReliableTopicMetrics {
    pub wal_append_total: u64,
    pub wal_append_bytes_total: u64,
    pub wal_fsync_total: u64,
    pub wal_flush_latency_ms_p50: f64,
    pub wal_flush_latency_ms_p95: f64,
    pub wal_flush_latency_ms_p99: f64,
    pub cloud_upload_bytes_total: u64,
    pub cloud_upload_objects_total: u64,
}

pub async fn topic_page(
    Path(topic): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // Cache
    {
        let cache = state.topic_page_cache.lock().await;
        if let Some(entry) = cache.get(&topic) {
            if entry.expires_at > Instant::now() {
                return Json(entry.value.clone()).into_response();
            }
        }
        drop(cache);
    }

    // Core data via gRPC
    let (desc, subs) = match fetch_core_topic_data(&state, &topic).await {
        Ok(v) => v,
        Err(e) => {
            let (code, body) = crate::server::http::map_error(e);
            return (code, body).into_response();
        }
    };

    // Query Prometheus for topic metrics
    let mut errors: Vec<String> = Vec::new();
    let (metrics, mut q_errors) = query_topic_metrics(&state, &topic)
        .await
        .unwrap_or_else(|e| {
            (
                empty_metrics(),
                vec![format!(
                    "prometheus query failed for topic {}: {}",
                    topic, e
                )],
            )
        });
    errors.append(&mut q_errors);

    // TODO: Remove this adaptation block once UI is updated
    // Map new schema registry fields to old format for backward compatibility
    // Web UI will be updated later to display schema registry information
    let (type_schema, schema_data) = if let Some(subject) = desc.schema_subject {
        // Topic has a schema from registry - encode info as JSON for potential debugging
        let schema_info = serde_json::json!({
            "schema_subject": subject,
            "schema_id": desc.schema_id,
            "schema_version": desc.schema_version,
            "schema_type": desc.schema_type,
            "compatibility_mode": desc.compatibility_mode,
        });
        (1, STANDARD.encode(schema_info.to_string().as_bytes()))
    } else {
        // No schema attached
        (0, String::new())
    };

    let topic_dto = Topic {
        name: desc.name,
        type_schema, // TODO: Remove once UI updated
        schema_data, // TODO: Remove once UI updated
        // TODO: Add once UI updated:
        // schema_subject: desc.schema_subject,
        // schema_id: desc.schema_id,
        // schema_version: desc.schema_version,
        // schema_type: desc.schema_type,
        // compatibility_mode: desc.compatibility_mode,
        subscriptions: subs.subscriptions,
    };
    let dto = TopicPage {
        timestamp: chrono::Utc::now().to_rfc3339(),
        topic: topic_dto,
        metrics,
        errors,
    };

    let mut cache = state.topic_page_cache.lock().await;
    cache.insert(
        topic,
        CacheEntry {
            expires_at: Instant::now() + state.ttl,
            value: dto.clone(),
        },
    );

    Json(dto).into_response()
}

async fn fetch_core_topic_data(
    state: &AppState,
    topic: &str,
) -> anyhow::Result<(
    danube_core::admin_proto::DescribeTopicResponse,
    danube_core::admin_proto::SubscriptionListResponse,
)> {
    let req = danube_core::admin_proto::DescribeTopicRequest {
        name: topic.to_string(),
    };
    let desc = state.client.describe_topic(req).await?;
    let req = danube_core::admin_proto::TopicRequest {
        name: topic.to_string(),
    };
    let subs = state.client.list_subscriptions(req).await?;
    Ok((desc, subs))
}

fn empty_metrics() -> TopicMetrics {
    TopicMetrics {
        core: CoreCounters {
            msg_in_total: 0,
            msg_out_total: 0,
            bytes_in_total: 0,
            bytes_out_total: 0,
        },
        active: ActiveEntities {
            producers: 0,
            consumers: 0,
            subscriptions: 0,
        },
        rates: Rates {
            publish_rate_1m: 0.0,
            dispatch_rate_1m: 0.0,
        },
        quality: ProducerQuality {
            send_ok_total: 0,
            send_error_total: 0,
            send_error_by_code: Vec::new(),
        },
        latency_size: LatencyAndSize {
            send_latency_ms_p50: 0.0,
            send_latency_ms_p95: 0.0,
            send_latency_ms_p99: 0.0,
            msg_size_bytes_avg: 0.0,
        },
        reliable: None,
    }
}

async fn query_topic_metrics(
    state: &AppState,
    topic: &str,
) -> anyhow::Result<(TopicMetrics, Vec<String>)> {
    let mut errors: Vec<String> = Vec::new();

    // Use modular query functions from metrics module
    let (core_data, mut core_errs) = fetch_topic_core(&state.metrics, topic).await;
    errors.append(&mut core_errs);

    let (active_data, mut active_errs) = fetch_topic_active(&state.metrics, topic).await;
    errors.append(&mut active_errs);

    let (rates_data, mut rates_errs) = fetch_topic_rates(&state.metrics, topic).await;
    errors.append(&mut rates_errs);

    let (quality_data, mut quality_errs) = fetch_producer_quality(&state.metrics, topic).await;
    errors.append(&mut quality_errs);

    let (latency_data, mut latency_errs) = fetch_latency_percentiles(&state.metrics, topic).await;
    errors.append(&mut latency_errs);

    let (size_data, mut size_errs) = fetch_message_size(&state.metrics, topic).await;
    errors.append(&mut size_errs);

    let (reliable_data, mut reliable_errs) = fetch_reliable_metrics(&state.metrics, topic).await;
    errors.append(&mut reliable_errs);

    // Map shared types to UI view models
    let core = CoreCounters {
        msg_in_total: core_data.messages_in_total,
        msg_out_total: core_data.messages_out_total,
        bytes_in_total: core_data.bytes_in_total,
        bytes_out_total: core_data.bytes_out_total,
    };

    let active = ActiveEntities {
        producers: active_data.producers,
        consumers: active_data.consumers,
        subscriptions: active_data.subscriptions,
    };

    let rates = Rates {
        publish_rate_1m: rates_data.publish_rate_1m,
        dispatch_rate_1m: rates_data.dispatch_rate_1m,
    };

    let quality = ProducerQuality {
        send_ok_total: quality_data.send_ok_total,
        send_error_total: quality_data.send_error_total,
        send_error_by_code: quality_data.send_error_by_code,
    };

    let latency_size = LatencyAndSize {
        send_latency_ms_p50: latency_data.p50_ms,
        send_latency_ms_p95: latency_data.p95_ms,
        send_latency_ms_p99: latency_data.p99_ms,
        msg_size_bytes_avg: size_data.avg_bytes,
    };

    let reliable = reliable_data.map(|r| ReliableTopicMetrics {
        wal_append_total: r.wal_append_total,
        wal_append_bytes_total: r.wal_append_bytes_total,
        wal_fsync_total: r.wal_fsync_total,
        wal_flush_latency_ms_p50: r.wal_flush_latency_p50_ms,
        wal_flush_latency_ms_p95: r.wal_flush_latency_p95_ms,
        wal_flush_latency_ms_p99: r.wal_flush_latency_p99_ms,
        cloud_upload_bytes_total: r.cloud_upload_bytes_total,
        cloud_upload_objects_total: r.cloud_upload_objects_total,
    });

    Ok((
        TopicMetrics {
            core,
            active,
            rates,
            quality,
            latency_size,
            reliable,
        },
        errors,
    ))
}
