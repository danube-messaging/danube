use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use base64::{engine::general_purpose::STANDARD, Engine};

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
    pub type_schema: i32,        // DEPRECATED: 0 = no schema, 1 = has schema
    pub schema_data: String,     // DEPRECATED: base64-encoded JSON with schema info
    
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
        type_schema,       // TODO: Remove once UI updated
        schema_data,       // TODO: Remove once UI updated
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
    let req = danube_core::admin_proto::DescribeTopicRequest { name: topic.to_string() };
    let desc = state.client.describe_topic(req).await?;
    let req = danube_core::admin_proto::TopicRequest { name: topic.to_string() };
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

    let q_msg_in_total = format!("sum(danube_topic_messages_in_total{{topic=\"{}\"}})", topic);
    let q_msg_out_total = format!(
        "sum(danube_consumer_messages_out_total{{topic=\"{}\"}})",
        topic
    );
    let q_bytes_in_total = format!("sum(danube_topic_bytes_in_total{{topic=\"{}\"}})", topic);
    let q_bytes_out_total = format!(
        "sum(danube_consumer_bytes_out_total{{topic=\"{}\"}})",
        topic
    );

    let q_producers = format!("sum(danube_topic_active_producers{{topic=\"{}\"}})", topic);
    let q_consumers = format!("sum(danube_topic_active_consumers{{topic=\"{}\"}})", topic);
    let q_subscriptions = format!(
        "sum(danube_topic_active_subscriptions{{topic=\"{}\"}})",
        topic
    );

    let q_publish_rate_1m = format!(
        "sum(rate(danube_topic_messages_in_total{{topic=\"{}\"}}[1m]))",
        topic
    );
    let q_dispatch_rate_1m = format!(
        "sum(rate(danube_consumer_messages_out_total{{topic=\"{}\"}}[1m]))",
        topic
    );

    let q_send_ok_total = format!(
        "sum(danube_producer_send_total{{topic=\"{}\",result=\"ok\"}})",
        topic
    );
    let q_send_error_total = format!(
        "sum(danube_producer_send_total{{topic=\"{}\",result=\"error\"}})",
        topic
    );
    let q_send_error_by_code = format!(
        "sum by (error_code) (danube_producer_send_total{{topic=\"{}\",result=\"error\"}})",
        topic
    );

    let q_latency_p50 = format!(
        "danube_producer_send_latency_ms{{topic=\"{}\",quantile=\"0.5\"}}",
        topic
    );
    let q_latency_p95 = format!(
        "danube_producer_send_latency_ms{{topic=\"{}\",quantile=\"0.95\"}}",
        topic
    );
    let q_latency_p99 = format!(
        "danube_producer_send_latency_ms{{topic=\"{}\",quantile=\"0.99\"}}",
        topic
    );
    let q_size_sum = format!("danube_topic_message_size_bytes_sum{{topic=\"{}\"}}", topic);
    let q_size_count = format!(
        "danube_topic_message_size_bytes_count{{topic=\"{}\"}}",
        topic
    );

    let core = CoreCounters {
        msg_in_total: sum_u64(
            state.metrics.query_instant(&q_msg_in_total).await,
            &mut errors,
        ),
        msg_out_total: sum_u64(
            state.metrics.query_instant(&q_msg_out_total).await,
            &mut errors,
        ),
        bytes_in_total: sum_u64(
            state.metrics.query_instant(&q_bytes_in_total).await,
            &mut errors,
        ),
        bytes_out_total: sum_u64(
            state.metrics.query_instant(&q_bytes_out_total).await,
            &mut errors,
        ),
    };

    let active = ActiveEntities {
        producers: sum_u64(state.metrics.query_instant(&q_producers).await, &mut errors),
        consumers: sum_u64(state.metrics.query_instant(&q_consumers).await, &mut errors),
        subscriptions: sum_u64(
            state.metrics.query_instant(&q_subscriptions).await,
            &mut errors,
        ),
    };

    let rates = Rates {
        publish_rate_1m: sum_f64(
            state.metrics.query_instant(&q_publish_rate_1m).await,
            &mut errors,
        ),
        dispatch_rate_1m: sum_f64(
            state.metrics.query_instant(&q_dispatch_rate_1m).await,
            &mut errors,
        ),
    };

    let quality_send_ok = sum_u64(
        state.metrics.query_instant(&q_send_ok_total).await,
        &mut errors,
    );
    let quality_send_err = sum_u64(
        state.metrics.query_instant(&q_send_error_total).await,
        &mut errors,
    );
    let mut send_error_by_code: Vec<(String, u64)> = Vec::new();
    match state.metrics.query_instant(&q_send_error_by_code).await {
        Ok(resp) => {
            for r in resp.data.result.iter() {
                if let Some(code) = r.metric.get("error_code") {
                    if let Ok(v) = r.value.1.parse::<f64>() {
                        send_error_by_code.push((code.clone(), v as u64));
                    }
                }
            }
        }
        Err(e) => errors.push(format!("send_error_by_code query failed: {}", e)),
    }
    let quality = ProducerQuality {
        send_ok_total: quality_send_ok,
        send_error_total: quality_send_err,
        send_error_by_code,
    };

    let p50 = one_f64(
        state.metrics.query_instant(&q_latency_p50).await,
        &mut errors,
    );
    let p95 = one_f64(
        state.metrics.query_instant(&q_latency_p95).await,
        &mut errors,
    );
    let p99 = one_f64(
        state.metrics.query_instant(&q_latency_p99).await,
        &mut errors,
    );
    let size_sum = one_f64(state.metrics.query_instant(&q_size_sum).await, &mut errors);
    let size_count = one_f64(
        state.metrics.query_instant(&q_size_count).await,
        &mut errors,
    );
    let avg = if size_count > 0.0 {
        size_sum / size_count
    } else {
        0.0
    };
    let latency_size = LatencyAndSize {
        send_latency_ms_p50: p50,
        send_latency_ms_p95: p95,
        send_latency_ms_p99: p99,
        msg_size_bytes_avg: avg,
    };

    let reliable = build_reliable_metrics(state, topic, &mut errors).await;

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

fn sum_u64(resp: anyhow::Result<crate::server::metrics::PromResponse>, errors: &mut Vec<String>) -> u64 {
    match resp {
        Ok(r) => r
            .data
            .result
            .iter()
            .filter_map(|e| e.value.1.parse::<f64>().ok())
            .map(|v| v as u64)
            .sum(),
        Err(e) => {
            errors.push(e.to_string());
            0
        }
    }
}

fn sum_f64(resp: anyhow::Result<crate::server::metrics::PromResponse>, errors: &mut Vec<String>) -> f64 {
    match resp {
        Ok(r) => r
            .data
            .result
            .iter()
            .filter_map(|e| e.value.1.parse::<f64>().ok())
            .sum(),
        Err(e) => {
            errors.push(e.to_string());
            0.0
        }
    }
}

fn one_f64(resp: anyhow::Result<crate::server::metrics::PromResponse>, errors: &mut Vec<String>) -> f64 {
    match resp {
        Ok(r) => r
            .data
            .result
            .iter()
            .filter_map(|e| e.value.1.parse::<f64>().ok())
            .next()
            .unwrap_or(0.0),
        Err(e) => {
            errors.push(e.to_string());
            0.0
        }
    }
}

async fn build_reliable_metrics(
    state: &AppState,
    topic: &str,
    errors: &mut Vec<String>,
) -> Option<ReliableTopicMetrics> {
    let q_wal_app_total = format!("sum(danube_wal_append_total{{topic=\"{}\"}})", topic);
    let q_wal_app_bytes = format!("sum(danube_wal_append_bytes_total{{topic=\"{}\"}})", topic);
    let q_wal_fsync_total = format!("sum(danube_wal_fsync_total{{topic=\"{}\"}})", topic);
    let q_wal_p50 = format!(
        "danube_wal_flush_latency_ms{{topic=\"{}\",quantile=\"0.5\"}}",
        topic
    );
    let q_wal_p95 = format!(
        "danube_wal_flush_latency_ms{{topic=\"{}\",quantile=\"0.95\"}}",
        topic
    );
    let q_wal_p99 = format!(
        "danube_wal_flush_latency_ms{{topic=\"{}\",quantile=\"0.99\"}}",
        topic
    );
    let q_cloud_bytes = format!(
        "sum(danube_cloud_upload_bytes_total{{topic=\"{}\"}})",
        topic
    );
    let q_cloud_objects = format!(
        "sum(danube_cloud_upload_objects_total{{topic=\"{}\",result=\"ok\"}})",
        topic
    );

    // Determine if reliable by presence of any WAL metric
    let wal_append_total = sum_u64(state.metrics.query_instant(&q_wal_app_total).await, errors);
    if wal_append_total == 0
        && one_f64(state.metrics.query_instant(&q_wal_p50).await, errors) == 0.0
        && one_f64(state.metrics.query_instant(&q_wal_p95).await, errors) == 0.0
        && one_f64(state.metrics.query_instant(&q_wal_p99).await, errors) == 0.0
    {
        return None;
    }

    let wal_append_bytes_total =
        sum_u64(state.metrics.query_instant(&q_wal_app_bytes).await, errors);
    let wal_fsync_total = sum_u64(
        state.metrics.query_instant(&q_wal_fsync_total).await,
        errors,
    );
    let wal_flush_latency_ms_p50 = one_f64(state.metrics.query_instant(&q_wal_p50).await, errors);
    let wal_flush_latency_ms_p95 = one_f64(state.metrics.query_instant(&q_wal_p95).await, errors);
    let wal_flush_latency_ms_p99 = one_f64(state.metrics.query_instant(&q_wal_p99).await, errors);
    let cloud_upload_bytes_total =
        sum_u64(state.metrics.query_instant(&q_cloud_bytes).await, errors);
    let cloud_upload_objects_total =
        sum_u64(state.metrics.query_instant(&q_cloud_objects).await, errors);

    Some(ReliableTopicMetrics {
        wal_append_total,
        wal_append_bytes_total,
        wal_fsync_total,
        wal_flush_latency_ms_p50,
        wal_flush_latency_ms_p95,
        wal_flush_latency_ms_p99,
        cloud_upload_bytes_total,
        cloud_upload_objects_total,
    })
}
