use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};

use crate::metrics::queries::{
    fetch_latency_percentiles, fetch_message_size, fetch_producer_quality, fetch_reliable_metrics,
    fetch_topic_active, fetch_topic_core, fetch_topic_rates,
};
use crate::server::app::{AppState, CacheEntry};
use serde::Serialize;

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
    pub schema_subject: Option<String>,
    pub schema_id: Option<u64>,
    pub schema_version: Option<u32>,
    pub schema_type: Option<String>,
    pub compatibility_mode: Option<String>,
    pub subscriptions: Vec<SubscriptionDetail>,
}

#[derive(Debug, Serialize, Clone)]
pub struct SubscriptionDetail {
    pub name: String,
    pub subscription_type: String,
    pub failure_policy: Option<SubscriptionFailurePolicy>,
}

#[derive(Debug, Serialize, Clone)]
pub struct SubscriptionFailurePolicy {
    pub max_redelivery_count: u32,
    pub ack_timeout_ms: u64,
    pub base_redelivery_delay_ms: u64,
    pub max_redelivery_delay_ms: u64,
    pub backoff_strategy: String, // "fixed" | "exponential"
    pub dead_letter_topic: Option<String>,
    pub poison_policy: String, // "dead_letter" | "block" | "drop"
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
    pub wal_flush_total: u64,
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

    let mut subscriptions = Vec::new();
    let mut seen_subs = std::collections::HashSet::new();
    for sub_name in subs.subscriptions {
        if sub_name == "cursor" || sub_name == "failure_policy" {
            continue;
        }
        if !seen_subs.insert(sub_name.clone()) {
            continue;
        }
        let req = danube_core::admin_proto::GetSubscriptionFailurePolicyRequest {
            topic: topic.clone(),
            subscription: sub_name.clone(),
        };
        let (failure_policy, sub_type) = match state.client.get_subscription_failure_policy(req).await {
            Ok(res) => {
                let fp = res.failure_policy.map(|fp| {
                    let backoff_strategy = match fp.backoff_strategy {
                        1 => "exponential".to_string(),
                        _ => "fixed".to_string(),
                    };
                    let poison_policy = match fp.poison_policy {
                        0 => "dead_letter".to_string(),
                        1 => "block".to_string(),
                        2 => "drop".to_string(),
                        _ => "unknown".to_string(),
                    };
                    SubscriptionFailurePolicy {
                        max_redelivery_count: fp.max_redelivery_count,
                        ack_timeout_ms: fp.ack_timeout_ms,
                        base_redelivery_delay_ms: fp.base_redelivery_delay_ms,
                        max_redelivery_delay_ms: fp.max_redelivery_delay_ms,
                        backoff_strategy,
                        dead_letter_topic: fp.dead_letter_topic,
                        poison_policy,
                    }
                });
                let st = match res.subscription_type {
                    1 => "Shared".to_string(),
                    2 => "Failover".to_string(),
                    3 => "KeyShared".to_string(),
                    _ => "Exclusive".to_string(),
                };
                (fp, st)
            }
            Err(_) => (None, "Exclusive".to_string()),
        };
        subscriptions.push(SubscriptionDetail {
            name: sub_name,
            subscription_type: sub_type,
            failure_policy,
        });
    }

    let topic_dto = Topic {
        name: desc.name,
        schema_subject: desc.schema_subject,
        schema_id: desc.schema_id,
        schema_version: desc.schema_version,
        schema_type: desc.schema_type,
        compatibility_mode: desc.compatibility_mode,
        subscriptions,
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
        wal_flush_total: r.wal_flush_total,
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
