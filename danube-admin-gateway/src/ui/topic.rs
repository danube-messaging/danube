use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use base64::{engine::general_purpose::STANDARD, Engine};

use crate::app::{AppState, CacheEntry};
use serde::Serialize;

#[derive(Clone, Serialize)]
pub struct TopicPage {
    pub timestamp: String,
    pub topic: Topic,
    pub metrics: TopicMetrics,
    pub errors: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct Topic {
    pub name: String,
    pub type_schema: i32,
    pub schema_data: String,
    pub subscriptions: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct TopicMetrics {
    pub msg_in_total: u64,
    pub msg_out_total: u64,
    pub msg_backlog: u64,
    pub storage_bytes: u64,
    pub producers: u64,
    pub consumers: u64,
    pub publish_rate_1m: f64,
    pub dispatch_rate_1m: f64,
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
            let (code, body) = crate::http::map_error(e);
            return (code, body).into_response();
        }
    };

    // Scrape topic metrics across all brokers; aggregate values for this topic label
    let (msg_in_total, producers, consumers, errors) =
        scrape_topic_metrics_across_brokers(&state, &topic).await;

    // Populate only the requested metrics; others remain zero for now
    let metrics = TopicMetrics {
        msg_in_total,
        msg_out_total: 0,
        msg_backlog: 0,
        storage_bytes: 0,
        producers,
        consumers,
        publish_rate_1m: 0.0,
        dispatch_rate_1m: 0.0,
    };

    let topic_dto = Topic {
        name: desc.name,
        type_schema: desc.type_schema,
        schema_data: STANDARD.encode(desc.schema_data),
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
    let desc = state.client.describe_topic(topic).await?;
    let subs = state.client.list_subscriptions(topic).await?;
    Ok((desc, subs))
}

async fn scrape_topic_metrics_across_brokers(
    state: &AppState,
    topic: &str,
) -> (u64, u64, u64, Vec<String>) {
    let mut errors: Vec<String> = Vec::new();
    let mut msg_in_total: u64 = 0;
    let mut producers: u64 = 0;
    let mut consumers: u64 = 0;

    match state.client.list_brokers().await {
        Ok(brokers) => {
            for br in brokers.brokers.iter() {
                // Prefer metrics_addr if available else host from broker_addr
                let (host, port) = if !br.metrics_addr.is_empty() {
                    let maddr = br
                        .metrics_addr
                        .trim_start_matches("http://")
                        .trim_start_matches("https://");
                    let mut parts = maddr.split(':');
                    let host = parts.next().unwrap_or("localhost").to_string();
                    let port = parts
                        .next()
                        .and_then(|p| p.parse::<u16>().ok())
                        .unwrap_or(state.metrics.base_port());
                    (host, port)
                } else {
                    let addr = br
                        .broker_addr
                        .trim_start_matches("http://")
                        .trim_start_matches("https://");
                    let mut parts = addr.split(':');
                    let host = parts.next().unwrap_or("localhost").to_string();
                    (host, state.metrics.base_port())
                };

                match state.metrics.scrape_host_port(&host, port).await {
                    Ok(text) => {
                        let map = crate::metrics::parse_prometheus(&text);
                        if let Some(series) = map.get("danube_topic_messages_in_total") {
                            for (labels, value) in series.iter() {
                                if labels.get("topic").map(|t| t == topic).unwrap_or(false) {
                                    msg_in_total += *value as u64;
                                }
                            }
                        }
                        if let Some(series) = map.get("danube_topic_active_producers") {
                            for (labels, value) in series.iter() {
                                if labels.get("topic").map(|t| t == topic).unwrap_or(false) {
                                    producers += *value as u64;
                                }
                            }
                        }
                        if let Some(series) = map.get("danube_topic_active_consumers") {
                            for (labels, value) in series.iter() {
                                if labels.get("topic").map(|t| t == topic).unwrap_or(false) {
                                    consumers += *value as u64;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        errors.push(format!("metrics scrape failed for {}: {}", br.broker_id, e))
                    }
                }
            }
        }
        Err(e) => errors.push(format!("list_brokers failed: {}", e)),
    }

    (msg_in_total, producers, consumers, errors)
}
