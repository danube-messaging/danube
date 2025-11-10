use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use base64::{engine::general_purpose::STANDARD, Engine};

use crate::app::{AppState, CacheEntry};
use crate::ui::shared::resolve_metrics_endpoint;
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

    // Scrape the hosting broker (broker_id is expected to be present)
    let mut errors: Vec<String> = Vec::new();
    let brokers = match state.client.list_brokers().await {
        Ok(b) => b,
        Err(e) => {
            let (code, body) = crate::http::map_error(e);
            return (code, body).into_response();
        }
    };
    let host_br = if let Some(br) = brokers
        .brokers
        .iter()
        .find(|b| b.broker_id == desc.broker_id)
    {
        br
    } else {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("hosting broker {} not found", desc.broker_id)})),
        )
            .into_response();
    };
    let (host, port) = resolve_metrics_endpoint(&state, host_br);
    let (msg_in_total, producers, consumers) =
        match scrape_topic_metrics_for_host(&state, &host, port, &topic).await {
            Ok(vals) => vals,
            Err(e) => {
                errors.push(format!(
                    "metrics scrape failed for host broker {}: {}",
                    host_br.broker_id, e
                ));
                (0, 0, 0)
            }
        };

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

fn parse_topic_metrics(
    map: &std::collections::HashMap<String, Vec<(std::collections::HashMap<String, String>, f64)>>,
    topic: &str,
) -> (u64, u64, u64) {
    let mut msg_in_total: u64 = 0;
    let mut producers: u64 = 0;
    let mut consumers: u64 = 0;
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
    (msg_in_total, producers, consumers)
}

async fn scrape_topic_metrics_for_host(
    state: &AppState,
    host: &str,
    port: u16,
    topic: &str,
) -> anyhow::Result<(u64, u64, u64)> {
    let text = state.metrics.scrape_host_port(host, port).await?;
    let map = crate::metrics::parse_prometheus(&text);
    Ok(parse_topic_metrics(&map, topic))
}
