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

    // Query Prometheus for topic metrics
    let mut errors: Vec<String> = Vec::new();
    let (msg_in_total, producers, consumers) = match query_topic_metrics(&state, &topic).await {
        Ok(vals) => vals,
        Err(e) => {
            errors.push(format!("prometheus query failed for topic {}: {}", topic, e));
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

async fn query_topic_metrics(state: &AppState, topic: &str) -> anyhow::Result<(u64, u64, u64)> {
    let q_in = format!(
        "sum(danube_topic_messages_in_total{{topic=\"{}\"}})",
        topic
    );
    let q_prod = format!(
        "sum(danube_topic_active_producers{{topic=\"{}\"}})",
        topic
    );
    let q_cons = format!(
        "sum(danube_topic_active_consumers{{topic=\"{}\"}})",
        topic
    );

    let msg_in_total: u64 = state
        .metrics
        .query_instant(&q_in)
        .await?
        .data
        .result
        .iter()
        .filter_map(|r| r.value.1.parse::<f64>().ok())
        .map(|v| v as u64)
        .sum();

    let producers: u64 = state
        .metrics
        .query_instant(&q_prod)
        .await?
        .data
        .result
        .iter()
        .filter_map(|r| r.value.1.parse::<f64>().ok())
        .map(|v| v as u64)
        .sum();

    let consumers: u64 = state
        .metrics
        .query_instant(&q_cons)
        .await?
        .data
        .result
        .iter()
        .filter_map(|r| r.value.1.parse::<f64>().ok())
        .map(|v| v as u64)
        .sum();

    Ok((msg_in_total, producers, consumers))
}
