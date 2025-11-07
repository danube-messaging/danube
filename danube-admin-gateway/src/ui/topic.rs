use std::sync::Arc;
use std::time::Instant;

use axum::{extract::{Path, State}, response::IntoResponse, Json};
use base64::{engine::general_purpose::STANDARD, Engine};

use crate::{AppState, CacheEntry};
use crate::dto::{TopicPageDto, TopicDto, TopicMetricsDto};

pub async fn topic_page(Path(topic): Path<String>, State(state): State<Arc<AppState>>) -> impl IntoResponse {
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
    let desc = match state.client.describe_topic(&topic).await {
        Ok(d) => d,
        Err(e) => {
            let (code, body) = crate::http::map_error(e);
            return (code, body).into_response();
        }
    };
    let subs = match state.client.list_subscriptions(&topic).await {
        Ok(s) => s,
        Err(e) => {
            let (code, body) = crate::http::map_error(e);
            return (code, body).into_response();
        }
    };

    let topic_dto = TopicDto {
        name: desc.name,
        type_schema: desc.type_schema,
        schema_data: STANDARD.encode(desc.schema_data),
        subscriptions: subs.subscriptions,
    };

    // Scrape topic metrics across all brokers; aggregate values for this topic label
    let mut errors: Vec<String> = Vec::new();
    let mut msg_in_total: u64 = 0;
    let mut producers: u64 = 0;
    let mut consumers: u64 = 0;

    let brokers = match state.client.list_brokers().await {
        Ok(b) => b,
        Err(e) => {
            let (code, body) = crate::http::map_error(e);
            return (code, body).into_response();
        }
    };

    for br in brokers.brokers.iter() {
        let host = br.broker_addr.split(':').next().unwrap_or(br.broker_addr.as_str());
        match state.metrics.scrape(host).await {
            Ok(text) => {
                let map = crate::metrics::parse_prometheus(&text);
                if let Some(series) = map.get("danube_topic_messages_in_total") {
                    for (labels, value) in series.iter() {
                        if labels.get("topic").map(|t| t == &topic).unwrap_or(false) {
                            msg_in_total += *value as u64;
                        }
                    }
                }
                if let Some(series) = map.get("danube_topic_active_producers") {
                    for (labels, value) in series.iter() {
                        if labels.get("topic").map(|t| t == &topic).unwrap_or(false) {
                            producers += *value as u64;
                        }
                    }
                }
                if let Some(series) = map.get("danube_topic_active_consumers") {
                    for (labels, value) in series.iter() {
                        if labels.get("topic").map(|t| t == &topic).unwrap_or(false) {
                            consumers += *value as u64;
                        }
                    }
                }
            }
            Err(e) => {
                errors.push(format!("metrics scrape failed for {}: {}", br.broker_id, e));
            }
        }
    }

    // Populate only the requested metrics; others remain zero for now
    let metrics = TopicMetricsDto {
        msg_in_total,
        msg_out_total: 0,
        msg_backlog: 0,
        storage_bytes: 0,
        producers,
        consumers,
        publish_rate_1m: 0.0,
        dispatch_rate_1m: 0.0,
    };

    let dto = TopicPageDto {
        timestamp: chrono::Utc::now().to_rfc3339(),
        topic: topic_dto,
        metrics,
        errors,
    };

    let mut cache = state.topic_page_cache.lock().await;
    cache.insert(topic, CacheEntry { expires_at: Instant::now() + state.ttl, value: dto.clone() });

    Json(dto).into_response()
}
