use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, response::IntoResponse, Json};
use serde::Serialize;

use crate::app::{AppState, CacheEntry};
use crate::ui::broker::{BrokerIdentity, BrokerTopicMini};
use crate::ui::shared::fetch_brokers;

#[derive(Clone, Serialize)]
pub struct TopicsResponse {
    pub timestamp: String,
    pub brokers: Vec<BrokerIdentity>,
    pub topics: Vec<BrokerTopicMini>,
    pub errors: Vec<String>,
}

pub async fn cluster_topics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    {
        let cache = state.topics_cache.lock().await;
        if let Some(entry) = cache.as_ref() {
            if entry.expires_at > Instant::now() {
                return Json(entry.value.clone()).into_response();
            }
        }
        drop(cache);
    }
    let mut errors: Vec<String> = Vec::new();

    // Fetch cluster brokers
    let brokers = match fetch_brokers(&state).await {
        Ok(b) => b,
        Err(e) => {
            let (code, body) = crate::http::map_error(e);
            return (code, body).into_response();
        }
    };

    let brokers_out: Vec<BrokerIdentity> = brokers
        .brokers
        .iter()
        .map(|b| BrokerIdentity {
            broker_id: b.broker_id.clone(),
            broker_addr: b.broker_addr.clone(),
            broker_role: b.broker_role.clone(),
            broker_status: b.broker_status.clone(),
        })
        .collect();

    // Build cluster-wide topics by aggregating producers/consumers/subscriptions per topic
    let q_prod = "danube_topic_active_producers".to_string();
    let q_cons = "danube_topic_active_consumers".to_string();
    let q_subs = "danube_topic_active_subscriptions".to_string();

    let mut prod_by_topic: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    match state.metrics.query_instant(&q_prod).await {
        Ok(resp) => {
            for r in resp.data.result.iter() {
                if let Some(t) = r.metric.get("topic") {
                    if let Ok(v) = r.value.1.parse::<f64>() {
                        *prod_by_topic.entry(t.clone()).or_insert(0) += v as u64;
                    }
                }
            }
        }
        Err(e) => errors.push(format!("producers query failed: {}", e)),
    }

    let mut cons_by_topic: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    match state.metrics.query_instant(&q_cons).await {
        Ok(resp) => {
            for r in resp.data.result.iter() {
                if let Some(t) = r.metric.get("topic") {
                    if let Ok(v) = r.value.1.parse::<f64>() {
                        *cons_by_topic.entry(t.clone()).or_insert(0) += v as u64;
                    }
                }
            }
        }
        Err(e) => errors.push(format!("consumers query failed: {}", e)),
    }

    let mut subs_by_topic: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    match state.metrics.query_instant(&q_subs).await {
        Ok(resp) => {
            for r in resp.data.result.iter() {
                if let Some(t) = r.metric.get("topic") {
                    if let Ok(v) = r.value.1.parse::<f64>() {
                        *subs_by_topic.entry(t.clone()).or_insert(0) += v as u64;
                    }
                }
            }
        }
        Err(e) => errors.push(format!("subscriptions query failed: {}", e)),
    }

    let mut topics: Vec<BrokerTopicMini> = Vec::new();
    let mut topic_names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    topic_names.extend(prod_by_topic.keys().cloned());
    topic_names.extend(cons_by_topic.keys().cloned());
    topic_names.extend(subs_by_topic.keys().cloned());

    for name in topic_names.into_iter() {
        let p = *prod_by_topic.get(&name).unwrap_or(&0);
        let c = *cons_by_topic.get(&name).unwrap_or(&0);
        let s = *subs_by_topic.get(&name).unwrap_or(&0);
        topics.push(BrokerTopicMini {
            name,
            producers_connected: p,
            consumers_connected: c,
            subscriptions: s,
        });
    }

    let dto = TopicsResponse {
        timestamp: chrono::Utc::now().to_rfc3339(),
        brokers: brokers_out,
        topics,
        errors,
    };

    let mut cache = state.topics_cache.lock().await;
    *cache = Some(CacheEntry {
        expires_at: Instant::now() + state.ttl,
        value: dto.clone(),
    });

    Json(dto).into_response()
}
