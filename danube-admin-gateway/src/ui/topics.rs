use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, response::IntoResponse, Json};
use serde::Serialize;

use crate::app::{AppState, CacheEntry};
use crate::ui::broker::{BrokerIdentity, BrokerTopicMini};
use danube_core::admin_proto::TopicInfo;
use crate::ui::shared::fetch_brokers;

#[derive(Clone, Serialize)]
pub struct BrokerWithTopics {
    pub broker: BrokerIdentity,
    pub topics: Vec<BrokerTopicMini>,
}

#[derive(Clone, Serialize)]
pub struct TopicsResponse {
    pub timestamp: String,
    pub brokers: Vec<BrokerWithTopics>,
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

    // For each broker, fetch topics assigned to that broker using per-broker metrics
    let mut brokers_with_topics: Vec<BrokerWithTopics> = Vec::new();
    for b in brokers.brokers.iter() {
        let ident = BrokerIdentity {
            broker_id: b.broker_id.clone(),
            broker_addr: b.broker_addr.clone(),
            broker_role: b.broker_role.clone(),
            broker_status: b.broker_status.clone(),
        };
        match fetch_topics_for_broker(&state, &b.broker_id).await {
            Ok(v) => brokers_with_topics.push(BrokerWithTopics { broker: ident, topics: v }),
            Err(e) => {
                errors.push(format!("topics fetch failed for broker {}: {}", b.broker_id, e));
                brokers_with_topics.push(BrokerWithTopics { broker: ident, topics: Vec::new() });
            }
        }
    }

    let dto = TopicsResponse {
        timestamp: chrono::Utc::now().to_rfc3339(),
        brokers: brokers_with_topics,
        errors,
    };

    let mut cache = state.topics_cache.lock().await;
    *cache = Some(CacheEntry {
        expires_at: Instant::now() + state.ttl,
        value: dto.clone(),
    });

    Json(dto).into_response()
}

async fn fetch_topics_for_broker(state: &AppState, broker_id: &str) -> anyhow::Result<Vec<BrokerTopicMini>> {
    let q_prod = format!("danube_topic_active_producers{{broker=\"{}\"}}", broker_id);
    let q_cons = format!("danube_topic_active_consumers{{broker=\"{}\"}}", broker_id);
    let q_subs = format!("danube_topic_active_subscriptions{{broker=\"{}\"}}", broker_id);

    let mut prod_by_topic: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    match state.metrics.query_instant(&q_prod).await {
        Ok(resp) => {
            for r in resp.data.result.iter() {
                if let Some(t) = r.metric.get("topic") {
                    if let Ok(v) = r.value.1.parse::<f64>() {
                        prod_by_topic.insert(t.clone(), v as u64);
                    }
                }
            }
        }
        Err(e) => return Err(anyhow::anyhow!(format!("producers query failed: {}", e))),
    }

    let mut cons_by_topic: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    match state.metrics.query_instant(&q_cons).await {
        Ok(resp) => {
            for r in resp.data.result.iter() {
                if let Some(t) = r.metric.get("topic") {
                    if let Ok(v) = r.value.1.parse::<f64>() {
                        cons_by_topic.insert(t.clone(), v as u64);
                    }
                }
            }
        }
        Err(e) => return Err(anyhow::anyhow!(format!("consumers query failed: {}", e))),
    }

    let mut subs_by_topic: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    match state.metrics.query_instant(&q_subs).await {
        Ok(resp) => {
            for r in resp.data.result.iter() {
                if let Some(t) = r.metric.get("topic") {
                    if let Ok(v) = r.value.1.parse::<f64>() {
                        subs_by_topic.insert(t.clone(), v as u64);
                    }
                }
            }
        }
        Err(e) => return Err(anyhow::anyhow!(format!("subscriptions query failed: {}", e))),
    }

    // Fetch authoritative topic list via gRPC and enrich with metric counts
    let list = state.client.list_broker_topics(broker_id).await?;
    let mut infos: Vec<TopicInfo> = list.topics;
    infos.sort_by(|a, b| a.name.cmp(&b.name));
    let mut out: Vec<BrokerTopicMini> = Vec::new();
    for info in infos.into_iter() {
        let name = info.name.clone();
        let p = *prod_by_topic.get(&name).unwrap_or(&0);
        let c = *cons_by_topic.get(&name).unwrap_or(&0);
        let s = *subs_by_topic.get(&name).unwrap_or(&0);
        out.push(BrokerTopicMini {
            name,
            delivery: info.delivery,
            producers_connected: p,
            consumers_connected: c,
            subscriptions: s,
        });
    }
    Ok(out)
}
