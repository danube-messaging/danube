use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};

use crate::app::{AppState, CacheEntry};
use serde::Serialize;

#[derive(Clone, Serialize)]
pub struct BrokerPage {
    pub timestamp: String,
    pub broker: BrokerIdentity,
    pub metrics: BrokerMetrics,
    pub topics: Vec<BrokerTopicMini>,
    pub errors: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct BrokerTopicMini {
    pub name: String,
    pub producers_connected: u64,
    pub consumers_connected: u64,
}

#[derive(Clone, Serialize)]
pub struct BrokerMetrics {
    pub rpc_total: u64,
    pub rpc_rate_1m: f64,
    pub topics_owned: u64,
    pub producers_connected: u64,
    pub consumers_connected: u64,
    pub inbound_bytes_total: u64,
    pub outbound_bytes_total: u64,
    pub errors_5xx_total: u64,
}

#[derive(Clone, Serialize)]
pub struct BrokerIdentity {
    pub broker_id: String,
    pub broker_addr: String,
    pub broker_role: String,
}

pub async fn broker_page(
    Path(broker_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // Cache
    {
        let cache = state.broker_page_cache.lock().await;
        if let Some(entry) = cache.get(&broker_id) {
            if entry.expires_at > Instant::now() {
                return Json(entry.value.clone()).into_response();
            }
        }
        drop(cache);
    }

    // Resolve broker identity
    let br = match fetch_target_broker(&state, &broker_id).await {
        Ok(v) => v,
        Err(e) => {
            return (
                axum::http::StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
        }
    };

    // Query metrics and topic list
    let (metrics, topics, scrape_err) = query_metrics_and_topics(&state, &broker_id).await;

    let mut errors = Vec::new();
    if let Some(err) = scrape_err {
        errors.push(format!("metrics scrape failed: {}", err));
    }

    let broker_page = BrokerPage {
        timestamp: chrono::Utc::now().to_rfc3339(),
        broker: BrokerIdentity {
            broker_id,
            broker_addr: br.broker_addr.clone(),
            broker_role: br.broker_role.clone(),
        },
        metrics,
        topics,
        errors,
    };

    let mut cache = state.broker_page_cache.lock().await;
    cache.insert(
        broker_page.broker.broker_id.clone(),
        CacheEntry {
            expires_at: Instant::now() + state.ttl,
            value: broker_page.clone(),
        },
    );

    Json(broker_page).into_response()
}

async fn fetch_target_broker(
    state: &AppState,
    broker_id: &str,
) -> anyhow::Result<danube_core::admin_proto::BrokerInfo> {
    let brokers = state.client.list_brokers().await?;
    for br in brokers.brokers.iter() {
        if br.broker_id == broker_id {
            return Ok(br.clone());
        }
    }
    Err(anyhow::anyhow!("unknown broker"))
}

async fn query_metrics_and_topics(
    state: &AppState,
    broker_id: &str,
) -> (BrokerMetrics, Vec<BrokerTopicMini>, Option<String>) {
    let q_topics_owned = format!("danube_broker_topics_owned{{broker=\"{}\"}}", broker_id);
    let q_rpc_total = format!("sum(danube_broker_rpc_total{{broker=\"{}\"}})", broker_id);
    let q_prod = format!("danube_topic_active_producers{{broker=\"{}\"}}", broker_id);
    let q_cons = format!("danube_topic_active_consumers{{broker=\"{}\"}}", broker_id);

    let mut errors: Vec<String> = Vec::new();

    let topics_owned: u64 = match state.metrics.query_instant(&q_topics_owned).await {
        Ok(resp) => resp
            .data
            .result
            .iter()
            .filter_map(|r| r.value.1.parse::<f64>().ok())
            .map(|v| v as u64)
            .sum(),
        Err(e) => {
            errors.push(format!("topics_owned query failed: {}", e));
            0
        }
    };

    let rpc_total: u64 = match state.metrics.query_instant(&q_rpc_total).await {
        Ok(resp) => resp
            .data
            .result
            .iter()
            .filter_map(|r| r.value.1.parse::<f64>().ok())
            .map(|v| v as u64)
            .sum(),
        Err(e) => {
            errors.push(format!("rpc_total query failed: {}", e));
            0
        }
    };

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
        Err(e) => errors.push(format!("producers query failed: {}", e)),
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
        Err(e) => errors.push(format!("consumers query failed: {}", e)),
    }

    let mut topics: Vec<BrokerTopicMini> = Vec::new();
    let mut producers_connected_sum = 0u64;
    let mut consumers_connected_sum = 0u64;
    let mut topic_names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    topic_names.extend(prod_by_topic.keys().cloned());
    topic_names.extend(cons_by_topic.keys().cloned());
    for name in topic_names.into_iter() {
        let p = *prod_by_topic.get(&name).unwrap_or(&0);
        let c = *cons_by_topic.get(&name).unwrap_or(&0);
        producers_connected_sum += p;
        consumers_connected_sum += c;
        topics.push(BrokerTopicMini {
            name,
            producers_connected: p,
            consumers_connected: c,
        });
    }

    let metrics = BrokerMetrics {
        rpc_total,
        rpc_rate_1m: 0.0,
        topics_owned,
        producers_connected: producers_connected_sum,
        consumers_connected: consumers_connected_sum,
        inbound_bytes_total: 0,
        outbound_bytes_total: 0,
        errors_5xx_total: 0,
    };

    let err = if errors.is_empty() {
        None
    } else {
        Some(errors.join("; "))
    };

    (metrics, topics, err)
}
