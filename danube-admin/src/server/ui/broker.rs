use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};

use crate::metrics::queries::{fetch_broker_metrics_for_ui, fetch_topics_by_broker};
use crate::server::app::{AppState, CacheEntry};
use danube_core::admin_proto::TopicInfo;
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
    pub delivery: String,
    pub producers_connected: u64,
    pub consumers_connected: u64,
    pub subscriptions: u64,
}

#[derive(Clone, Serialize)]
pub struct BrokerMetrics {
    pub rpc_total: u64,
    pub topics_owned: u64,
    pub inbound_bytes_total: u64,
    pub outbound_bytes_total: u64,
    pub errors_5xx_total: u64,
}

#[derive(Clone, Serialize)]
pub struct BrokerIdentity {
    pub broker_id: String,
    pub broker_addr: String,
    pub broker_role: String,
    pub broker_status: String,
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

    // Fetch authoritative topic list via gRPC, then enrich with metrics
    let mut errors = Vec::new();
    let req = danube_core::admin_proto::BrokerRequest {
        broker_id: broker_id.clone(),
    };
    let topic_infos: Vec<TopicInfo> = match state.client.list_broker_topics(req).await {
        Ok(resp) => resp.topics,
        Err(e) => {
            errors.push(format!("list_broker_topics failed: {}", e));
            Vec::new()
        }
    };

    // Query metrics filtered by the gRPC-provided topic names
    let (metrics, topics, scrape_err) =
        query_metrics_and_topics(&state, &broker_id, &topic_infos).await;

    if let Some(err) = scrape_err {
        errors.push(format!("metrics scrape failed: {}", err));
    }

    let broker_page = BrokerPage {
        timestamp: chrono::Utc::now().to_rfc3339(),
        broker: BrokerIdentity {
            broker_id,
            broker_addr: br.broker_addr.clone(),
            broker_role: br.broker_role.clone(),
            broker_status: br.broker_status.clone(),
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
    topic_infos: &[TopicInfo],
) -> (BrokerMetrics, Vec<BrokerTopicMini>, Option<String>) {
    let mut errors: Vec<String> = Vec::new();

    // Use authoritative topic list length for topics_owned
    let topics_owned: u64 = topic_infos.len() as u64;

    // Fetch broker-level metrics using shared query
    let (bytes, rpc_total, mut broker_errs) =
        fetch_broker_metrics_for_ui(&state.metrics, broker_id).await;
    errors.append(&mut broker_errs);

    // Fetch per-topic metrics using shared query
    let (topics_map, mut topic_errs) = fetch_topics_by_broker(&state.metrics, broker_id).await;
    errors.append(&mut topic_errs);

    // Build topics strictly from provided list; enrich with metrics counts
    let mut topics: Vec<BrokerTopicMini> = Vec::new();
    for info in topic_infos.iter() {
        let name = &info.name;
        let (p, c, s) = topics_map.get(name).copied().unwrap_or((0, 0, 0));
        topics.push(BrokerTopicMini {
            name: name.clone(),
            delivery: info.delivery.clone(),
            producers_connected: p,
            consumers_connected: c,
            subscriptions: s,
        });
    }

    let metrics = BrokerMetrics {
        rpc_total,
        topics_owned,
        inbound_bytes_total: bytes.bytes_in_total,
        outbound_bytes_total: bytes.bytes_out_total,
        errors_5xx_total: 0,
    };

    let err = if errors.is_empty() {
        None
    } else {
        Some(errors.join("; "))
    };

    (metrics, topics, err)
}
