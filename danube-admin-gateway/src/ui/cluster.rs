use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, response::IntoResponse, Json};
use serde::Serialize;

use crate::app::{AppState, CacheEntry};
use crate::ui::shared::fetch_brokers;

#[derive(Clone, Serialize)]
pub struct ClusterPage {
    pub timestamp: String,
    pub brokers: Vec<ClusterBroker>,
    pub totals: ClusterTotals,
    pub errors: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct ClusterBrokerStats {
    pub topics_owned: u64,
    pub rpc_total: u64,
    pub rpc_rate_1m: f64,
    pub active_connections: u64,
    pub errors_5xx_total: u64,
}

#[derive(Clone, Serialize)]
pub struct ClusterBroker {
    pub broker_id: String,
    pub broker_addr: String,
    pub broker_role: String,
    pub stats: ClusterBrokerStats,
}

#[derive(Clone, Serialize)]
pub struct ClusterTotals {
    pub broker_count: u64,
    pub topics_total: u64,
    pub rpc_total: u64,
    pub active_connections: u64,
}

pub async fn cluster_page(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    {
        let cache = state.cluster_page_cache.lock().await;
        if let Some(entry) = cache.as_ref() {
            if entry.expires_at > Instant::now() {
                return Json(entry.value.clone()).into_response();
            }
        }
        drop(cache);
    }

    let mut errors: Vec<String> = Vec::new();
    let mut brokers_out: Vec<ClusterBroker> = Vec::new();
    let mut totals_topics = 0u64;
    let mut totals_rpc = 0u64;
    let mut totals_conns = 0u64;

    let brokers = match fetch_brokers(&state).await {
        Ok(b) => b,
        Err(e) => {
            let (code, body) = crate::http::map_error(e);
            return (code, body).into_response();
        }
    };

    for br in brokers.brokers.iter() {
        let (topics_owned, rpc_total) = match query_broker_metrics(&state, &br.broker_id).await {
            Ok(vals) => vals,
            Err(e) => {
                errors.push(format!("metrics scrape failed for {}: {}", br.broker_id, e));
                (0, 0)
            }
        };

        let stats = ClusterBrokerStats {
            topics_owned,
            rpc_total,
            rpc_rate_1m: 0.0,
            active_connections: 0,
            errors_5xx_total: 0,
        };
        totals_topics += stats.topics_owned;
        totals_rpc += stats.rpc_total;
        totals_conns += stats.active_connections;
        brokers_out.push(ClusterBroker {
            broker_id: br.broker_id.clone(),
            broker_addr: br.broker_addr.clone(),
            broker_role: br.broker_role.clone(),
            stats,
        });
    }

    let broker_count = brokers_out.len() as u64;

    let dto = ClusterPage {
        timestamp: chrono::Utc::now().to_rfc3339(),
        brokers: brokers_out,
        totals: ClusterTotals {
            broker_count,
            topics_total: totals_topics,
            rpc_total: totals_rpc,
            active_connections: totals_conns,
        },
        errors,
    };

    let mut cache = state.cluster_page_cache.lock().await;
    *cache = Some(CacheEntry {
        expires_at: Instant::now() + state.ttl,
        value: dto.clone(),
    });

    Json(dto).into_response()
}

// shared helpers imported from crate::ui::shared

async fn query_broker_metrics(state: &AppState, broker_id: &str) -> anyhow::Result<(u64, u64)> {
    let q_topics = format!("danube_broker_topics_owned{{broker=\"{}\"}}", broker_id);
    let q_rpc_total = format!("sum(danube_broker_rpc_total{{broker=\"{}\"}})", broker_id);

    let topics_resp = state.metrics.query_instant(&q_topics).await?;
    let mut topics_owned: u64 = 0;
    for r in topics_resp.data.result.iter() {
        if let Ok(v) = r.value.1.parse::<f64>() {
            topics_owned += v as u64;
        }
    }

    let rpc_resp = state.metrics.query_instant(&q_rpc_total).await?;
    let mut rpc_total: u64 = 0;
    for r in rpc_resp.data.result.iter() {
        if let Ok(v) = r.value.1.parse::<f64>() {
            rpc_total += v as u64;
        }
    }

    Ok((topics_owned, rpc_total))
}
