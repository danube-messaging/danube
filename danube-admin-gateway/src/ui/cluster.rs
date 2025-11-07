use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, response::IntoResponse, Json};

use crate::dto::{ClusterBrokerDto, ClusterBrokerStatsDto, ClusterPageDto, ClusterTotalsDto};
use crate::{AppState, CacheEntry};

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
    let mut brokers_out: Vec<ClusterBrokerDto> = Vec::new();
    let mut totals_topics = 0u64;
    let mut totals_rpc = 0u64;
    let mut totals_conns = 0u64;

    let brokers = match state.client.list_brokers().await {
        Ok(b) => b,
        Err(e) => {
            let (code, body) = crate::http::map_error(e);
            return (code, body).into_response();
        }
    };

    for br in brokers.brokers.iter() {
        let host = br
            .broker_addr
            .split(':')
            .next()
            .unwrap_or(br.broker_addr.as_str())
            .to_string();
        let scrape = state.metrics.scrape(&host).await;
        let (topics_owned, rpc_total) = match scrape {
            Ok(text) => {
                let map = crate::metrics::parse_prometheus(&text);
                let topics_owned = map
                    .get("danube_broker_topics_owned")
                    .and_then(|v| v.first())
                    .map(|(_, val)| *val as u64)
                    .unwrap_or(0);
                let rpc_total = map
                    .get("danube_broker_rpc_total")
                    .and_then(|v| v.first())
                    .map(|(_, val)| *val as u64)
                    .unwrap_or(0);
                (topics_owned, rpc_total)
            }
            Err(e) => {
                errors.push(format!("metrics scrape failed for {}: {}", br.broker_id, e));
                (0, 0)
            }
        };

        let stats = ClusterBrokerStatsDto {
            topics_owned,
            rpc_total,
            rpc_rate_1m: 0.0,
            active_connections: 0,
            errors_5xx_total: 0,
        };
        totals_topics += stats.topics_owned;
        totals_rpc += stats.rpc_total;
        totals_conns += stats.active_connections;
        brokers_out.push(ClusterBrokerDto {
            broker_id: br.broker_id.clone(),
            broker_addr: br.broker_addr.clone(),
            broker_role: br.broker_role.clone(),
            stats,
        });
    }

    let dto = ClusterPageDto {
        timestamp: chrono::Utc::now().to_rfc3339(),
        brokers: brokers_out,
        totals: ClusterTotalsDto {
            broker_count: brokers.brokers.len() as u64,
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
