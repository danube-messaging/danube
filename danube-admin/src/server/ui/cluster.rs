use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};

use crate::metrics::queries::fetch_broker_connections;
use crate::server::app::{AppState, CacheEntry};
use crate::server::ui::shared::fetch_brokers;

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
    pub active_connections: u64,
    pub errors_5xx_total: u64,
}

#[derive(Clone, Serialize)]
pub struct ClusterBroker {
    pub broker_id: String,
    pub broker_addr: String,
    pub broker_role: String,
    pub broker_status: String,
    pub admin_addr: String,
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
            let (code, body) = crate::server::http::map_error(e);
            return (code, body).into_response();
        }
    };

    for br in brokers.brokers.iter() {
        // Get authoritative topic list via gRPC
        let req = danube_core::admin_proto::BrokerRequest {
            broker_id: br.broker_id.clone(),
        };
        let (topics_owned, topic_names_set) = match state.client.list_broker_topics(req).await {
            Ok(list) => {
                let names: std::collections::HashSet<String> =
                    list.topics.into_iter().map(|t| t.name).collect();
                (names.len() as u64, names)
            }
            Err(e) => {
                errors.push(format!(
                    "list_broker_topics failed for {}: {}",
                    br.broker_id, e
                ));
                (0u64, std::collections::HashSet::new())
            }
        };

        let (rpc_total, active_connections) =
            match query_broker_metrics(&state, &br.broker_id, &topic_names_set).await {
                Ok(vals) => vals,
                Err(e) => {
                    errors.push(format!("metrics scrape failed for {}: {}", br.broker_id, e));
                    (0, 0)
                }
            };

        let stats = ClusterBrokerStats {
            topics_owned,
            rpc_total,
            active_connections,
            errors_5xx_total: 0,
        };
        totals_topics += stats.topics_owned;
        totals_rpc += stats.rpc_total;
        totals_conns += stats.active_connections;
        brokers_out.push(ClusterBroker {
            broker_id: br.broker_id.clone(),
            broker_addr: br.broker_addr.clone(),
            broker_role: br.broker_role.clone(),
            broker_status: br.broker_status.clone(),
            admin_addr: br.admin_addr.clone(),
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

async fn query_broker_metrics(
    state: &AppState,
    broker_id: &str,
    topic_names: &std::collections::HashSet<String>,
) -> anyhow::Result<(u64, u64)> {
    // Use shared query to fetch broker connections
    let (data, errors) = fetch_broker_connections(&state.metrics, broker_id, topic_names).await;
    if !errors.is_empty() {
        return Err(anyhow::anyhow!(errors.join("; ")));
    }
    Ok((data.rpc_total, data.active_connections))
}

#[derive(Serialize)]
pub struct ClusterBalanceInfo {
    pub coefficient_of_variation: f64,
    pub mean_load: f64,
    pub max_load: f64,
    pub min_load: f64,
    pub std_deviation: f64,
    pub broker_count: u32,
    pub assignment_strategy: String,
    pub brokers: Vec<BrokerLoadInfo>,
}

#[derive(Serialize)]
pub struct BrokerLoadInfo {
    pub broker_id: String,
    pub load: f64,
    pub topic_count: u32,
    pub is_overloaded: bool,
    pub is_underloaded: bool,
}

pub async fn cluster_balance(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let leader_client = get_leader_client(&state).await;
    let req = danube_core::admin_proto::ClusterBalanceRequest {};
    match leader_client.get_cluster_balance(req).await {
        Ok(res) => {
            let brokers = res.brokers.into_iter().map(|b| BrokerLoadInfo {
                broker_id: b.broker_id.to_string(),
                load: b.load,
                topic_count: b.topic_count,
                is_overloaded: b.is_overloaded,
                is_underloaded: b.is_underloaded,
            }).collect();
            let info = ClusterBalanceInfo {
                coefficient_of_variation: res.coefficient_of_variation,
                mean_load: res.mean_load,
                max_load: res.max_load,
                min_load: res.min_load,
                std_deviation: res.std_deviation,
                broker_count: res.broker_count,
                assignment_strategy: res.assignment_strategy,
                brokers,
            };
            Json(info).into_response()
        }
        Err(e) => {
            (
                axum::http::StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "error": e.to_string()
                }))
            ).into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct RebalanceRequestDto {
    pub dry_run: Option<bool>,
    pub max_moves: Option<u32>,
}

#[derive(Serialize)]
pub struct RebalanceResponseDto {
    pub success: bool,
    pub moves_executed: u32,
    pub proposed_moves: Vec<ProposedMoveDto>,
    pub error_message: String,
}

#[derive(Serialize)]
pub struct ProposedMoveDto {
    pub topic_name: String,
    pub from_broker: String,
    pub to_broker: String,
    pub estimated_load: f64,
    pub reason: String,
}

pub async fn cluster_rebalance(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RebalanceRequestDto>,
) -> impl IntoResponse {
    let leader_client = get_leader_client(&state).await;
    let request = danube_core::admin_proto::RebalanceRequest {
        dry_run: req.dry_run.unwrap_or(false),
        max_moves: req.max_moves,
    };
    match leader_client.trigger_rebalance(request).await {
        Ok(res) => {
            let proposed_moves = res.proposed_moves.into_iter().map(|m| ProposedMoveDto {
                topic_name: m.topic_name,
                from_broker: m.from_broker.to_string(),
                to_broker: m.to_broker.to_string(),
                estimated_load: m.estimated_load,
                reason: m.reason,
            }).collect();
            Json(RebalanceResponseDto {
                success: res.success,
                moves_executed: res.moves_executed,
                proposed_moves,
                error_message: res.error_message,
            }).into_response()
          }
          Err(e) => {
              (
                  axum::http::StatusCode::BAD_GATEWAY,
                  Json(RebalanceResponseDto {
                      success: false,
                      moves_executed: 0,
                      proposed_moves: Vec::new(),
                      error_message: e.to_string(),
                  })
              ).into_response()
          }
    }
}

async fn get_leader_client(state: &AppState) -> crate::core::AdminGrpcClient {
    // 1. Attempt to resolve from cache first to avoid any extra network calls
    let cached_leader = {
        let cache = state.cluster_page_cache.lock().await;
        cache.as_ref().and_then(|entry| {
            entry.value.brokers.iter()
                .find(|b| b.broker_role == "Cluster_Leader")
                .map(|b| b.admin_addr.clone())
        })
    };

    if let Some(mut endpoint) = cached_leader {
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            endpoint = format!("http://{}", endpoint);
        }
        let endpoint = endpoint.replace("0.0.0.0", "127.0.0.1");

        let config = crate::core::GrpcClientConfig {
            endpoint,
            request_timeout_ms: 10000,
            token: state.client.token(),
            ..Default::default()
        };

        if let Ok(client) = crate::core::AdminGrpcClient::connect(config).await {
            return client;
        }
    }

    // 2. Fallback to querying dynamic broker list if cache is empty or connection fails
    if let Ok(list) = state.client.list_brokers().await {
        if let Some(leader_broker) = list.brokers.iter().find(|b| b.broker_role == "Cluster_Leader") {
            let mut endpoint = leader_broker.admin_addr.clone();
            if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                endpoint = format!("http://{}", endpoint);
            }
            let endpoint = endpoint.replace("0.0.0.0", "127.0.0.1");

            let config = crate::core::GrpcClientConfig {
                endpoint,
                request_timeout_ms: 10000,
                token: state.client.token(),
                ..Default::default()
            };

            if let Ok(client) = crate::core::AdminGrpcClient::connect(config).await {
                return client;
            }
        }
    }
    state.client.clone()
}
