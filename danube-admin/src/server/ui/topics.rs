use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, response::IntoResponse, Json};
use serde::Serialize;

use crate::metrics::queries::fetch_topics_by_broker;
use crate::server::app::{AppState, CacheEntry};
use crate::server::ui::broker::{BrokerIdentity, BrokerTopicMini};
use crate::server::ui::shared::fetch_brokers;
use danube_core::admin_proto::TopicInfo;

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
            let (code, body) = crate::server::http::map_error(e);
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
            Ok(v) => brokers_with_topics.push(BrokerWithTopics {
                broker: ident,
                topics: v,
            }),
            Err(e) => {
                errors.push(format!(
                    "topics fetch failed for broker {}: {}",
                    b.broker_id, e
                ));
                brokers_with_topics.push(BrokerWithTopics {
                    broker: ident,
                    topics: Vec::new(),
                });
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

async fn fetch_topics_for_broker(
    state: &AppState,
    broker_id: &str,
) -> anyhow::Result<Vec<BrokerTopicMini>> {
    // Use shared query to fetch per-topic metrics by broker
    let (topics_map, errors) = fetch_topics_by_broker(&state.metrics, broker_id).await;
    if !errors.is_empty() {
        return Err(anyhow::anyhow!(errors.join("; ")));
    }

    // Fetch authoritative topic list via gRPC and enrich with metric counts
    let req = danube_core::admin_proto::BrokerRequest {
        broker_id: broker_id.to_string(),
    };
    let list = state.client.list_broker_topics(req).await?;
    let mut infos: Vec<TopicInfo> = list.topics;
    infos.sort_by(|a, b| a.name.cmp(&b.name));

    let out: Vec<BrokerTopicMini> = infos
        .into_iter()
        .map(|info| {
            let (p, c, s) = topics_map.get(&info.name).copied().unwrap_or((0, 0, 0));
            BrokerTopicMini {
                name: info.name,
                delivery: info.delivery,
                producers_connected: p,
                consumers_connected: c,
                subscriptions: s,
            }
        })
        .collect();
    Ok(out)
}
