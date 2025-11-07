use std::sync::Arc;
use std::time::Instant;

use axum::{extract::{Path, State}, response::IntoResponse, Json};

use crate::{AppState, CacheEntry};
use crate::dto::{BrokerPageDto, BrokerIdentityDto, BrokerMetricsDto, BrokerTopicMiniDto};

pub async fn broker_page(Path(broker_id): Path<String>, State(state): State<Arc<AppState>>) -> impl IntoResponse {
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

    // Resolve broker identity (simple scan from ListBrokers)
    let brokers = match state.client.list_brokers().await {
        Ok(b) => b,
        Err(e) => {
            let (code, body) = crate::http::map_error(e);
            return (code, body).into_response();
        }
    };
    let mut target = None;
    for br in brokers.brokers.iter() {
        if br.broker_id == broker_id {
            target = Some((br.broker_addr.clone(), br.broker_role.clone()));
            break;
        }
    }
    let Some((broker_addr, broker_role)) = target else {
        return (axum::http::StatusCode::NOT_FOUND, Json(serde_json::json!({"error":"unknown broker"}))).into_response();
    };

    // Scrape metrics and topic list from Prometheus (port 9040 by default)
    let host = broker_addr.split(':').next().unwrap_or(broker_addr.as_str());
    let (metrics, topics, scrape_err): (BrokerMetricsDto, Vec<BrokerTopicMiniDto>, Option<String>) = match state.metrics.scrape(host).await {
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

            // Build topic list using topic-labeled gauges
            let mut prod_by_topic: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
            if let Some(series) = map.get("danube_topic_active_producers") {
                for (labels, value) in series.iter() {
                    if let Some(t) = labels.get("topic") {
                        prod_by_topic.insert(t.clone(), *value as u64);
                    }
                }
            }
            let mut cons_by_topic: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
            if let Some(series) = map.get("danube_topic_active_consumers") {
                for (labels, value) in series.iter() {
                    if let Some(t) = labels.get("topic") {
                        cons_by_topic.insert(t.clone(), *value as u64);
                    }
                }
            }

            let mut topics: Vec<BrokerTopicMiniDto> = Vec::new();
            let mut producers_connected_sum = 0u64;
            let mut consumers_connected_sum = 0u64;
            // Union of keys from producers and consumers maps
            let mut topic_names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            topic_names.extend(prod_by_topic.keys().cloned());
            topic_names.extend(cons_by_topic.keys().cloned());
            for name in topic_names.into_iter() {
                let p = *prod_by_topic.get(&name).unwrap_or(&0);
                let c = *cons_by_topic.get(&name).unwrap_or(&0);
                producers_connected_sum += p;
                consumers_connected_sum += c;
                topics.push(BrokerTopicMiniDto { name, producers_connected: p, consumers_connected: c });
            }

            let metrics = BrokerMetricsDto {
                rpc_total,
                rpc_rate_1m: 0.0,
                topics_owned,
                producers_connected: producers_connected_sum,
                consumers_connected: consumers_connected_sum,
                inbound_bytes_total: 0,
                outbound_bytes_total: 0,
                errors_5xx_total: 0,
            };

            (metrics, topics, None)
        }
        Err(e) => {
            let metrics = BrokerMetricsDto {
                rpc_total: 0,
                rpc_rate_1m: 0.0,
                topics_owned: 0,
                producers_connected: 0,
                consumers_connected: 0,
                inbound_bytes_total: 0,
                outbound_bytes_total: 0,
                errors_5xx_total: 0,
            };
            (metrics, Vec::new(), Some(e.to_string()))
        }
    };

    let mut errors = Vec::new();
    if let Some(err) = scrape_err { errors.push(format!("metrics scrape failed: {}", err)); }

    let dto = BrokerPageDto {
        timestamp: chrono::Utc::now().to_rfc3339(),
        broker: BrokerIdentityDto {
            broker_id,
            broker_addr,
            broker_role,
        },
        metrics,
        topics,
        errors,
    };

    let mut cache = state.broker_page_cache.lock().await;
    cache.insert(dto.broker.broker_id.clone(), CacheEntry { expires_at: Instant::now() + state.ttl, value: dto.clone() });

    Json(dto).into_response()
}
