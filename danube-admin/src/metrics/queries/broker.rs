//! Broker-related metric queries

use std::collections::{HashMap, HashSet};

use crate::metrics::client::MetricsClient;
use crate::metrics::types::{BrokerActive, BrokerBytes, BrokerMetrics, MetricResult};

use super::helpers::sum_u64;

// ===== Modular Broker Metric Queries (composable building blocks) =====

/// Fetch broker RPC total
pub async fn fetch_broker_rpc(client: &MetricsClient, broker_id: &str) -> (u64, Vec<String>) {
    let mut errors = Vec::new();
    let q = format!("sum(danube_broker_rpc_total{{broker=\"{}\"}})", broker_id);
    let rpc_total = sum_u64(client.query_instant(&q).await, &mut errors);
    (rpc_total, errors)
}

/// Fetch broker topics owned count
pub async fn fetch_broker_topics_owned(
    client: &MetricsClient,
    broker_id: &str,
) -> (u64, Vec<String>) {
    let mut errors = Vec::new();
    let q = format!("danube_broker_topics_owned{{broker=\"{}\"}}", broker_id);
    let topics_owned = sum_u64(client.query_instant(&q).await, &mut errors);
    (topics_owned, errors)
}

/// Fetch broker bytes in/out
pub async fn fetch_broker_bytes(
    client: &MetricsClient,
    broker_id: &str,
) -> (BrokerBytes, Vec<String>) {
    let mut errors = Vec::new();

    let q_in = format!(
        "sum(danube_topic_bytes_in_total{{broker=\"{}\"}})",
        broker_id
    );
    let q_out = format!(
        "sum(danube_consumer_bytes_out_total{{broker=\"{}\"}})",
        broker_id
    );

    let data = BrokerBytes {
        bytes_in_total: sum_u64(client.query_instant(&q_in).await, &mut errors),
        bytes_out_total: sum_u64(client.query_instant(&q_out).await, &mut errors),
    };
    (data, errors)
}

/// Fetch broker active producers/consumers
pub async fn fetch_broker_active(
    client: &MetricsClient,
    broker_id: &str,
) -> (BrokerActive, Vec<String>) {
    let mut errors = Vec::new();

    let q_prod = format!(
        "sum(danube_topic_active_producers{{broker=\"{}\"}})",
        broker_id
    );
    let q_cons = format!(
        "sum(danube_topic_active_consumers{{broker=\"{}\"}})",
        broker_id
    );

    let data = BrokerActive {
        producers: sum_u64(client.query_instant(&q_prod).await, &mut errors),
        consumers: sum_u64(client.query_instant(&q_cons).await, &mut errors),
    };
    (data, errors)
}

// ===== Composed Broker Queries =====

/// Fetch metrics for a specific broker (for MCP)
/// Composed from modular query functions for reusability
pub async fn fetch_broker_metrics(
    client: &MetricsClient,
    broker_id: &str,
) -> MetricResult<BrokerMetrics> {
    let mut errors = Vec::new();

    let (topics_owned, mut errs) = fetch_broker_topics_owned(client, broker_id).await;
    errors.append(&mut errs);

    let (rpc_total, mut errs) = fetch_broker_rpc(client, broker_id).await;
    errors.append(&mut errs);

    let (active, mut errs) = fetch_broker_active(client, broker_id).await;
    errors.append(&mut errs);

    let (bytes, mut errs) = fetch_broker_bytes(client, broker_id).await;
    errors.append(&mut errs);

    MetricResult::with_errors(
        BrokerMetrics {
            broker_id: broker_id.to_string(),
            topics_owned,
            rpc_total,
            producers: active.producers,
            consumers: active.consumers,
            bytes_in_total: bytes.bytes_in_total,
            bytes_out_total: bytes.bytes_out_total,
        },
        errors,
    )
}

/// Broker metrics for UI (rpc_total, bytes_in, bytes_out)
/// Composed from modular query functions
pub async fn fetch_broker_metrics_for_ui(
    client: &MetricsClient,
    broker_id: &str,
) -> (BrokerBytes, u64, Vec<String>) {
    let mut errors = Vec::new();

    let (rpc_total, mut errs) = fetch_broker_rpc(client, broker_id).await;
    errors.append(&mut errs);

    let (bytes, mut errs) = fetch_broker_bytes(client, broker_id).await;
    errors.append(&mut errs);

    (bytes, rpc_total, errors)
}

/// Broker connection stats (rpc_total + active connections filtered by topics)
#[derive(Clone, Debug, Default)]
pub struct BrokerConnectionStats {
    pub rpc_total: u64,
    pub active_connections: u64,
}

/// Fetch broker RPC and active connections (producers + consumers filtered by topic set)
pub async fn fetch_broker_connections(
    client: &MetricsClient,
    broker_id: &str,
    topic_names: &HashSet<String>,
) -> (BrokerConnectionStats, Vec<String>) {
    let mut errors = Vec::new();

    let q_rpc = format!("sum(danube_broker_rpc_total{{broker=\"{}\"}})", broker_id);
    let q_prod = format!("danube_topic_active_producers{{broker=\"{}\"}}", broker_id);
    let q_cons = format!("danube_topic_active_consumers{{broker=\"{}\"}}", broker_id);

    let rpc_total = sum_u64(client.query_instant(&q_rpc).await, &mut errors);

    let mut producers: u64 = 0;
    match client.query_instant(&q_prod).await {
        Ok(resp) => {
            for r in resp.data.result.iter() {
                if let Some(t) = r.metric.get("topic") {
                    if topic_names.contains(t) {
                        if let Ok(v) = r.value.1.parse::<f64>() {
                            producers += v as u64;
                        }
                    }
                }
            }
        }
        Err(e) => errors.push(format!("producers query failed: {}", e)),
    }

    let mut consumers: u64 = 0;
    match client.query_instant(&q_cons).await {
        Ok(resp) => {
            for r in resp.data.result.iter() {
                if let Some(t) = r.metric.get("topic") {
                    if topic_names.contains(t) {
                        if let Ok(v) = r.value.1.parse::<f64>() {
                            consumers += v as u64;
                        }
                    }
                }
            }
        }
        Err(e) => errors.push(format!("consumers query failed: {}", e)),
    }

    let data = BrokerConnectionStats {
        rpc_total,
        active_connections: producers + consumers,
    };
    (data, errors)
}

/// Fetch metrics for topics by broker, returns map of topic -> (producers, consumers, subscriptions)
pub async fn fetch_topics_by_broker(
    client: &MetricsClient,
    broker_id: &str,
) -> (HashMap<String, (u64, u64, u64)>, Vec<String>) {
    let mut errors = Vec::new();
    let mut result: HashMap<String, (u64, u64, u64)> = HashMap::new();

    let q_prod = format!("danube_topic_active_producers{{broker=\"{}\"}}", broker_id);
    let q_cons = format!("danube_topic_active_consumers{{broker=\"{}\"}}", broker_id);
    let q_subs = format!(
        "danube_topic_active_subscriptions{{broker=\"{}\"}}",
        broker_id
    );

    // Producers by topic
    if let Ok(resp) = client.query_instant(&q_prod).await {
        for r in resp.data.result.iter() {
            if let Some(topic) = r.metric.get("topic") {
                if let Ok(v) = r.value.1.parse::<f64>() {
                    let entry = result.entry(topic.clone()).or_insert((0, 0, 0));
                    entry.0 = v as u64;
                }
            }
        }
    } else {
        errors.push("producers query failed".to_string());
    }

    // Consumers by topic
    if let Ok(resp) = client.query_instant(&q_cons).await {
        for r in resp.data.result.iter() {
            if let Some(topic) = r.metric.get("topic") {
                if let Ok(v) = r.value.1.parse::<f64>() {
                    let entry = result.entry(topic.clone()).or_insert((0, 0, 0));
                    entry.1 = v as u64;
                }
            }
        }
    } else {
        errors.push("consumers query failed".to_string());
    }

    // Subscriptions by topic
    if let Ok(resp) = client.query_instant(&q_subs).await {
        for r in resp.data.result.iter() {
            if let Some(topic) = r.metric.get("topic") {
                if let Ok(v) = r.value.1.parse::<f64>() {
                    let entry = result.entry(topic.clone()).or_insert((0, 0, 0));
                    entry.2 = v as u64;
                }
            }
        }
    } else {
        errors.push("subscriptions query failed".to_string());
    }

    (result, errors)
}
