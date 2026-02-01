//! MCP tools for querying Prometheus metrics

use crate::metrics::{queries, MetricsClient};
use schemars::JsonSchema;
use serde::Deserialize;

/// Parameters for broker metrics query
#[derive(Debug, Deserialize, JsonSchema)]
pub struct BrokerMetricsParams {
    /// Broker ID to query metrics for.
    /// Must match the broker_id label in Prometheus metrics.
    /// Use list_brokers to discover active broker IDs.
    /// Example: "1", "broker-1", "danube-broker-0"
    pub broker_id: String,
}

/// Parameters for topic metrics query
#[derive(Debug, Deserialize, JsonSchema)]
pub struct TopicMetricsParams {
    /// Full topic name including namespace.
    /// Must match the topic label in Prometheus metrics.
    /// Format: "/namespace/topic-name"
    /// Example: "/default/user-events", "/production/analytics"
    pub topic: String,
}

/// Parameters for raw Prometheus query
#[derive(Debug, Deserialize, JsonSchema)]
pub struct RawQueryParams {
    /// PromQL query string to execute.
    /// Supports full PromQL syntax including functions, aggregations, and time ranges.
    /// Examples:
    ///   - "sum(rate(danube_topic_messages_in_total[5m]))"
    ///   - "danube_subscription_lag{topic=\"/default/events\"}"
    ///   - "histogram_quantile(0.99, rate(danube_producer_send_latency_bucket[5m]))"
    pub query: String,
}

/// Get cluster-wide metrics summary
pub async fn get_cluster_metrics(client: &MetricsClient) -> String {
    let result = queries::fetch_cluster_metrics(client).await;
    let m = &result.data;

    let mut output = String::from("=== Cluster Metrics ===\n\n");
    output.push_str(&format!("Brokers:        {}\n", m.broker_count));
    output.push_str(&format!("Topics:         {}\n", m.topics_total));
    output.push_str(&format!("Producers:      {}\n", m.producers_total));
    output.push_str(&format!("Consumers:      {}\n", m.consumers_total));
    output.push_str(&format!("Subscriptions:  {}\n", m.subscriptions_total));
    output.push_str("\n--- Throughput (1m avg) ---\n");
    output.push_str(&format!(
        "Messages In:    {:.2} msg/s\n",
        m.messages_in_rate
    ));
    output.push_str(&format!(
        "Messages Out:   {:.2} msg/s\n",
        m.messages_out_rate
    ));
    output.push_str(&format!("Bytes In:       {:.2} B/s\n", m.bytes_in_rate));
    output.push_str(&format!("Bytes Out:      {:.2} B/s\n", m.bytes_out_rate));
    output.push_str("\n--- Balance ---\n");
    output.push_str(&format!(
        "Imbalance CV:   {:.4} (lower is better, 0 = perfect)\n",
        m.imbalance_cv
    ));

    if !result.errors.is_empty() {
        output.push_str("\n--- Errors ---\n");
        for e in &result.errors {
            output.push_str(&format!("  - {}\n", e));
        }
    }

    output
}

/// Get metrics for a specific broker
pub async fn get_broker_metrics(client: &MetricsClient, params: BrokerMetricsParams) -> String {
    let result = queries::fetch_broker_metrics(client, &params.broker_id).await;
    let m = &result.data;

    let mut output = format!("=== Broker {} Metrics ===\n\n", params.broker_id);
    output.push_str(&format!("Topics Owned:   {}\n", m.topics_owned));
    output.push_str(&format!("RPC Total:      {}\n", m.rpc_total));
    output.push_str(&format!("Producers:      {}\n", m.producers));
    output.push_str(&format!("Consumers:      {}\n", m.consumers));
    output.push_str(&format!("Bytes In:       {} bytes\n", m.bytes_in_total));
    output.push_str(&format!("Bytes Out:      {} bytes\n", m.bytes_out_total));

    if !result.errors.is_empty() {
        output.push_str("\n--- Errors ---\n");
        for e in &result.errors {
            output.push_str(&format!("  - {}\n", e));
        }
    }

    output
}

/// Get metrics for a specific topic
pub async fn get_topic_metrics(client: &MetricsClient, params: TopicMetricsParams) -> String {
    let result = queries::fetch_topic_metrics(client, &params.topic).await;
    let m = &result.data;

    let mut output = format!("=== Topic {} Metrics ===\n\n", params.topic);

    output.push_str("--- Counters ---\n");
    output.push_str(&format!("Messages In:    {}\n", m.messages_in_total));
    output.push_str(&format!("Messages Out:   {}\n", m.messages_out_total));
    output.push_str(&format!("Bytes In:       {} bytes\n", m.bytes_in_total));
    output.push_str(&format!("Bytes Out:      {} bytes\n", m.bytes_out_total));

    output.push_str("\n--- Active Entities ---\n");
    output.push_str(&format!("Producers:      {}\n", m.producers));
    output.push_str(&format!("Consumers:      {}\n", m.consumers));
    output.push_str(&format!("Subscriptions:  {}\n", m.subscriptions));

    output.push_str("\n--- Rates (1m avg) ---\n");
    output.push_str(&format!("Publish Rate:   {:.2} msg/s\n", m.publish_rate_1m));
    output.push_str(&format!(
        "Dispatch Rate:  {:.2} msg/s\n",
        m.dispatch_rate_1m
    ));

    output.push_str("\n--- Consumer Lag ---\n");
    output.push_str(&format!(
        "Total Lag:      {} messages\n",
        m.subscription_lag
    ));

    output.push_str("\n--- Latency (p50/p95/p99) ---\n");
    output.push_str(&format!(
        "Send Latency:   {:.2} / {:.2} / {:.2} ms\n",
        m.latency_p50_ms, m.latency_p95_ms, m.latency_p99_ms
    ));

    if !result.errors.is_empty() {
        output.push_str("\n--- Errors ---\n");
        for e in &result.errors {
            output.push_str(&format!("  - {}\n", e));
        }
    }

    output
}

/// Execute a raw PromQL query
pub async fn query_prometheus(client: &MetricsClient, params: RawQueryParams) -> String {
    match queries::query_raw(client, &params.query).await {
        Ok(output) => output,
        Err(e) => format!("Query failed: {}", e),
    }
}
