//! MCP tools for querying Prometheus metrics with broker gRPC snapshot fallback

use crate::core::grpc_client::AdminGrpcClient;
use crate::metrics::{queries, MetricsClient};
use danube_core::admin_proto::GetMetricsSnapshotRequest;
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

async fn format_grpc_snapshot_fallback(
    grpc: &AdminGrpcClient,
    title: &str,
    topic_filter: Option<&str>,
    prom_errors: &[String],
) -> String {
    let mut output = format!("=== {} (gRPC Telemetry Snapshot Fallback) ===\n", title);
    output.push_str("Prometheus was unreachable or returned errors:\n");
    for e in prom_errors {
        output.push_str(&format!("  - {}\n", e));
    }

    match grpc
        .get_metrics_snapshot(GetMetricsSnapshotRequest {
            metric_names: Vec::new(),
            window_seconds: 300,
        })
        .await
    {
        Ok(resp) => {
            output.push_str(&format!(
                "\nConnected Broker: {} | Collected At: {}ms\n\n",
                resp.broker_id, resp.collected_at_ms
            ));
            let mut matched = 0usize;
            for series in resp.series {
                if let Some(topic) = topic_filter {
                    let topic_label = series.labels.get("topic").map(String::as_str);
                    if topic_label != Some(topic)
                        && topic_label != Some(topic.trim_start_matches('/'))
                    {
                        continue;
                    }
                }
                matched += 1;
                let labels = if series.labels.is_empty() {
                    String::new()
                } else {
                    let pairs: Vec<String> = series
                        .labels
                        .iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect();
                    format!(" {{{}}}", pairs.join(", "))
                };
                output.push_str(&format!(
                    "- {}{} [{}]: {} ({} samples in window)\n",
                    series.name,
                    labels,
                    series.kind,
                    series.current_value,
                    series.history.len()
                ));
            }
            if matched == 0 {
                output.push_str("(No matching metrics in broker telemetry ring buffer.)\n");
            }
        }
        Err(err) => {
            output.push_str(&format!(
                "\ngRPC GetMetricsSnapshot fallback also failed: {}\n",
                err
            ));
        }
    }
    output
}

/// Get cluster-wide metrics summary
pub async fn get_cluster_metrics(client: &MetricsClient, grpc: &AdminGrpcClient) -> String {
    let result = queries::fetch_cluster_metrics(client).await;
    if !result.errors.is_empty() {
        return format_grpc_snapshot_fallback(grpc, "Cluster Metrics", None, &result.errors).await;
    }
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

    output
}

/// Get metrics for a specific broker
pub async fn get_broker_metrics(
    client: &MetricsClient,
    grpc: &AdminGrpcClient,
    params: BrokerMetricsParams,
) -> String {
    let result = queries::fetch_broker_metrics(client, &params.broker_id).await;
    if !result.errors.is_empty() {
        return format_grpc_snapshot_fallback(
            grpc,
            &format!("Broker {} Metrics", params.broker_id),
            None,
            &result.errors,
        )
        .await;
    }
    let m = &result.data;

    let mut output = format!("=== Broker {} Metrics ===\n\n", params.broker_id);
    output.push_str(&format!("Topics Owned:   {}\n", m.topics_owned));
    output.push_str(&format!("RPC Total:      {}\n", m.rpc_total));
    output.push_str(&format!("Producers:      {}\n", m.producers));
    output.push_str(&format!("Consumers:      {}\n", m.consumers));
    output.push_str(&format!("Bytes In:       {} bytes\n", m.bytes_in_total));
    output.push_str(&format!("Bytes Out:      {} bytes\n", m.bytes_out_total));

    output
}

/// Get metrics for a specific topic
pub async fn get_topic_metrics(
    client: &MetricsClient,
    grpc: &AdminGrpcClient,
    params: TopicMetricsParams,
) -> String {
    let result = queries::fetch_topic_metrics(client, &params.topic).await;
    if !result.errors.is_empty() {
        return format_grpc_snapshot_fallback(
            grpc,
            &format!("Topic {} Metrics", params.topic),
            Some(&params.topic),
            &result.errors,
        )
        .await;
    }
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

    output
}

/// Execute a raw PromQL query
pub async fn query_prometheus(client: &MetricsClient, params: RawQueryParams) -> String {
    match queries::query_raw(client, &params.query).await {
        Ok(output) => output,
        Err(e) => format!("Query failed: {}", e),
    }
}
