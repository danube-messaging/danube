//! Metrics resource for Danube MCP
//!
//! Provides the Prometheus metrics catalog that AI assistants can read
//! to understand available metrics for custom PromQL queries.

use rmcp::model::{RawResource, ReadResourceResult, Resource, ResourceContents};

/// URI for the metrics catalog resource
pub const METRICS_CATALOG_URI: &str = "danube://metrics";

/// Get the metrics resource definition
pub fn resource() -> Resource {
    Resource {
        raw: RawResource {
            uri: METRICS_CATALOG_URI.to_string(),
            name: "danube-metrics".to_string(),
            title: Some("Danube Prometheus Metrics Catalog".to_string()),
            description: Some(
                "Complete list of Danube Prometheus metrics with descriptions and labels. \
                 Use this to construct custom PromQL queries."
                    .to_string(),
            ),
            mime_type: Some("text/plain".to_string()),
            size: None,
            icons: None,
            meta: None,
        },
        annotations: None,
    }
}

/// Read the metrics catalog content
pub fn read(uri: &str) -> Option<ReadResourceResult> {
    if uri == METRICS_CATALOG_URI {
        Some(ReadResourceResult {
            contents: vec![ResourceContents::TextResourceContents {
                uri: METRICS_CATALOG_URI.to_string(),
                mime_type: Some("text/plain".to_string()),
                text: get_metrics_catalog(),
                meta: None,
            }],
        })
    } else {
        None
    }
}

/// Generate the metrics catalog content
fn get_metrics_catalog() -> String {
    r#"# Danube Prometheus Metrics Catalog

This document lists all Prometheus metrics exposed by Danube brokers.
Use these metric names to construct custom PromQL queries.

## Cluster-Level Metrics

### Broker Metrics
- `danube_broker_info` - Broker metadata (labels: broker_id, cluster, version)
- `danube_broker_topics_owned` - Number of topics owned by each broker
- `danube_broker_rpc_total` - Total RPC requests handled by broker

### Topic Metrics
- `danube_topic_messages_in_total{topic="..."}` - Total messages published to topic
- `danube_topic_messages_out_total{topic="..."}` - Total messages dispatched from topic
- `danube_topic_bytes_in_total{topic="..."}` - Total bytes received by topic
- `danube_topic_bytes_out_total{topic="..."}` - Total bytes sent from topic

### Producer/Consumer Metrics
- `danube_producers_active{topic="..."}` - Active producer count per topic
- `danube_consumers_active{topic="..."}` - Active consumer count per topic
- `danube_subscriptions_active{topic="..."}` - Active subscription count per topic

### Throughput Rates (use with rate())
- `rate(danube_topic_messages_in_total[1m])` - Publish rate (msg/s)
- `rate(danube_topic_messages_out_total[1m])` - Dispatch rate (msg/s)
- `rate(danube_topic_bytes_in_total[1m])` - Inbound throughput (bytes/s)
- `rate(danube_topic_bytes_out_total[1m])` - Outbound throughput (bytes/s)

### Latency Metrics (Histogram)
- `danube_producer_send_latency_bucket{topic="...", le="..."}` - Producer send latency histogram
- `danube_producer_send_latency_sum{topic="..."}` - Sum of all send latencies
- `danube_producer_send_latency_count{topic="..."}` - Count of send operations

### Consumer Lag
- `danube_subscription_lag{topic="...", subscription="..."}` - Messages behind for subscription

### Error Metrics
- `danube_producer_send_errors_total{topic="..."}` - Total producer send errors

## Persistent Storage Metrics (Reliable Topics)

### WAL (Write-Ahead Log)
- `danube_wal_writes_total{topic="..."}` - Total WAL write operations
- `danube_wal_write_bytes_total{topic="..."}` - Total bytes written to WAL
- `danube_wal_segments{topic="..."}` - Current WAL segment count

### Cloud Storage
- `danube_cloud_uploads_total{topic="..."}` - Total cloud upload operations
- `danube_cloud_upload_bytes_total{topic="..."}` - Total bytes uploaded to cloud
- `danube_cloud_upload_errors_total{topic="..."}` - Cloud upload errors

## Common PromQL Query Patterns

### Cluster Overview
```promql
# Total messages across all topics
sum(danube_topic_messages_in_total)

# Total producers in cluster
sum(danube_producers_active)

# Messages per second cluster-wide
sum(rate(danube_topic_messages_in_total[1m]))
```

### Per-Broker Analysis
```promql
# Topics per broker
danube_broker_topics_owned

# RPC load per broker
rate(danube_broker_rpc_total[1m])
```

### Topic Deep Dive
```promql
# Publish rate for specific topic
rate(danube_topic_messages_in_total{topic="/default/my-topic"}[1m])

# P99 latency using histogram_quantile
histogram_quantile(0.99, rate(danube_producer_send_latency_bucket{topic="/default/my-topic"}[5m]))

# Consumer lag
danube_subscription_lag{topic="/default/my-topic"}
```

### Alerts/Anomaly Detection
```promql
# High consumer lag (>1000 messages)
danube_subscription_lag > 1000

# Error rate spike
rate(danube_producer_send_errors_total[5m]) > 0.1

# Unbalanced cluster (coefficient of variation)
stddev(danube_broker_topics_owned) / avg(danube_broker_topics_owned)
```

## Labels Reference

| Label | Description | Example |
|-------|-------------|---------|
| broker_id | Broker identifier | "1", "broker-1" |
| topic | Full topic path | "/default/my-topic" |
| subscription | Subscription name | "my-subscription" |
| cluster | Cluster name | "danube-cluster" |
| le | Histogram bucket bound | "0.005", "0.01", "0.025" |
"#.to_string()
}
