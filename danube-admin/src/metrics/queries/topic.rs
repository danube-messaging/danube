//! Topic-related metric queries

use crate::metrics::client::MetricsClient;
use crate::metrics::types::{
    LatencyPercentiles, MessageSizeStats, MetricResult, ProducerQuality, ReliableMetrics,
    SubscriptionLag, TopicActive, TopicCore, TopicMetrics, TopicRates,
};

use super::helpers::{one_f64, sum_f64, sum_u64};

/// Fetch metrics for a specific topic (summary for MCP)
/// Composed from modular query functions for reusability
pub async fn fetch_topic_metrics(
    client: &MetricsClient,
    topic: &str,
) -> MetricResult<TopicMetrics> {
    let mut errors = Vec::new();

    // Fetch using modular functions
    let (core, mut core_errs) = fetch_topic_core(client, topic).await;
    errors.append(&mut core_errs);

    let (active, mut active_errs) = fetch_topic_active(client, topic).await;
    errors.append(&mut active_errs);

    let (rates, mut rates_errs) = fetch_topic_rates(client, topic).await;
    errors.append(&mut rates_errs);

    let (lag, mut lag_errs) = fetch_subscription_lag(client, topic).await;
    errors.append(&mut lag_errs);

    let (latency, mut latency_errs) = fetch_latency_histogram(client, topic).await;
    errors.append(&mut latency_errs);

    MetricResult::with_errors(
        TopicMetrics {
            topic: topic.to_string(),
            messages_in_total: core.messages_in_total,
            messages_out_total: core.messages_out_total,
            bytes_in_total: core.bytes_in_total,
            bytes_out_total: core.bytes_out_total,
            producers: active.producers,
            consumers: active.consumers,
            subscriptions: active.subscriptions,
            publish_rate_1m: rates.publish_rate_1m,
            dispatch_rate_1m: rates.dispatch_rate_1m,
            subscription_lag: lag.total_lag_messages,
            latency_p50_ms: latency.p50_ms,
            latency_p95_ms: latency.p95_ms,
            latency_p99_ms: latency.p99_ms,
        },
        errors,
    )
}

// ===== Modular Topic Metric Queries (composable building blocks) =====

/// Fetch core message/byte counters for a topic
pub async fn fetch_topic_core(client: &MetricsClient, topic: &str) -> (TopicCore, Vec<String>) {
    let mut errors = Vec::new();

    let q_msg_in = format!("sum(danube_topic_messages_in_total{{topic=\"{}\"}})", topic);
    let q_msg_out = format!(
        "sum(danube_consumer_messages_out_total{{topic=\"{}\"}})",
        topic
    );
    let q_bytes_in = format!("sum(danube_topic_bytes_in_total{{topic=\"{}\"}})", topic);
    let q_bytes_out = format!(
        "sum(danube_consumer_bytes_out_total{{topic=\"{}\"}})",
        topic
    );

    let data = TopicCore {
        messages_in_total: sum_u64(client.query_instant(&q_msg_in).await, &mut errors),
        messages_out_total: sum_u64(client.query_instant(&q_msg_out).await, &mut errors),
        bytes_in_total: sum_u64(client.query_instant(&q_bytes_in).await, &mut errors),
        bytes_out_total: sum_u64(client.query_instant(&q_bytes_out).await, &mut errors),
    };
    (data, errors)
}

/// Fetch active entities (producers, consumers, subscriptions) for a topic
pub async fn fetch_topic_active(client: &MetricsClient, topic: &str) -> (TopicActive, Vec<String>) {
    let mut errors = Vec::new();

    let q_prod = format!("sum(danube_topic_active_producers{{topic=\"{}\"}})", topic);
    let q_cons = format!("sum(danube_topic_active_consumers{{topic=\"{}\"}})", topic);
    let q_subs = format!(
        "sum(danube_topic_active_subscriptions{{topic=\"{}\"}})",
        topic
    );

    let data = TopicActive {
        producers: sum_u64(client.query_instant(&q_prod).await, &mut errors),
        consumers: sum_u64(client.query_instant(&q_cons).await, &mut errors),
        subscriptions: sum_u64(client.query_instant(&q_subs).await, &mut errors),
    };
    (data, errors)
}

/// Fetch message rates (publish/dispatch per minute) for a topic
pub async fn fetch_topic_rates(client: &MetricsClient, topic: &str) -> (TopicRates, Vec<String>) {
    let mut errors = Vec::new();

    let q_pub = format!(
        "sum(rate(danube_topic_messages_in_total{{topic=\"{}\"}}[1m]))",
        topic
    );
    let q_disp = format!(
        "sum(rate(danube_consumer_messages_out_total{{topic=\"{}\"}}[1m]))",
        topic
    );

    let data = TopicRates {
        publish_rate_1m: sum_f64(client.query_instant(&q_pub).await, &mut errors),
        dispatch_rate_1m: sum_f64(client.query_instant(&q_disp).await, &mut errors),
    };
    (data, errors)
}

/// Fetch producer send quality metrics (ok/error counts, error breakdown by code)
pub async fn fetch_producer_quality(
    client: &MetricsClient,
    topic: &str,
) -> (ProducerQuality, Vec<String>) {
    let mut errors = Vec::new();

    let q_ok = format!(
        "sum(danube_producer_send_total{{topic=\"{}\",result=\"ok\"}})",
        topic
    );
    let q_err = format!(
        "sum(danube_producer_send_total{{topic=\"{}\",result=\"error\"}})",
        topic
    );
    let q_err_by_code = format!(
        "sum by (error_code) (danube_producer_send_total{{topic=\"{}\",result=\"error\"}})",
        topic
    );

    let send_ok_total = sum_u64(client.query_instant(&q_ok).await, &mut errors);
    let send_error_total = sum_u64(client.query_instant(&q_err).await, &mut errors);

    let mut send_error_by_code: Vec<(String, u64)> = Vec::new();
    match client.query_instant(&q_err_by_code).await {
        Ok(resp) => {
            for r in resp.data.result.iter() {
                if let Some(code) = r.metric.get("error_code") {
                    if let Ok(v) = r.value.1.parse::<f64>() {
                        send_error_by_code.push((code.clone(), v as u64));
                    }
                }
            }
        }
        Err(e) => errors.push(format!("send_error_by_code query failed: {}", e)),
    }

    let data = ProducerQuality {
        send_ok_total,
        send_error_total,
        send_error_by_code,
    };
    (data, errors)
}

/// Fetch message size statistics for a topic
pub async fn fetch_message_size(
    client: &MetricsClient,
    topic: &str,
) -> (MessageSizeStats, Vec<String>) {
    let mut errors = Vec::new();

    let q_sum = format!("danube_topic_message_size_bytes_sum{{topic=\"{}\"}}", topic);
    let q_count = format!(
        "danube_topic_message_size_bytes_count{{topic=\"{}\"}}",
        topic
    );

    let sum_bytes = one_f64(client.query_instant(&q_sum).await, &mut errors);
    let count = one_f64(client.query_instant(&q_count).await, &mut errors) as u64;
    let avg_bytes = if count > 0 {
        sum_bytes / count as f64
    } else {
        0.0
    };

    let data = MessageSizeStats {
        avg_bytes,
        sum_bytes,
        count,
    };
    (data, errors)
}

/// Fetch latency percentiles from histogram buckets (computes quantiles from raw data)
/// Use this for accurate latency calculation over a time window
pub async fn fetch_latency_histogram(
    client: &MetricsClient,
    topic: &str,
) -> (LatencyPercentiles, Vec<String>) {
    let mut errors = Vec::new();

    let q_p50 = format!("histogram_quantile(0.5, sum(rate(danube_producer_send_latency_ms_bucket{{topic=\"{}\"}}[5m])) by (le))", topic);
    let q_p95 = format!("histogram_quantile(0.95, sum(rate(danube_producer_send_latency_ms_bucket{{topic=\"{}\"}}[5m])) by (le))", topic);
    let q_p99 = format!("histogram_quantile(0.99, sum(rate(danube_producer_send_latency_ms_bucket{{topic=\"{}\"}}[5m])) by (le))", topic);

    let data = LatencyPercentiles {
        p50_ms: one_f64(client.query_instant(&q_p50).await, &mut errors),
        p95_ms: one_f64(client.query_instant(&q_p95).await, &mut errors),
        p99_ms: one_f64(client.query_instant(&q_p99).await, &mut errors),
    };
    (data, errors)
}

/// Fetch latency percentiles from pre-computed summary quantiles
/// Use this for quick reads when summary metrics are available
pub async fn fetch_latency_percentiles(
    client: &MetricsClient,
    topic: &str,
) -> (LatencyPercentiles, Vec<String>) {
    let mut errors = Vec::new();

    let q_p50 = format!(
        "danube_producer_send_latency_ms{{topic=\"{}\",quantile=\"0.5\"}}",
        topic
    );
    let q_p95 = format!(
        "danube_producer_send_latency_ms{{topic=\"{}\",quantile=\"0.95\"}}",
        topic
    );
    let q_p99 = format!(
        "danube_producer_send_latency_ms{{topic=\"{}\",quantile=\"0.99\"}}",
        topic
    );

    let data = LatencyPercentiles {
        p50_ms: one_f64(client.query_instant(&q_p50).await, &mut errors),
        p95_ms: one_f64(client.query_instant(&q_p95).await, &mut errors),
        p99_ms: one_f64(client.query_instant(&q_p99).await, &mut errors),
    };
    (data, errors)
}

/// Fetch subscription lag for a topic
pub async fn fetch_subscription_lag(
    client: &MetricsClient,
    topic: &str,
) -> (SubscriptionLag, Vec<String>) {
    let mut errors = Vec::new();

    let q_lag = format!(
        "sum(danube_subscription_lag_messages{{topic=\"{}\"}})",
        topic
    );

    let data = SubscriptionLag {
        total_lag_messages: sum_u64(client.query_instant(&q_lag).await, &mut errors),
    };
    (data, errors)
}

/// Fetch reliable/persistent topic metrics (WAL + Cloud)
/// Returns None if no WAL metrics are present (topic is not persistent)
pub async fn fetch_reliable_metrics(
    client: &MetricsClient,
    topic: &str,
) -> (Option<ReliableMetrics>, Vec<String>) {
    let mut errors = Vec::new();

    let q_wal_app = format!("sum(danube_wal_append_total{{topic=\"{}\"}})", topic);
    let q_wal_bytes = format!("sum(danube_wal_append_bytes_total{{topic=\"{}\"}})", topic);
    let q_wal_fsync = format!("sum(danube_wal_fsync_total{{topic=\"{}\"}})", topic);
    let q_wal_p50 = format!(
        "danube_wal_flush_latency_ms{{topic=\"{}\",quantile=\"0.5\"}}",
        topic
    );
    let q_wal_p95 = format!(
        "danube_wal_flush_latency_ms{{topic=\"{}\",quantile=\"0.95\"}}",
        topic
    );
    let q_wal_p99 = format!(
        "danube_wal_flush_latency_ms{{topic=\"{}\",quantile=\"0.99\"}}",
        topic
    );
    let q_cloud_bytes = format!(
        "sum(danube_cloud_upload_bytes_total{{topic=\"{}\"}})",
        topic
    );
    let q_cloud_objects = format!(
        "sum(danube_cloud_upload_objects_total{{topic=\"{}\",result=\"ok\"}})",
        topic
    );

    // Check if topic has WAL metrics (indicates persistent/reliable topic)
    let wal_append_total = sum_u64(client.query_instant(&q_wal_app).await, &mut errors);
    let wal_p50 = one_f64(client.query_instant(&q_wal_p50).await, &mut errors);
    let wal_p95 = one_f64(client.query_instant(&q_wal_p95).await, &mut errors);
    let wal_p99 = one_f64(client.query_instant(&q_wal_p99).await, &mut errors);

    // If no WAL activity, this is not a persistent topic
    if wal_append_total == 0 && wal_p50 == 0.0 && wal_p95 == 0.0 && wal_p99 == 0.0 {
        return (None, errors);
    }

    let data = ReliableMetrics {
        wal_append_total,
        wal_append_bytes_total: sum_u64(client.query_instant(&q_wal_bytes).await, &mut errors),
        wal_fsync_total: sum_u64(client.query_instant(&q_wal_fsync).await, &mut errors),
        wal_flush_latency_p50_ms: wal_p50,
        wal_flush_latency_p95_ms: wal_p95,
        wal_flush_latency_p99_ms: wal_p99,
        cloud_upload_bytes_total: sum_u64(client.query_instant(&q_cloud_bytes).await, &mut errors),
        cloud_upload_objects_total: sum_u64(
            client.query_instant(&q_cloud_objects).await,
            &mut errors,
        ),
    };
    (Some(data), errors)
}
