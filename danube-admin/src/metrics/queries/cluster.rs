//! Cluster-wide metric queries

use crate::metrics::client::MetricsClient;
use crate::metrics::types::{ClusterMetrics, MetricResult};

use super::helpers::{one_f64, sum_f64, sum_u64};

/// Fetch cluster-wide metrics
pub async fn fetch_cluster_metrics(client: &MetricsClient) -> MetricResult<ClusterMetrics> {
    let mut errors = Vec::new();

    let topics_total = sum_u64(
        client
            .query_instant("count(danube_broker_topics_owned)")
            .await,
        &mut errors,
    );
    let producers_total = sum_u64(
        client
            .query_instant("sum(danube_topic_active_producers)")
            .await,
        &mut errors,
    );
    let consumers_total = sum_u64(
        client
            .query_instant("sum(danube_topic_active_consumers)")
            .await,
        &mut errors,
    );
    let subscriptions_total = sum_u64(
        client
            .query_instant("sum(danube_topic_active_subscriptions)")
            .await,
        &mut errors,
    );
    let broker_count = sum_u64(
        client
            .query_instant("count(danube_leader_election_state)")
            .await,
        &mut errors,
    );
    let messages_in_rate = sum_f64(
        client
            .query_instant("sum(rate(danube_topic_messages_in_total[1m]))")
            .await,
        &mut errors,
    );
    let messages_out_rate = sum_f64(
        client
            .query_instant("sum(rate(danube_consumer_messages_out_total[1m]))")
            .await,
        &mut errors,
    );
    let bytes_in_rate = sum_f64(
        client
            .query_instant("sum(rate(danube_topic_bytes_in_total[1m]))")
            .await,
        &mut errors,
    );
    let bytes_out_rate = sum_f64(
        client
            .query_instant("sum(rate(danube_consumer_bytes_out_total[1m]))")
            .await,
        &mut errors,
    );
    let imbalance_cv = one_f64(
        client.query_instant("danube_cluster_imbalance_cv").await,
        &mut errors,
    );

    MetricResult::with_errors(
        ClusterMetrics {
            broker_count,
            topics_total,
            producers_total,
            consumers_total,
            subscriptions_total,
            messages_in_rate,
            messages_out_rate,
            bytes_in_rate,
            bytes_out_rate,
            imbalance_cv,
        },
        errors,
    )
}
