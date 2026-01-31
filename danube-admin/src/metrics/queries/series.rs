//! Time-series and raw query functions

use std::collections::HashMap;

use crate::metrics::client::MetricsClient;

/// Series data point (timestamp_ms, value)
pub type SeriesPoint = (i64, f64);

/// Fetch a time-series from Prometheus range query
pub async fn fetch_topic_series(
    client: &MetricsClient,
    query: &str,
    from: i64,
    to: i64,
    step: &str,
) -> Result<Vec<SeriesPoint>, String> {
    match client.query_range(query, from, to, step).await {
        Ok(resp) => {
            let points = resp
                .data
                .result
                .get(0)
                .map(|s| {
                    s.values
                        .iter()
                        .filter_map(|(ts, v)| {
                            v.parse::<f64>()
                                .ok()
                                .map(|vv| ((*ts as f64 * 1000.0) as i64, vv))
                        })
                        .collect()
                })
                .unwrap_or_default();
            Ok(points)
        }
        Err(e) => Err(format!("query failed: {}", e)),
    }
}

/// Fetch multiple time-series (with labels) from Prometheus range query
pub async fn fetch_topic_series_multi(
    client: &MetricsClient,
    query: &str,
    from: i64,
    to: i64,
    step: &str,
) -> Result<Vec<(HashMap<String, String>, Vec<SeriesPoint>)>, String> {
    match client.query_range(query, from, to, step).await {
        Ok(resp) => {
            let results = resp
                .data
                .result
                .iter()
                .map(|s| {
                    let labels = s.metric.clone();
                    let points: Vec<SeriesPoint> = s
                        .values
                        .iter()
                        .filter_map(|(ts, v)| {
                            v.parse::<f64>()
                                .ok()
                                .map(|vv| ((*ts as f64 * 1000.0) as i64, vv))
                        })
                        .collect();
                    (labels, points)
                })
                .collect();
            Ok(results)
        }
        Err(e) => Err(format!("query failed: {}", e)),
    }
}

/// Execute a raw PromQL query and return formatted results
pub async fn query_raw(client: &MetricsClient, query: &str) -> Result<String, String> {
    match client.query_instant(query).await {
        Ok(resp) => {
            let mut output = format!("Query: {}\nResults:\n", query);
            if resp.data.result.is_empty() {
                output.push_str("  (no data)\n");
            } else {
                for r in resp.data.result.iter() {
                    let labels: Vec<String> = r
                        .metric
                        .iter()
                        .map(|(k, v)| format!("{}=\"{}\"", k, v))
                        .collect();
                    let labels_str = if labels.is_empty() {
                        "{}".to_string()
                    } else {
                        format!("{{{}}}", labels.join(", "))
                    };
                    output.push_str(&format!("  {} => {}\n", labels_str, r.value.1));
                }
            }
            Ok(output)
        }
        Err(e) => Err(format!("Query failed: {}", e)),
    }
}
