use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::app::AppState;

#[derive(Clone, Serialize)]
pub struct TopicSeriesResponse {
    pub series: Vec<Series>,
    pub errors: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct Series {
    pub name: String,
    pub labels: Option<HashMap<String, String>>,
    pub points: Vec<(i64, f64)>,
}

#[derive(Deserialize)]
pub struct SeriesParams {
    pub from: i64,
    pub to: i64,
    pub step: String,
}

pub async fn topic_series(
    Path(topic): Path<String>,
    Query(p): Query<SeriesParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let mut errors: Vec<String> = Vec::new();

    let clamp = |v: i64, min: i64, max: i64| v.max(min).min(max);
    let now = chrono::Utc::now().timestamp();
    let from = clamp(p.from, now - 24 * 3600, now);
    let to = clamp(p.to, from + 1, now);
    let step = if p.step.is_empty() { "15s".to_string() } else { p.step };

    let q_publish = format!(
        "sum(rate(danube_topic_messages_in_total{{topic=\"{}\"}}[1m]))",
        topic
    );
    let q_dispatch = format!(
        "sum(rate(danube_consumer_messages_out_total{{topic=\"{}\"}}[1m]))",
        topic
    );
    let q_bytes_in = format!(
        "sum(rate(danube_topic_bytes_in_total{{topic=\"{}\"}}[1m]))",
        topic
    );
    let q_bytes_out = format!(
        "sum(rate(danube_consumer_bytes_out_total{{topic=\"{}\"}}[1m]))",
        topic
    );
    let q_errors_by_code = format!(
        "sum by (error_code) (rate(danube_producer_send_total{{topic=\"{}\",result=\"error\"}}[5m]))",
        topic
    );

    let mut out: Vec<Series> = Vec::new();

    let to_points = |vals: &Vec<(f64, String)>| -> Vec<(i64, f64)> {
        vals.iter()
            .filter_map(|(ts, v)| v.parse::<f64>().ok().map(|vv| ((*ts as f64 * 1000.0) as i64, vv)))
            .collect()
    };

    match state.metrics.query_range(&q_publish, from, to, &step).await {
        Ok(resp) => {
            let points = resp
                .data
                .result
                .get(0)
                .map(|s| to_points(&s.values))
                .unwrap_or_default();
            out.push(Series { name: "publish_rate_1m".to_string(), labels: None, points });
        }
        Err(e) => errors.push(format!("publish_rate_1m query failed: {}", e)),
    }

    match state.metrics.query_range(&q_dispatch, from, to, &step).await {
        Ok(resp) => {
            let points = resp
                .data
                .result
                .get(0)
                .map(|s| to_points(&s.values))
                .unwrap_or_default();
            out.push(Series { name: "dispatch_rate_1m".to_string(), labels: None, points });
        }
        Err(e) => errors.push(format!("dispatch_rate_1m query failed: {}", e)),
    }

    match state.metrics.query_range(&q_bytes_in, from, to, &step).await {
        Ok(resp) => {
            let points = resp
                .data
                .result
                .get(0)
                .map(|s| to_points(&s.values))
                .unwrap_or_default();
            out.push(Series { name: "bytes_in_rate_1m".to_string(), labels: None, points });
        }
        Err(e) => errors.push(format!("bytes_in_rate_1m query failed: {}", e)),
    }

    match state.metrics.query_range(&q_bytes_out, from, to, &step).await {
        Ok(resp) => {
            let points = resp
                .data
                .result
                .get(0)
                .map(|s| to_points(&s.values))
                .unwrap_or_default();
            out.push(Series { name: "bytes_out_rate_1m".to_string(), labels: None, points });
        }
        Err(e) => errors.push(format!("bytes_out_rate_1m query failed: {}", e)),
    }

    match state.metrics.query_range(&q_errors_by_code, from, to, &step).await {
        Ok(resp) => {
            for s in resp.data.result.iter() {
                let mut labels = HashMap::new();
                if let Some(code) = s.metric.get("error_code") {
                    labels.insert("error_code".to_string(), code.clone());
                }
                out.push(Series {
                    name: "producer_send_errors".to_string(),
                    labels: Some(labels),
                    points: to_points(&s.values),
                });
            }
        }
        Err(e) => errors.push(format!("producer_send_errors query failed: {}", e)),
    }

    Json(TopicSeriesResponse { series: out, errors }).into_response()
}
