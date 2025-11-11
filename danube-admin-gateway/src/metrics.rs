use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct MetricsConfig {
    pub base_url: String,
    pub timeout_ms: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:9090".to_string(),
            timeout_ms: 800,
        }
    }
}

#[derive(Clone)]
pub struct MetricsClient {
    cfg: MetricsConfig,
    http: reqwest::Client,
}

impl MetricsClient {
    pub fn new(cfg: MetricsConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(cfg.timeout_ms))
            .build()?;
        Ok(Self { cfg, http })
    }

    pub async fn query_instant(&self, query: &str) -> Result<PromResponse> {
        let url = format!(
            "{}/api/v1/query",
            self.cfg.base_url.trim_end_matches('/')
        );
        let resp = self
            .http
            .get(url)
            .query(&[("query", query)])
            .send()
            .await?
            .error_for_status()?;
        let body = resp.json::<PromResponse>().await?;
        if body.status != "success" {
            tracing::error!(status = %body.status, %query, "prometheus instant query returned non-success status");
        }
        if body.data.result_type != "vector" {
            tracing::error!(result_type = %body.data.result_type, %query, "unexpected result_type for instant query");
        }
        Ok(body)
    }
}

#[derive(serde::Deserialize, Debug)]
pub struct PromResponse {
    pub status: String,
    pub data: PromData,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PromData {
    pub result_type: String,
    pub result: Vec<PromResult>,
}

#[derive(serde::Deserialize, Debug)]
pub struct PromResult {
    pub metric: HashMap<String, String>,
    pub value: (f64, String),
}
