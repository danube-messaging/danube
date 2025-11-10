use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct MetricsConfig {
    pub scheme: String,  // http or https
    pub port: u16,       // default 9040
    pub path: String,    // default /metrics
    pub timeout_ms: u64, // per-scrape timeout
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            scheme: "http".to_string(),
            port: 9040,
            path: "/metrics".to_string(),
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

    pub async fn scrape_host_port(&self, host: &str, port: u16) -> Result<String> {
        let url = format!("{}://{}:{}{}", self.cfg.scheme, host, port, self.cfg.path);
        let resp = self.http.get(&url).send().await?.error_for_status()?;
        let body = resp.text().await?;
        Ok(body)
    }

    pub fn base_port(&self) -> u16 {
        self.cfg.port
    }
}

// Extremely simple Prometheus text parser for single-sample gauges/counters.
// Returns map: metric_name -> Vec<(labels_map, value)>
pub fn parse_prometheus(text: &str) -> HashMap<String, Vec<(HashMap<String, String>, f64)>> {
    let mut out: HashMap<String, Vec<(HashMap<String, String>, f64)>> = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Format: name{labels} value  OR name value
        if let Some((left, val_str)) = line.rsplit_once(' ') {
            if let Ok(value) = val_str.parse::<f64>() {
                let (name, labels) = if let Some(idx) = left.find('{') {
                    let name = &left[..idx];
                    let rest = &left[idx + 1..];
                    if let Some(end_idx) = rest.find('}') {
                        let labels_str = &rest[..end_idx];
                        (name, parse_labels(labels_str))
                    } else {
                        (left, HashMap::new())
                    }
                } else {
                    (left, HashMap::new())
                };
                out.entry(name.to_string())
                    .or_default()
                    .push((labels, value));
            }
        }
    }
    out
}

fn parse_labels(s: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for part in s.split(',') {
        if let Some((k, v)) = part.split_once('=') {
            let v = v.trim_matches('"');
            m.insert(k.to_string(), v.to_string());
        }
    }
    m
}
