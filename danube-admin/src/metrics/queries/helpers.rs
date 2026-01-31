//! Helper functions for parsing Prometheus responses

use crate::metrics::client::PromResponse;

/// Sum all values in a Prometheus response as u64
pub fn sum_u64(resp: anyhow::Result<PromResponse>, errors: &mut Vec<String>) -> u64 {
    match resp {
        Ok(r) => r
            .data
            .result
            .iter()
            .filter_map(|e| e.value.1.parse::<f64>().ok())
            .map(|v| v as u64)
            .sum(),
        Err(e) => {
            errors.push(e.to_string());
            0
        }
    }
}

/// Sum all values in a Prometheus response as f64
pub fn sum_f64(resp: anyhow::Result<PromResponse>, errors: &mut Vec<String>) -> f64 {
    match resp {
        Ok(r) => r
            .data
            .result
            .iter()
            .filter_map(|e| e.value.1.parse::<f64>().ok())
            .sum(),
        Err(e) => {
            errors.push(e.to_string());
            0.0
        }
    }
}

/// Get the first value from a Prometheus response as f64
pub fn one_f64(resp: anyhow::Result<PromResponse>, errors: &mut Vec<String>) -> f64 {
    match resp {
        Ok(r) => r
            .data
            .result
            .iter()
            .filter_map(|e| e.value.1.parse::<f64>().ok())
            .next()
            .unwrap_or(0.0),
        Err(e) => {
            errors.push(e.to_string());
            0.0
        }
    }
}
