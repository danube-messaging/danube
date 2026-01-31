//! Shared metrics module for querying Prometheus
//!
//! This module provides a unified interface for fetching Danube metrics
//! from Prometheus, used by both the UI server and MCP tools.

pub mod client;
pub mod queries;
pub mod types;

pub use client::{MetricsClient, MetricsConfig};
